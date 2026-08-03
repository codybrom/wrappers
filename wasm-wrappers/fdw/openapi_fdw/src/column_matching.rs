//! Column name matching and JSON-to-Cell conversion
//!
//! Handles the mapping between SQL column names and JSON keys,
//! including camelCase, snake_case, and normalized matching strategies.

use std::borrow::Cow;

use serde_json::Value as JsonValue;

use crate::bindings::supabase::wrappers::{
    time,
    types::{Cell, FdwError, TypeOid},
    utils,
};
use crate::{OpenApiFdw, extract_effective_row};

/// Longest value echoed into a debug log line. Debug logging goes to the
/// Postgres log, which is more widely readable than the table itself, so a
/// diagnostic never reproduces a whole field.
const MAX_DEBUG_VALUE_LEN: usize = 60;

/// Truncate a value for a debug log line, on a char boundary.
fn truncate_for_log(value: &str) -> String {
    match value.char_indices().nth(MAX_DEBUG_VALUE_LEN) {
        Some((idx, _)) => format!("{}…", &value[..idx]),
        None => value.to_owned(),
    }
}

/// Whether `value` is a canonical 8-4-4-4-12 hyphenated hex UUID.
///
/// `Cell::Uuid` is handed to the host verbatim, so a malformed value would only
/// fail later (and less clearly) inside Postgres. Rejecting it here degrades to
/// NULL, consistent with every other parse failure in `convert_string_to_cell`.
pub(crate) fn is_canonical_uuid(value: &str) -> bool {
    const GROUPS: [usize; 5] = [8, 4, 4, 4, 12];
    let mut groups = value.split('-');
    for len in GROUPS {
        match groups.next() {
            Some(g) if g.len() == len && g.bytes().all(|b| b.is_ascii_hexdigit()) => {}
            _ => return false,
        }
    }
    groups.next().is_none()
}

/// How a SQL column name was resolved to a JSON key.
///
/// Avoids cloning strings that already exist in CachedColumn -- only the
/// case-insensitive fallback (rare) needs its own allocation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyMatch {
    /// JSON key matches CachedColumn::name exactly
    Exact,
    /// JSON key matches CachedColumn::camel_name
    CamelCase,
    /// JSON key matched case-insensitively (stores the original API key)
    CaseInsensitive(String),
    /// Column resolves to an injected WHERE-clause value (used as a URL
    /// param). Resolved once in build_column_key_map so iter_scan doesn't probe
    /// injected_params per row.
    Injected(String),
    /// Column absent from the probe row (or ambiguous under normalized
    /// matching). Only the cheap exact/camel lookups are retried per row.
    Missing,
}

/// Pre-computed column metadata to avoid repeated WASM boundary crossings.
///
/// During iter_scan, each call to ctx.get_columns(), col.name(), and
/// col.type_oid() crosses the WASM boundary. By caching these once in
/// begin_scan, we eliminate ~2000 boundary crossings per 100-row scan.
#[derive(Debug)]
pub(crate) struct CachedColumn {
    pub name: String,
    pub type_oid: TypeOid,
    pub camel_name: String,
    pub lower_name: String,
    /// Alphanumeric-only lowercase name for normalized matching.
    /// Strips @, ., -, $ etc. so @id / _id / id can match.
    pub alnum_name: String,
}

/// Convert snake_case to camelCase
pub(crate) fn to_camel_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = false;

    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_uppercase().next().unwrap_or(c));
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}

/// Strip non-alphanumeric chars and lowercase for normalized matching.
///
/// Used to match JSON keys with special characters (@id, user.name, $oid)
/// to sanitized SQL column names (_id, user_name, _oid).
pub(crate) fn normalize_to_alnum(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .collect::<String>()
        .to_lowercase()
}

impl OpenApiFdw {
    /// Normalize a date/datetime string for RFC3339 parsing.
    ///
    /// Handles two non-RFC3339 formats:
    /// - Date-only "2024-01-15" becomes "2024-01-15T00:00:00Z"
    /// - ISO 8601 tz without colon "2024-01-15T12:00:00+0000" becomes "2024-01-15T12:00:00+00:00"
    ///
    /// Returns Cow<str> to avoid allocating when the string is already valid.
    pub(crate) fn normalize_datetime(s: &str) -> Cow<'_, str> {
        // Date-only: exactly 10 chars matching YYYY-MM-DD pattern
        if s.len() == 10 && s.as_bytes().get(4) == Some(&b'-') && s.as_bytes().get(7) == Some(&b'-')
        {
            return Cow::Owned(format!("{s}T00:00:00Z"));
        }

        // Fix timezone offset without colon: +0000 → +00:00, -0500 → -05:00
        // ISO 8601 allows ±HHMM but RFC 3339 requires ±HH:MM
        let bytes = s.as_bytes();
        let len = bytes.len();
        if len >= 5 {
            let sign_pos = len - 4;
            if (bytes[sign_pos - 1] == b'+' || bytes[sign_pos - 1] == b'-')
                && bytes[sign_pos..].iter().all(|b| b.is_ascii_digit())
            {
                let mut fixed = String::with_capacity(len + 1);
                fixed.push_str(&s[..sign_pos + 2]);
                fixed.push(':');
                fixed.push_str(&s[sign_pos + 2..]);
                return Cow::Owned(fixed);
            }
        }

        Cow::Borrowed(s)
    }

    /// Build a map from column index to resolved JSON key, using the first row's keys.
    ///
    /// Runs the 3-step matching (exact, camelCase, case-insensitive) once per column
    /// instead of once per column per row. Called after each make_request.
    ///
    /// Only the first row of each page is probed. This works because most APIs return
    /// rows with the same key shape. If a later row has different keys, unmatched
    /// columns fall back to an O(n) scan in json_to_cell_cached (correct but slower).
    pub(crate) fn build_column_key_map(&mut self) {
        // Probe the first row's object (if any) for key shape. Injected params
        // are resolved regardless of row shape (they don't depend on the row).
        let obj = self
            .src_rows
            .first()
            .map(|r| extract_effective_row(r, self.object_path.as_deref()))
            .and_then(JsonValue::as_object);

        self.column_key_map = self
            .cached_columns
            .iter()
            .map(|cc| {
                // attrs is special-cased (returns entire row), no key lookup needed
                if cc.name == "attrs" {
                    return None;
                }
                // Injected WHERE-clause values take precedence over key matching
                // and are fixed for the whole page, so resolve them here rather
                // than probing injected_params per row — and independent of
                // whether the probe row is an object.
                if let Some(value) = self.injected_params.get(&cc.lower_name) {
                    return Some(KeyMatch::Injected(value.clone()));
                }
                let obj = obj?;
                if obj.contains_key(&cc.name) {
                    Some(KeyMatch::Exact)
                } else if obj.contains_key(&cc.camel_name) {
                    Some(KeyMatch::CamelCase)
                } else if let Some(key) = obj.keys().find(|k| k.to_lowercase() == cc.lower_name) {
                    Some(KeyMatch::CaseInsensitive(key.clone()))
                } else {
                    // Normalized match: strip non-alphanumeric chars and compare
                    // (handles @id↔_id, user.name↔user_name, etc.). If MORE THAN
                    // ONE key normalizes to the same form the match is ambiguous
                    // — mark Missing (NULL) rather than binding an arbitrary key.
                    let mut it = obj
                        .keys()
                        .filter(|k| normalize_to_alnum(k) == cc.alnum_name);
                    match (it.next(), it.next()) {
                        (Some(key), None) => Some(KeyMatch::CaseInsensitive(key.clone())),
                        _ => Some(KeyMatch::Missing),
                    }
                }
            })
            .collect();
    }

    /// Convert a JSON value to a Cell based on the target PostgreSQL type.
    ///
    /// Handles type coercion, date/time parsing, and numeric conversions.
    pub(crate) fn convert_json_to_cell(
        src: &JsonValue,
        type_oid: &TypeOid,
    ) -> Result<Option<Cell>, FdwError> {
        let cell = match type_oid {
            TypeOid::Bool => src.as_bool().map(Cell::Bool),
            TypeOid::I8 => src
                .as_i64()
                .and_then(|v| i8::try_from(v).ok())
                .map(Cell::I8),
            TypeOid::I16 => src
                .as_i64()
                .and_then(|v| i16::try_from(v).ok())
                .map(Cell::I16),
            TypeOid::I32 => src
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(Cell::I32),
            TypeOid::I64 => src.as_i64().map(Cell::I64),
            #[allow(clippy::cast_possible_truncation)]
            TypeOid::F32 => src.as_f64().map(|v| Cell::F32(v as f32)),
            TypeOid::F64 => src.as_f64().map(Cell::F64),
            // Numeric maps to Cell::Numeric(f64) — the only numeric container the
            // host ABI exposes — so integers above 2^53 lose precision. This is a
            // framework-level limitation, not locally fixable; use a bigint or
            // text column for exact large-integer values.
            TypeOid::Numeric => src.as_f64().map(Cell::Numeric),
            TypeOid::String => Some(Cell::String(
                src.as_str()
                    .map_or_else(|| src.to_string(), ToOwned::to_owned),
            )),
            TypeOid::Date => {
                if let Some(s) = src.as_str() {
                    // A single unparseable date must not abort the whole scan;
                    // degrade to NULL (like the numeric-overflow paths and
                    // convert_string_to_cell) rather than propagating the error.
                    time::parse_from_rfc3339(&Self::normalize_datetime(s))
                        .ok()
                        .map(|ts| Cell::Date(ts / 1_000_000))
                } else {
                    // Unix timestamp (seconds since epoch)
                    src.as_i64().map(Cell::Date)
                }
            }
            TypeOid::Timestamp | TypeOid::Timestamptz => {
                let wrap: fn(i64) -> Cell = if matches!(type_oid, TypeOid::Timestamp) {
                    Cell::Timestamp
                } else {
                    Cell::Timestamptz
                };
                if let Some(s) = src.as_str() {
                    // Degrade an unparseable timestamp to NULL rather than failing
                    // the entire scan on one bad row.
                    time::parse_from_rfc3339(&Self::normalize_datetime(s))
                        .ok()
                        .map(wrap)
                } else {
                    // Unix timestamp (seconds since epoch) → microseconds
                    src.as_i64()
                        .and_then(|epoch| epoch.checked_mul(1_000_000))
                        .map(wrap)
                }
            }
            // Reject a non-UUID string rather than handing the host a malformed
            // Cell::Uuid (see is_canonical_uuid). Debug mode reports the
            // resulting NULL via json_to_cell_cached.
            TypeOid::Uuid => src
                .as_str()
                .filter(|v| is_canonical_uuid(v))
                .map(|v| Cell::Uuid(v.to_owned())),
            // Json and unknown types: serialize to JSON string
            TypeOid::Json | TypeOid::Other(_) => Some(Cell::Json(src.to_string())),
        };

        Ok(cell)
    }

    /// Convert a string value from path/query params to a Cell based on target type.
    ///
    /// Used for injecting WHERE clause values that were used as URL parameters.
    pub(crate) fn convert_string_to_cell(value: &str, type_oid: &TypeOid) -> Option<Cell> {
        match type_oid {
            TypeOid::Bool => value.parse::<bool>().ok().map(Cell::Bool),
            TypeOid::I8 => value.parse::<i8>().ok().map(Cell::I8),
            TypeOid::I16 => value.parse::<i16>().ok().map(Cell::I16),
            TypeOid::I32 => value.parse::<i32>().ok().map(Cell::I32),
            TypeOid::I64 => value.parse::<i64>().ok().map(Cell::I64),
            #[allow(clippy::cast_possible_truncation)]
            TypeOid::F32 => value.parse::<f64>().ok().map(|v| Cell::F32(v as f32)),
            TypeOid::F64 => value.parse::<f64>().ok().map(Cell::F64),
            TypeOid::Numeric => value.parse::<f64>().ok().map(Cell::Numeric),
            TypeOid::Date => time::parse_from_rfc3339(&Self::normalize_datetime(value))
                .ok()
                .map(|ts| Cell::Date(ts / 1_000_000)),
            TypeOid::Timestamp | TypeOid::Timestamptz => {
                let wrap: fn(i64) -> Cell = if matches!(type_oid, TypeOid::Timestamp) {
                    Cell::Timestamp
                } else {
                    Cell::Timestamptz
                };
                time::parse_from_rfc3339(&Self::normalize_datetime(value))
                    .ok()
                    .map(wrap)
            }
            // Type-symmetric with convert_json_to_cell: a uuid target yields a
            // Cell::Uuid, not a Cell::String, so injected uuid rowids match the
            // column type. A non-UUID value degrades to NULL like every other
            // parse failure here rather than becoming a malformed Cell::Uuid.
            TypeOid::Uuid => is_canonical_uuid(value).then(|| Cell::Uuid(value.to_string())),
            TypeOid::Json => Some(Cell::Json(value.to_string())),
            TypeOid::String | TypeOid::Other(_) => Some(Cell::String(value.to_string())),
        }
    }

    /// Convert a JSON value to a Cell using cached column metadata and pre-resolved key map.
    ///
    /// Uses CachedColumn fields instead of WASM resource methods, and the pre-built
    /// column_key_map for O(1) JSON key lookup instead of per-row 3-step matching.
    pub(crate) fn json_to_cell_cached(
        &self,
        src_row: &JsonValue,
        col_idx: usize,
    ) -> Result<Option<Cell>, FdwError> {
        let cc = &self.cached_columns[col_idx];

        // Special handling for 'attrs' column - returns entire row as JSON
        if cc.name == "attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        // Injected WHERE-clause value (resolved once in build_column_key_map):
        // coerce to the target column type. A value that can't be represented in
        // that type becomes NULL (matching convert_json_to_cell) rather than a
        // wrong-typed Cell::String.
        if let Some(Some(KeyMatch::Injected(value))) = self.column_key_map.get(col_idx) {
            return Ok(Self::convert_string_to_cell(value, &cc.type_oid));
        }

        // Use pre-resolved key from column_key_map for O(1) lookup
        let src = src_row.as_object().and_then(|obj| {
            match self.column_key_map.get(col_idx) {
                Some(Some(KeyMatch::Exact)) => obj.get(&cc.name),
                Some(Some(KeyMatch::CamelCase)) => obj.get(&cc.camel_name),
                Some(Some(KeyMatch::CaseInsensitive(key))) => obj.get(key),
                Some(Some(KeyMatch::Missing)) => {
                    // Absent in the probe row: retry only the cheap exact/camel
                    // HashMap lookups (covers optional fields omitted in some
                    // rows); skip the expensive per-row case-insensitive and
                    // normalized scans that always failed on the probe row.
                    obj.get(&cc.name).or_else(|| obj.get(&cc.camel_name))
                }
                // Injected is handled above; None (key map not built from an
                // object row) falls back to the full 4-step match.
                _ => obj
                    .get(&cc.name)
                    .or_else(|| obj.get(&cc.camel_name))
                    .or_else(|| {
                        obj.iter()
                            .find(|(k, _)| k.to_lowercase() == cc.lower_name)
                            .map(|(_, v)| v)
                    })
                    .or_else(|| {
                        // Normalized: strip non-alnum, compare (handles @-keys, dots, etc.)
                        obj.iter()
                            .find(|(k, _)| normalize_to_alnum(k) == cc.alnum_name)
                            .map(|(_, v)| v)
                    }),
            }
        });

        let src = match src {
            Some(v) if !v.is_null() => v,
            _ => {
                self.warn_unmatched_key(src_row, col_idx);
                return Ok(None);
            }
        };

        let cell = Self::convert_json_to_cell(src, &cc.type_oid)?;
        if cell.is_none() && self.config.debug {
            // A value was present but could not be represented in the target
            // type (unparseable date/timestamp, out-of-range integer, non-UUID
            // string, ...). Reading NULL is deliberate — one bad row must not
            // abort the scan — but a systematically mistyped column would
            // otherwise be an entire column of silent NULLs with no signal.
            utils::report_info(&format!(
                "[openapi_fdw] column '{}' read as NULL: the response value could not be \
                 converted to the column's Postgres type. Value: {}",
                cc.name,
                truncate_for_log(&src.to_string())
            ));
        }
        Ok(cell)
    }

    /// Debug-only diagnostic for a column that resolved to NULL because the key
    /// map was built from a probe row that lacked the key.
    ///
    /// `KeyMatch::Missing` deliberately retries only the cheap exact/camel
    /// lookups per row, so a key present in *this* row under a case or
    /// punctuation variant is not selected. That is invisible in production;
    /// under debug, report it. Reporting must not change results, so the
    /// discovered key is named, never used.
    fn warn_unmatched_key(&self, src_row: &JsonValue, col_idx: usize) {
        let cc = &self.cached_columns[col_idx];
        if self.config.debug
            && matches!(
                self.column_key_map.get(col_idx),
                Some(Some(KeyMatch::Missing))
            )
            && let Some(obj) = src_row.as_object()
            && let Some(key) = obj.keys().find(|k| {
                k.to_lowercase() == cc.lower_name || normalize_to_alnum(k) == cc.alnum_name
            })
        {
            utils::report_info(&format!(
                "[openapi_fdw] column '{}' read as NULL, but this row has key '{}', which \
                 matches only case-insensitively or after normalization. The key map is built \
                 from the first row, where '{}' was absent, so later rows retry just the exact \
                 and camelCase keys. Rename the column to match the API key exactly.",
                cc.name, key, cc.name
            ));
        }
    }
}

#[cfg(test)]
#[path = "column_matching_tests.rs"]
mod tests;
