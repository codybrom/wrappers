//! Response parsing: data extraction and pagination handling

use serde_json::Value as JsonValue;

use crate::OpenApiFdw;
use crate::bindings::supabase::wrappers::{types::FdwError, utils};
use crate::pagination::PaginationToken;

/// Common wrapper keys for auto-detecting the data array in API responses.
pub(crate) const WRAPPER_KEYS: &[&str] = &[
    "data", "results", "items", "records", "entries", "features", "@graph",
];

/// Metadata/pagination keys that may accompany an object-valued wrapper in a
/// pure-envelope response without making the response a business object.
///
/// Deliberately conservative: every entry must be implausible as a *business*
/// field on a record, because a false positive here silently discards real
/// data. Bare `total`, `count`, `size`, `url`, `object`, `self` and `kind` are
/// excluded for exactly that reason — an order `{items: {...}, total: 99.99}`
/// must not be mistaken for an envelope around `items`. Their unambiguous
/// compound forms are listed instead. Omitting a genuine metadata key only
/// means the response is left unwrapped, which the user can correct with an
/// explicit `response_path`; including a business key silently drops columns.
///
/// This list governs OBJECT-valued wrappers only. Array-valued wrappers — the
/// common list envelope, e.g. Stripe's `{object: "list", data: [...], url: …}`
/// — are matched earlier and unconditionally, so they are unaffected.
const ENVELOPE_META_KEYS: &[&str] = &[
    "has_more",
    "meta",
    "metadata",
    "pagination",
    "links",
    "_links",
    "next",
    "next_url",
    "previous",
    "prev",
    "cursor",
    "next_cursor",
    "total_count",
    "total_pages",
    "page",
    "per_page",
    "page_size",
    "offset",
    "limit",
];

/// Returns true if `obj` is a pure envelope around `wrapper_key`: every other
/// top-level key is a known metadata/pagination key, so unwrapping the wrapper
/// won't discard sibling business data.
fn is_pure_envelope(obj: &JsonValue, wrapper_key: &str) -> bool {
    obj.as_object().is_some_and(|map| {
        map.keys().all(|k| {
            k.as_str() == wrapper_key
                || ENVELOPE_META_KEYS.iter().any(|m| m.eq_ignore_ascii_case(k))
        })
    })
}

/// JSON pointer paths to check for a next-page URL.
const NEXT_URL_PATHS: &[&str] = &[
    "/meta/pagination/next",
    "/meta/pagination/next_url",
    "/pagination/next",
    "/pagination/next_url",
    "/links/next",
    "/links/next_url",
    "/next",
    "/next_url",
    "/_links/next/href",
];

/// JSON pointer paths to check for a boolean "has more pages" flag.
const HAS_MORE_PATHS: &[&str] = &[
    "/meta/pagination/has_more",
    "/has_more",
    "/pagination/has_more",
];

/// JSON pointer paths to check for a next-page cursor value.
const CURSOR_PATHS: &[&str] = &[
    "/meta/pagination/next_cursor",
    "/pagination/next_cursor",
    "/next_cursor",
    "/cursor",
];

impl OpenApiFdw {
    /// Extract the data array from the response, taking ownership to avoid cloning
    pub(crate) fn extract_data(&self, resp: &mut JsonValue) -> Result<Vec<JsonValue>, FdwError> {
        // If response_path is specified, use it
        if let Some(ref path) = self.response_path {
            if let Some(data) = resp.pointer_mut(path).map(JsonValue::take) {
                return Self::json_to_rows(data);
            }
            // response_path not found — fall through to auto-detection (common
            // when a rowid lookup returns a single object instead of the
            // collection). Warn in debug so a genuinely mistyped response_path is
            // diagnosable rather than silently ignored.
            if self.config.debug {
                utils::report_info(&format!(
                    "[openapi_fdw] response_path '{path}' not found in response; \
                     falling back to auto-detection."
                ));
            }
        }

        // Direct array response
        if resp.is_array() {
            return Self::json_to_rows(resp.take());
        }

        // Try common wrapper patterns
        if resp.is_object() {
            // Prefer an array-valued wrapper key — the unambiguous list-envelope
            // case (e.g. {data: [...]}).
            for key in WRAPPER_KEYS {
                if resp.get(*key).is_some_and(JsonValue::is_array) {
                    return Self::json_to_rows(resp[*key].take());
                }
            }

            // An object-valued wrapper key is the record holder only when the
            // response is a pure envelope (every sibling is known metadata).
            // Otherwise the response is itself a single business object that
            // merely contains a field named like a wrapper key (e.g.
            // {id, name, data:{...}}), and unwrapping would discard the siblings.
            for key in WRAPPER_KEYS {
                if resp.get(*key).is_some_and(JsonValue::is_object) && is_pure_envelope(resp, key) {
                    return Self::json_to_rows(resp[*key].take());
                }
            }

            // Single object response
            return Ok(vec![resp.take()]);
        }

        Err(format!(
            "Unable to extract data from response (type: {}). \
             Expected an array, object with a known wrapper key \
             ({}), \
             or set response_path in table options.",
            match resp {
                JsonValue::Null => "null",
                JsonValue::Bool(_) => "boolean",
                JsonValue::Number(_) => "number",
                JsonValue::String(_) => "string",
                _ => "unknown",
            },
            WRAPPER_KEYS.join(", "),
        ))
    }

    /// Convert a JSON value to a vector of row objects (takes ownership, no cloning)
    pub(crate) fn json_to_rows(data: JsonValue) -> Result<Vec<JsonValue>, FdwError> {
        match data {
            JsonValue::Array(arr) => Ok(arr),
            data if data.is_object() => Ok(vec![data]),
            _ => Err(format!(
                "Response data is not an array or object (got {})",
                match &data {
                    JsonValue::Null => "null",
                    JsonValue::Bool(_) => "boolean",
                    JsonValue::Number(_) => "number",
                    JsonValue::String(_) => "string",
                    _ => "unknown",
                }
            )),
        }
    }

    /// Handle pagination from the response.
    ///
    /// Precedence (first match wins):
    ///   1. Explicit `cursor_path` configured by the user
    ///   2. RFC 8288 `Link` header with `rel="next"` (GitHub, GitLab, etc.)
    ///   3. JSON-body auto-detection: known next-URL paths
    ///   4. JSON-body auto-detection: `has_more` flag + cursor paths
    pub(crate) fn handle_pagination(&mut self, resp: &JsonValue, headers: &[(String, String)]) {
        self.pagination.clear_next();

        // 1. Try configured cursor path first (explicit user config wins)
        if !self.cursor_path.is_empty() {
            if let Some(value) = Self::extract_non_empty_string(resp, &self.cursor_path) {
                if value.starts_with("http://") || value.starts_with("https://") {
                    self.pagination.next = Some(PaginationToken::Url(value));
                } else {
                    self.pagination.next = Some(PaginationToken::Cursor(value));
                }
                return;
            }
        }

        // 2. RFC 8288 Link header with rel="next" (cross-origin protection
        //    is enforced later in resolve_pagination_url when the URL is used).
        if let Some(url) = find_link_header_next(headers) {
            self.pagination.next = Some(PaginationToken::Url(url));
            return;
        }

        // Only try body auto-detection for object responses
        if resp.as_object().is_none() {
            return;
        }

        // 3. Check for next URL in common JSON-body locations
        for path in NEXT_URL_PATHS {
            if let Some(url) = Self::extract_non_empty_string(resp, path) {
                self.pagination.next = Some(PaginationToken::Url(url));
                return;
            }
        }

        // Check for has_more flag with cursor
        let has_more = HAS_MORE_PATHS
            .iter()
            .find_map(|p| resp.pointer(p))
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);

        if !has_more {
            return;
        }

        // Find next cursor
        for path in CURSOR_PATHS {
            if let Some(cursor) = Self::extract_non_empty_string(resp, path) {
                self.pagination.next = Some(PaginationToken::Cursor(cursor));
                return;
            }
        }
    }

    /// Extract a non-empty string from a JSON pointer path
    pub(crate) fn extract_non_empty_string(json: &JsonValue, path: &str) -> Option<String> {
        json.pointer(path)
            .and_then(JsonValue::as_str)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
    }
}

/// Find the URL of the first `Link` header entry with `rel="next"`.
///
/// Header names are matched case-insensitively. Multiple `Link` headers
/// (whether comma-concatenated or sent as separate entries) are all searched.
pub(crate) fn find_link_header_next(headers: &[(String, String)]) -> Option<String> {
    headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("link"))
        .find_map(|(_, value)| parse_link_header_next(value))
}

/// Parse an RFC 8288 `Link` header value and return the URL of the first
/// entry whose `rel` parameter contains the value `next`.
///
/// Handles multi-link headers like:
///   `<https://api/items?page=2>; rel="next", <https://api/items?page=10>; rel="last"`
/// and multi-rel values like `rel="next prev"`.
fn parse_link_header_next(value: &str) -> Option<String> {
    for entry in split_link_entries(value) {
        let entry = entry.trim();
        if !entry.starts_with('<') {
            continue;
        }
        let Some(close) = entry.find('>') else {
            continue;
        };
        let url = entry[1..close].trim();
        if url.is_empty() {
            continue;
        }
        let params = &entry[close + 1..];
        if has_rel_next(params) {
            return Some(url.to_string());
        }
    }
    None
}

/// Split a Link header value into entries on top-level commas, ignoring
/// commas inside angle-bracketed URIs or quoted strings. Honors RFC 7230
/// `quoted-pair` escapes so a backslash-escaped quote inside a parameter
/// value (e.g. `title="a \"q\""`) does not flip the quote state.
fn split_link_entries(s: &str) -> Vec<&str> {
    let mut entries = Vec::new();
    let mut depth = 0i32;
    let mut in_quotes = false;
    let mut escape = false;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match c {
            '\\' if in_quotes => escape = true,
            '<' if !in_quotes => depth += 1,
            '>' if !in_quotes => depth = depth.saturating_sub(1),
            '"' => in_quotes = !in_quotes,
            ',' if depth == 0 && !in_quotes => {
                entries.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start <= s.len() {
        entries.push(&s[start..]);
    }
    entries
}

/// Returns true if the parameter list (after the URI part of a Link entry)
/// contains a `rel` parameter whose value is or includes `next`.
fn has_rel_next(params: &str) -> bool {
    for raw in params.split(';') {
        let part = raw.trim();
        let Some((name, value)) = part.split_once('=') else {
            continue;
        };
        if !name.trim().eq_ignore_ascii_case("rel") {
            continue;
        }
        let value = value.trim().trim_matches('"');
        return value
            .split_ascii_whitespace()
            .any(|v| v.eq_ignore_ascii_case("next"));
    }
    false
}

#[cfg(test)]
#[path = "response_tests.rs"]
mod tests;
