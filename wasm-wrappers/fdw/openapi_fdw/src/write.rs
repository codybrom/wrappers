//! Write (data modify) support: INSERT / UPDATE / DELETE over HTTP
//!
//! All write configuration comes from table options parsed in begin_modify,
//! before any HTTP request is made. Writes are strictly per-row: each modify
//! hook issues exactly one HTTP request (the host's HTTP middleware already
//! retries transient failures; see send_once in request.rs).

use std::collections::HashMap;

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::bindings::supabase::wrappers::{
    http, stats, time,
    types::{Cell, FdwError, FdwResult, Options},
    utils,
};
use crate::request::{method_label, redact_query_param};
use crate::{FDW_NAME, OpenApiFdw};

// Table option names (write path)
const OPT_WRITABLE: &str = "writable";
const OPT_INSERT_METHOD: &str = "insert_method";
const OPT_UPDATE_METHOD: &str = "update_method";
const OPT_DELETE_METHOD: &str = "delete_method";
const OPT_WRITE_ENDPOINT: &str = "write_endpoint";
const OPT_INSERT_ENDPOINT: &str = "insert_endpoint";
const OPT_UPDATE_ENDPOINT: &str = "update_endpoint";
const OPT_DELETE_ENDPOINT: &str = "delete_endpoint";
const OPT_ROWID_LOCATION: &str = "rowid_location";
const OPT_UPDATE_ROWID_LOCATION: &str = "update_rowid_location";
const OPT_DELETE_ROWID_LOCATION: &str = "delete_rowid_location";
const OPT_ROWID_BODY_KEY: &str = "rowid_body_key";
const OPT_ROWID_PARAM: &str = "rowid_param";
const OPT_BODY_ROOT_PATH: &str = "body_root_path";
const OPT_BODY_WRAP: &str = "body_wrap";
const OPT_SUCCESS_PATH: &str = "success_path";
const OPT_SUCCESS_VALUE: &str = "success_value";
const OPT_SUCCESS_STATUS: &str = "success_status";

const DEFAULT_SUCCESS_VALUE: &str = "SUCCESS";

/// Column automatically added by IMPORT FOREIGN SCHEMA holding the full JSON
/// response row; never sent back to the API on writes.
const ATTRS_COLUMN: &str = "attrs";

/// Where the rowid is placed for UPDATE/DELETE requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum RowidLocation {
    /// Appended as a URL path segment: `.../{rowid}`
    #[default]
    Url,
    /// Injected into the JSON body under `rowid_body_key`
    Body,
    /// Appended as a query parameter: `...?{rowid_param}={rowid}`
    Query,
}

/// Shape of the JSON body inside `body_root_path`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum BodyWrap {
    /// `{root: {...}}`
    #[default]
    Object,
    /// `{root: [{...}]}`
    Array,
}

/// Write (DML) configuration, resolved from table options in begin_modify.
///
/// A fresh instance is built per begin_modify, so per-table write options
/// cannot bleed across tables (unlike ServerConfig pagination fields, which
/// need an explicit save/restore). Deliberately separate from the scan state
/// on OpenApiFdw: Postgres interleaves the foreign scan with modify in the
/// same UPDATE/DELETE statement, so write hooks must not clobber scan fields.
#[derive(Debug, Clone)]
pub(crate) struct WriteConfig {
    pub(crate) writable: bool,

    // Effective HTTP verb per operation; None means the operation is disabled.
    pub(crate) insert_method: Option<http::Method>,
    pub(crate) update_method: Option<http::Method>,
    pub(crate) delete_method: Option<http::Method>,

    // Endpoint templates, pre-resolved through the fallback chain:
    // per-op endpoint -> write_endpoint -> endpoint.
    pub(crate) insert_endpoint: String,
    pub(crate) update_endpoint: String,
    pub(crate) delete_endpoint: String,

    // Rowid placement, pre-resolved: per-verb override -> rowid_location -> url.
    pub(crate) update_rowid_location: RowidLocation,
    pub(crate) delete_rowid_location: RowidLocation,

    // Rowid identifiers (rowid_column is lowercased, like the scan path's).
    pub(crate) rowid_column: String,
    pub(crate) rowid_body_key: String,
    pub(crate) rowid_param: String,

    // Body envelope.
    pub(crate) body_root_path: Option<String>,
    pub(crate) body_wrap: BodyWrap,

    // Success detection. success_status None accepts any 2xx except 207.
    pub(crate) success_path: Option<String>,
    pub(crate) success_value: String,
    pub(crate) success_status: Option<Vec<u16>>,
}

/// Parse an HTTP verb option value for a write operation (case-insensitive).
/// GET is rejected: a data-modifying request over GET is always a
/// misconfiguration.
pub(crate) fn parse_http_method(value: &str, option_name: &str) -> Result<http::Method, String> {
    match value.to_ascii_uppercase().as_str() {
        "POST" => Ok(http::Method::Post),
        "PUT" => Ok(http::Method::Put),
        "PATCH" => Ok(http::Method::Patch),
        "DELETE" => Ok(http::Method::Delete),
        _ => Err(format!(
            "Invalid {option_name} '{value}'. Must be one of POST, PUT, PATCH, DELETE."
        )),
    }
}

/// Parse a rowid location option value (case-insensitive).
pub(crate) fn parse_rowid_location(
    value: &str,
    option_name: &str,
) -> Result<RowidLocation, String> {
    match value.to_ascii_lowercase().as_str() {
        "url" => Ok(RowidLocation::Url),
        "body" => Ok(RowidLocation::Body),
        "query" => Ok(RowidLocation::Query),
        _ => Err(format!(
            "Invalid {option_name} '{value}'. Must be one of 'url', 'body', or 'query'."
        )),
    }
}

/// Parse a body_wrap option value (case-insensitive).
pub(crate) fn parse_body_wrap(value: &str) -> Result<BodyWrap, String> {
    match value.to_ascii_lowercase().as_str() {
        "object" => Ok(BodyWrap::Object),
        "array" => Ok(BodyWrap::Array),
        _ => Err(format!(
            "Invalid {OPT_BODY_WRAP} '{value}'. Must be 'object' or 'array'."
        )),
    }
}

/// Parse the comma-separated success_status allowlist (e.g. "200,201,202").
pub(crate) fn parse_success_status(value: &str) -> Result<Vec<u16>, String> {
    let codes: Vec<u16> = value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<u16>()
                .map_err(|_| format!("Invalid HTTP status code '{s}' in {OPT_SUCCESS_STATUS}"))
        })
        .collect::<Result<_, _>>()?;
    if codes.is_empty() {
        return Err(format!(
            "{OPT_SUCCESS_STATUS} is set but contains no status codes"
        ));
    }
    // A successful write must return a 2xx. A non-2xx entry (e.g. a 302, which
    // is typically body-less and would also bypass the success_path check) is a
    // misconfiguration, not a trust override — reject it.
    if let Some(&bad) = codes.iter().find(|&&c| !(200..=299).contains(&c)) {
        return Err(format!(
            "Invalid HTTP status code '{bad}' in {OPT_SUCCESS_STATUS}: a successful \
             write must return a 2xx status."
        ));
    }
    Ok(codes)
}

/// Whether a `success_status` allowlist contains a code that status alone can't
/// certify as a successful write — anything outside the trivially-successful
/// 200/201/204. Such an API is expected to encode the per-record outcome in the
/// response body, so success_path is mandatory (config time) AND an empty body
/// cannot be trusted as success (response time). Single source of truth for both
/// checks so they can't drift.
pub(crate) fn success_status_is_unusual(success_status: Option<&[u16]>) -> bool {
    success_status.is_some_and(|codes| codes.iter().any(|c| !matches!(c, 200 | 201 | 204)))
}

/// Validate that a `_path` option is a JSON pointer: starts with '/' and has
/// no empty segments.
pub(crate) fn validate_json_pointer(value: &str, option_name: &str) -> Result<(), String> {
    if !value.starts_with('/') || value[1..].split('/').any(str::is_empty) {
        return Err(format!(
            "Invalid {option_name} '{value}'. Must be a JSON pointer like '/data' or '/data/0/code'."
        ));
    }
    Ok(())
}

/// Resolve the per-operation endpoint fallback chain:
/// per-op endpoint -> write_endpoint -> endpoint.
fn resolve_endpoint(
    per_op: Option<String>,
    write_endpoint: Option<&str>,
    endpoint: &str,
) -> String {
    per_op
        .unwrap_or_else(|| write_endpoint.map_or_else(|| endpoint.to_string(), ToString::to_string))
}

/// Read write options from the Options resource and build a WriteConfig.
///
/// Thin wrapper around build_write_config (Options is a WASM resource and
/// cannot be constructed in unit tests).
pub(crate) fn write_config_from_options(
    opts: &Options,
    endpoint: &str,
    rowid_column: &str,
) -> Result<WriteConfig, FdwError> {
    build_write_config(&|name| opts.get(name), endpoint, rowid_column)
}

/// Assemble a WriteConfig from already-extracted option values.
///
/// All validation fires here — in begin_modify, before any HTTP request:
/// bad verbs, bad enums, non-pointer paths, body_wrap 'array' without
/// body_root_path, and the required-success_path predicate.
pub(crate) fn build_write_config(
    get: &dyn Fn(&str) -> Option<String>,
    endpoint: &str,
    rowid_column: &str,
) -> Result<WriteConfig, FdwError> {
    let writable = crate::parse_bool_flag(get(OPT_WRITABLE).as_deref());

    let insert_method = get(OPT_INSERT_METHOD)
        .map(|m| parse_http_method(&m, OPT_INSERT_METHOD))
        .transpose()?;
    let update_method = get(OPT_UPDATE_METHOD)
        .map(|m| parse_http_method(&m, OPT_UPDATE_METHOD))
        .transpose()?;
    let delete_method = get(OPT_DELETE_METHOD)
        .map(|m| parse_http_method(&m, OPT_DELETE_METHOD))
        .transpose()?;

    let write_endpoint = get(OPT_WRITE_ENDPOINT);
    let insert_endpoint = resolve_endpoint(
        get(OPT_INSERT_ENDPOINT),
        write_endpoint.as_deref(),
        endpoint,
    );
    let update_endpoint = resolve_endpoint(
        get(OPT_UPDATE_ENDPOINT),
        write_endpoint.as_deref(),
        endpoint,
    );
    let delete_endpoint = resolve_endpoint(
        get(OPT_DELETE_ENDPOINT),
        write_endpoint.as_deref(),
        endpoint,
    );

    let default_location = get(OPT_ROWID_LOCATION)
        .map(|v| parse_rowid_location(&v, OPT_ROWID_LOCATION))
        .transpose()?
        .unwrap_or_default();
    let update_rowid_location = get(OPT_UPDATE_ROWID_LOCATION)
        .map(|v| parse_rowid_location(&v, OPT_UPDATE_ROWID_LOCATION))
        .transpose()?
        .unwrap_or(default_location);
    let delete_rowid_location = get(OPT_DELETE_ROWID_LOCATION)
        .map(|v| parse_rowid_location(&v, OPT_DELETE_ROWID_LOCATION))
        .transpose()?
        .unwrap_or(default_location);

    let rowid_body_key = get(OPT_ROWID_BODY_KEY).unwrap_or_else(|| rowid_column.to_string());
    let rowid_param = get(OPT_ROWID_PARAM).unwrap_or_else(|| rowid_column.to_string());

    let body_root_path = get(OPT_BODY_ROOT_PATH);
    if let Some(ref path) = body_root_path {
        validate_json_pointer(path, OPT_BODY_ROOT_PATH)?;
    }
    let body_wrap = get(OPT_BODY_WRAP)
        .map(|v| parse_body_wrap(&v))
        .transpose()?
        .unwrap_or_default();
    if body_wrap == BodyWrap::Array && body_root_path.is_none() {
        return Err(format!(
            "{OPT_BODY_WRAP} 'array' requires {OPT_BODY_ROOT_PATH} to be set \
             (a bare top-level array body has no valid JSON shape)."
        ));
    }

    let success_path = get(OPT_SUCCESS_PATH);
    if let Some(ref path) = success_path {
        validate_json_pointer(path, OPT_SUCCESS_PATH)?;
    }
    let success_value = get(OPT_SUCCESS_VALUE).unwrap_or_else(|| DEFAULT_SUCCESS_VALUE.to_string());
    let success_status = get(OPT_SUCCESS_STATUS)
        .map(|v| parse_success_status(&v))
        .transpose()?;

    // Required-success_path predicate: a 2xx response can carry a per-record
    // failure code in the body (e.g. HTTP 202 with code != SUCCESS), which a
    // status check alone silently treats as success. When the table shape
    // signals such an API — an explicit status allowlist beyond the
    // trivially-meaningful 200/201/204, or an envelope body — require
    // success_path so the silent-corruption hole stays closed by construction.
    // (Options are table-level, so a single success_path covers every write
    // operation on the table.)
    if writable
        && success_path.is_none()
        && (success_status_is_unusual(success_status.as_deref()) || body_root_path.is_some())
    {
        return Err(format!(
            "{OPT_SUCCESS_PATH} is required for this table: the API may signal \
             per-record failure inside a 2xx response body. Set {OPT_SUCCESS_PATH} \
             (and optionally {OPT_SUCCESS_VALUE}) so failed writes raise an error."
        ));
    }

    Ok(WriteConfig {
        writable,
        insert_method,
        update_method,
        delete_method,
        insert_endpoint,
        update_endpoint,
        delete_endpoint,
        update_rowid_location,
        delete_rowid_location,
        rowid_column: rowid_column.to_string(),
        rowid_body_key,
        rowid_param,
        body_root_path,
        body_wrap,
        success_path,
        success_value,
        success_status,
    })
}

/// Convert a Cell to a type-faithful JSON value.
///
/// Unlike paddle's row_to_body, integers stay JSON numbers (not strings) and
/// Json cells pass through parsed, so nested objects work via jsonb columns.
pub(crate) fn cell_to_json(cell: &Cell, col_name: &str) -> Result<JsonValue, FdwError> {
    let value = match cell {
        Cell::Bool(v) => JsonValue::Bool(*v),
        Cell::I8(v) => JsonValue::from(*v),
        Cell::I16(v) => JsonValue::from(*v),
        Cell::I32(v) => JsonValue::from(*v),
        Cell::I64(v) => JsonValue::from(*v),
        Cell::F32(v) => serde_json::Number::from_f64(f64::from(*v))
            .map(JsonValue::Number)
            .ok_or(format!(
                "column '{col_name}' value is not representable in JSON"
            ))?,
        Cell::F64(v) | Cell::Numeric(v) => serde_json::Number::from_f64(*v)
            .map(JsonValue::Number)
            .ok_or(format!(
                "column '{col_name}' value is not representable in JSON"
            ))?,
        Cell::String(v) => JsonValue::String(v.clone()),
        Cell::Date(secs) => {
            // Cell::Date is seconds since Unix epoch; the host helper takes
            // microseconds (despite its name). Emit the date portion only.
            let usecs = secs
                .checked_mul(1_000_000)
                .ok_or(format!("column '{col_name}' date value out of range"))?;
            let rfc3339 = time::epoch_ms_to_rfc3339(usecs)?;
            let date = rfc3339.split('T').next().unwrap_or(&rfc3339).to_string();
            JsonValue::String(date)
        }
        Cell::Timestamp(usecs) | Cell::Timestamptz(usecs) => {
            JsonValue::String(time::epoch_ms_to_rfc3339(*usecs)?)
        }
        Cell::Json(v) => serde_json::from_str(v)
            .map_err(|e| format!("column '{col_name}' contains invalid JSON: {e}"))?,
        Cell::Uuid(v) => JsonValue::String(v.clone()),
        Cell::Other(_) => {
            return Err(format!(
                "column '{col_name}' type is not supported for writes"
            ));
        }
    };
    Ok(value)
}

/// Convert a Cell to a string for URL path/query placement.
///
/// Mirrors qual_value_to_string (request.rs): the same set of types the read
/// path supports for path parameters. Json/Date/Timestamp/Other return None.
pub(crate) fn cell_to_string(cell: &Cell) -> Option<String> {
    match cell {
        Cell::String(s) => Some(s.clone()),
        Cell::I8(n) => Some(n.to_string()),
        Cell::I16(n) => Some(n.to_string()),
        Cell::I32(n) => Some(n.to_string()),
        Cell::I64(n) => Some(n.to_string()),
        Cell::F32(n) => Some(n.to_string()),
        Cell::F64(n) | Cell::Numeric(n) => Some(n.to_string()),
        Cell::Bool(b) => Some(b.to_string()),
        Cell::Uuid(u) => Some(u.clone()),
        _ => None,
    }
}

/// Build a path-parameter substitution map from row columns/cells, keyed by
/// both original and lowercase column names (the read path's two-key trick).
///
/// The write path substitutes {param} placeholders from the row being
/// written, never from scan quals: the host keeps quals from a prior
/// begin_scan that are stale during modify.
pub(crate) fn row_param_map(cols: &[String], cells: &[Option<Cell>]) -> HashMap<String, String> {
    let mut params: HashMap<String, String> = HashMap::with_capacity(cols.len() * 2);
    for (col, cell) in cols.iter().zip(cells.iter()) {
        if let Some(cell) = cell
            && let Some(value) = cell_to_string(cell)
        {
            params.insert(col.to_lowercase(), value.clone());
            params.insert(col.clone(), value);
        }
    }
    params
}

/// Build the JSON body object from row columns/cells.
///
/// Skips: null cells (keeps PATCH sparse), the 'attrs' catch-all column, the
/// rowid column when skip_rowid is set (UPDATE/DELETE place the rowid via
/// rowid_location instead; INSERT passes None so a user-supplied id column is
/// kept), and columns consumed as {param} path placeholders (no double-emit).
pub(crate) fn build_body(
    cols: &[String],
    cells: &[Option<Cell>],
    skip_rowid: Option<&str>,
    path_params_consumed: &[String],
) -> Result<JsonMap<String, JsonValue>, FdwError> {
    let mut body = JsonMap::new();
    for (col, cell) in cols.iter().zip(cells.iter()) {
        let col_lower = col.to_lowercase();
        if col_lower == ATTRS_COLUMN
            || skip_rowid.is_some_and(|rowid| col_lower == rowid)
            || path_params_consumed.contains(&col_lower)
        {
            continue;
        }
        if let Some(cell) = cell {
            body.insert(col.clone(), cell_to_json(cell, col)?);
        }
    }
    Ok(body)
}

/// Wrap the body object per body_root_path and body_wrap.
///
/// `/data` + Object -> `{"data": {...}}`; `/data` + Array -> `{"data": [{...}]}`.
/// Multi-segment pointers fold inside-out: `/a/b` -> `{"a": {"b": ...}}`.
/// No root path returns the bare object (Array without a root is rejected in
/// build_write_config).
pub(crate) fn wrap_envelope(
    body: JsonMap<String, JsonValue>,
    body_root_path: Option<&str>,
    body_wrap: BodyWrap,
) -> JsonValue {
    let record = JsonValue::Object(body);
    match body_root_path {
        None => record,
        Some(path) => {
            let inner = match body_wrap {
                BodyWrap::Object => record,
                BodyWrap::Array => JsonValue::Array(vec![record]),
            };
            path.trim_start_matches('/')
                .split('/')
                .rev()
                .fold(inner, |acc, segment| {
                    let mut map = JsonMap::new();
                    map.insert(segment.to_string(), acc);
                    JsonValue::Object(map)
                })
        }
    }
}

/// Validate a write response: HTTP status gate, then body-level success check.
///
/// Status: with an explicit success_status allowlist, the status must be
/// listed; otherwise any 2xx is accepted. HTTP 207 Multi-Status is always
/// rejected — per-record outcomes inside a 207 cannot be verified. 404 is an
/// error for writes (the read path's 404-as-empty shortcut does not apply).
///
/// Body: when success_path is set, the response body must parse as JSON and
/// the value at that pointer must equal success_value. This is the primary
/// guard against APIs that signal per-record failure inside a 2xx response.
///
/// Errors are leak-safe: built from the status code and the pre-stripped
/// endpoint, never from the response URL or raw body (which can leak API key
/// query parameters — see the read path's error construction). Vendor detail
/// (the value found at success_path) is included only when include_detail is
/// true, i.e. when no API key is carried in the query string.
pub(crate) fn check_response(
    status_code: u16,
    body: &str,
    cfg: &WriteConfig,
    safe_endpoint: &str,
    include_detail: bool,
) -> FdwResult {
    if status_code == 207 {
        return Err(format!(
            "HTTP 207 Multi-Status response from API endpoint ({safe_endpoint}): \
             per-record outcomes cannot be verified, so the write is treated as failed."
        ));
    }

    let status_ok = match cfg.success_status {
        Some(ref allowlist) => allowlist.contains(&status_code),
        None => (200..=299).contains(&status_code),
    };
    if !status_ok {
        return Err(format!(
            "HTTP {status_code} error from API endpoint ({safe_endpoint})"
        ));
    }

    // A success_path check needs a body to inspect. A genuinely body-less
    // response (204/205/304) carries no per-record failure signal, so the
    // status gate above is the only check that can apply.
    //
    // But when success_path was made mandatory because the table shape signals a
    // body-encoded outcome — an envelope (body_root_path) OR an unusual
    // success_status like 202 — the API is expected to put the per-record result
    // in the body. An empty (non-204/205) 2xx body from such a table cannot be
    // verified, so fail closed rather than silently reporting a possibly-failed
    // write as success.
    if let Some(ref path) = cfg.success_path {
        let body_trimmed = body.trim();
        if body_trimmed.is_empty() {
            // 204/205 are the body-less 2xx statuses and are always allowed
            // empty; any other 2xx with an empty body where a body outcome was
            // required can't be verified.
            let requires_body_outcome = cfg.body_root_path.is_some()
                || success_status_is_unusual(cfg.success_status.as_deref());
            if requires_body_outcome && !matches!(status_code, 204 | 205) {
                return Err(format!(
                    "write to API endpoint ({safe_endpoint}) returned HTTP {status_code} \
                     with an empty body, so success at {OPT_SUCCESS_PATH} '{path}' cannot \
                     be verified; treating the write as failed."
                ));
            }
            return Ok(());
        }
        let parsed: JsonValue = serde_json::from_str(body_trimmed).map_err(|_| {
            format!(
                "write to API endpoint ({safe_endpoint}) returned HTTP {status_code} \
                 but the response body is not valid JSON, so success at \
                 {OPT_SUCCESS_PATH} '{path}' cannot be verified"
            )
        })?;
        let actual = parsed.pointer(path);
        // String values compare directly; bool/number outcome codes compare
        // via their JSON rendering (e.g. success_value 'true' matches JSON
        // true). Objects, arrays, and null never indicate success.
        let matches = actual.is_some_and(|v| match v {
            JsonValue::String(s) => *s == cfg.success_value,
            JsonValue::Bool(b) => b.to_string() == cfg.success_value,
            JsonValue::Number(n) => n.to_string() == cfg.success_value,
            _ => false,
        });
        if !matches {
            let detail = if include_detail {
                actual.map_or_else(
                    || " (no value found at that path)".to_string(),
                    |v| format!(" (got {v})"),
                )
            } else {
                String::new()
            };
            return Err(format!(
                "write to API endpoint ({safe_endpoint}) returned HTTP {status_code} \
                 but {OPT_SUCCESS_PATH} '{path}' did not match expected value \
                 '{}'{detail}",
                cfg.success_value
            ));
        }
    }

    Ok(())
}

impl OpenApiFdw {
    /// Build the URL for a write request.
    ///
    /// Substitutes {param} placeholders from the row-derived params map, then
    /// places the rowid per rowid_location, and appends the API key query
    /// parameter exactly like the read path. When the endpoint template
    /// already consumed the rowid as a {param} (e.g. '/records/{id}'), the
    /// Url and Query locations do not place it a second time.
    ///
    /// Returns (url, path_params_consumed) where path_params_consumed holds
    /// lowercase column names substituted into the path (excluded from the
    /// JSON body by build_body).
    pub(crate) fn build_write_url(
        &self,
        endpoint_template: &str,
        params: &HashMap<String, String>,
        rowid_value: Option<&str>,
        rowid_location: RowidLocation,
        rowid_column: &str,
        rowid_param: &str,
    ) -> Result<(String, Vec<String>), FdwError> {
        // Writes never re-inject values into scan rows; pass a throwaway map.
        let mut throwaway = HashMap::new();
        let (resolved, consumed) =
            Self::substitute_path_params(endpoint_template, params, &mut throwaway)?;

        let base = &self.config.base_url;
        let mut url = match (rowid_value, rowid_location) {
            (Some(id), RowidLocation::Url) if !consumed.contains(&rowid_column.to_lowercase()) => {
                let encoded = urlencoding::encode(id);
                match resolved.find('?') {
                    Some(pos) => {
                        format!("{base}{}/{encoded}{}", &resolved[..pos], &resolved[pos..])
                    }
                    None => format!("{base}{resolved}/{encoded}"),
                }
            }
            (Some(id), RowidLocation::Query)
                if !consumed.contains(&rowid_column.to_lowercase()) =>
            {
                let separator = if resolved.contains('?') { '&' } else { '?' };
                format!(
                    "{base}{resolved}{separator}{}={}",
                    urlencoding::encode(rowid_param),
                    urlencoding::encode(id)
                )
            }
            _ => format!("{base}{resolved}"),
        };

        // Add API key as query parameter if configured (shared with the read
        // path; also avoids a throwaway String allocation per append).
        self.append_api_key_query(&mut url);

        Ok((url, consumed))
    }

    /// Common write hook tail: send the request exactly once, check the
    /// response, and bump stats.
    ///
    /// No guest retry loop: the host's HTTP middleware already retries
    /// transient failures up to 3 times, including on non-idempotent
    /// POST/PATCH (see the data modify docs for the duplication hazard).
    pub(crate) fn execute_write(
        &self,
        method: http::Method,
        url: String,
        body: String,
        cfg: &WriteConfig,
        safe_endpoint: &str,
    ) -> FdwResult {
        let req = http::Request {
            method,
            url,
            headers: self.build_request_headers(),
            body,
        };
        let resp = Self::send_once(&req)?;

        if self.config.debug {
            let log_url = match self.config.api_key_query {
                Some((ref param_name, _)) => redact_query_param(&req.url, param_name),
                None => req.url.clone(),
            };
            utils::report_info(&format!(
                "[openapi_fdw] HTTP {} {} -> {} ({} bytes)",
                method_label(req.method),
                log_url,
                resp.status_code,
                resp.body.len()
            ));
        }

        check_response(
            resp.status_code,
            &resp.body,
            cfg,
            safe_endpoint,
            self.config.api_key_query.is_none(),
        )?;

        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);
        stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);
        Ok(())
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
