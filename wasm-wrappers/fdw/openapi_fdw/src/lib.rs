//! OpenAPI Foreign Data Wrapper
//!
//! A generic Wasm FDW that dynamically parses OpenAPI 3.0+ specifications
//! and exposes API endpoints as PostgreSQL foreign tables.

// Allow usize->i64 casts for stats (expected to fit on 64-bit systems)
#![allow(clippy::cast_possible_wrap)]

#[allow(warnings)]
mod bindings;
mod column_matching;
mod config;
mod pagination;
mod request;
mod response;
mod schema;
mod spec;
mod write;

use serde_json::Value as JsonValue;
use std::collections::HashMap;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats,
        types::{
            Cell, Context, FdwError, FdwResult, ImportForeignSchemaStmt, ImportSchemaType,
            OptionsType, Row,
        },
        utils,
    },
};

use column_matching::{CachedColumn, KeyMatch, normalize_to_alnum, to_camel_case};
use config::ServerConfig;
use pagination::PaginationState;
use schema::generate_all_tables;
use spec::OpenApiSpec;
use write::{RowidLocation, WriteConfig};

/// The OpenAPI FDW state
#[derive(Debug)]
struct OpenApiFdw {
    // Server-level configuration (set once in init, some overridden per table)
    config: ServerConfig,

    // OpenAPI spec (fetched on demand)
    spec: Option<OpenApiSpec>,

    // Current operation state (from table options)
    method: http::Method,
    request_body: String,
    endpoint: String,
    resolved_endpoint: String, // endpoint after path param substitution (for pagination)
    response_path: Option<String>,
    object_path: Option<String>, // Extract nested object from each row (e.g., "/properties" for GeoJSON)
    rowid_col: String,
    cursor_path: String,

    // Pagination state and loop detection
    pagination: PaginationState,

    // Write (DML) configuration, populated in begin_modify; None until then.
    // Kept separate from the scan fields above: Postgres interleaves the
    // foreign scan with modify in the same UPDATE/DELETE statement, so write
    // hooks must not clobber in-flight scan state.
    write: Option<WriteConfig>,

    // Qual values injected as URL path/query params (for injecting back into rows)
    injected_params: HashMap<String, String>,

    // Data buffers
    src_rows: Vec<JsonValue>,
    src_idx: usize,

    // Cached column metadata (populated in begin_scan, avoids WASM crossings in iter_scan)
    cached_columns: Vec<CachedColumn>,
    // Pre-resolved JSON key for each cached column (rebuilt per page in make_request)
    column_key_map: Vec<Option<KeyMatch>>,

    // Limit pushdown for early pagination stop
    src_limit: Option<i64>,
    consumed_row_cnt: i64,

    // Debug row counter (only active when config.debug is true)
    scan_row_count: i64,
}

impl Default for OpenApiFdw {
    fn default() -> Self {
        Self {
            config: ServerConfig::default(),
            spec: None,
            method: http::Method::Get,
            request_body: String::new(),
            endpoint: String::new(),
            resolved_endpoint: String::new(),
            response_path: None,
            object_path: None,
            rowid_col: String::new(),
            cursor_path: String::new(),
            pagination: PaginationState::default(),
            write: None,
            injected_params: HashMap::new(),
            src_rows: Vec::new(),
            src_idx: 0,
            cached_columns: Vec::new(),
            column_key_map: Vec::new(),
            src_limit: None,
            consumed_row_cnt: 0,
            scan_row_count: 0,
        }
    }
}

/// Global FDW instance pointer.
///
/// # Safety
///
/// This static mut is safe because Wasm execution is single-threaded:
/// - No concurrent access is possible (no data races)
/// - Initialized once in init() before any scan/modify methods are called
/// - All access goes through this_mut() which returns exclusive &mut reference
static mut INSTANCE: *mut OpenApiFdw = std::ptr::null_mut::<OpenApiFdw>();
static FDW_NAME: &str = "OpenApiFdw";

const HOST_VERSION_REQUIREMENT: &str = "^0.1.0";
const DEFAULT_PAGE_SIZE_PARAM: &str = "limit";
const DEFAULT_CURSOR_PARAM: &str = "after";
const DEFAULT_ROWID_COLUMN: &str = "id";

/// Validate that a URL starts with http:// or https://.
fn validate_url(url: &str, field_name: &str) -> Result<(), String> {
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(format!(
            "Invalid {field_name}: '{url}'. Must start with http:// or https://"
        ));
    }
    Ok(())
}

/// Parse a string option value as usize, returning a descriptive error.
fn parse_usize_option(value: &str, field_name: &str) -> Result<usize, String> {
    value
        .parse()
        .map_err(|_| format!("Invalid value for '{field_name}': '{value}'"))
}

/// Parse an optional string as a boolean flag, defaulting to `false`.
///
/// Case-insensitive; accepts `true/1/yes/on` as true. Used for every opt-in
/// flag (debug, writable, ...) so `writable 'TRUE'` is not silently read as
/// false.
pub(crate) fn parse_bool_flag(value: Option<&str>) -> bool {
    value.is_some_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "true" | "1" | "yes" | "on"
        )
    })
}

/// Parse an optional string as a boolean flag that defaults to `true`.
///
/// Case-insensitive; only an explicit false-ish value (`false/0/no/off`)
/// disables it. Used for opt-out flags like `include_attrs`.
pub(crate) fn parse_bool_flag_default_true(value: Option<&str>) -> bool {
    !value.is_some_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "false" | "0" | "no" | "off"
        )
    })
}

/// Whether a single qual can be honored remotely for LIMIT pushdown. A qual is
/// honored remotely only when it is a non-OR equality ('=') AND its value is
/// actually serialized to the API and re-injected into returned rows so
/// Postgres's local re-check passes — that is exactly the set that
/// `qual_value_to_string` accepts (String / integer / float / bool / uuid).
/// A '=' qual whose value cannot be stringified (numeric, date, timestamp,
/// json, arrays) is NOT pushed or injected, so Postgres filters it locally and
/// drops source rows; an early source-side LIMIT would then under-return.
/// `value_pushable` must therefore reflect `qual_value_to_string(q).is_some()`.
pub(crate) fn qual_allows_limit_pushdown(
    operator: &str,
    use_or: bool,
    value_pushable: bool,
) -> bool {
    operator == "=" && !use_or && value_pushable
}

/// Reject an UPDATE that changes the rowid column's value. `params` holds the
/// row's new values; `rowid_str` is the old rowid identifying the remote
/// resource. The rowid lives in the URL and is excluded from the body, so a
/// reassignment would be silently dropped — surface it as an error instead.
///
/// **Currently unreachable from a wasm guest, by host design.** The framework
/// strips the rowid column out of `new_row` before calling `update()` — see
/// `exec_foreign_update` in `supabase-wrappers/src/modify.rs`:
///
/// ```text
/// // remove junk attributes, including rowid attribute, from the new row
/// new_row.retain(|(col, _)| is_ft_col && state.rowid_name != col.as_str());
/// ```
///
/// so `params` never contains the rowid and this always returns `Ok(())`.
/// Verified end-to-end: `UPDATE ... SET id = 'r-2' WHERE id = 'r-1'` issues
/// `PATCH /wr_reassign/r-1` (the OLD id) and reports success — the silent drop
/// this was meant to prevent still happens. Closing it requires preserving the
/// new rowid for the guest, which is a `supabase-wrappers` core change. The
/// check is kept so it takes effect the moment that lands; the ignored pg_test
/// `openapi_write_rejects_rowid_reassignment` is the ready regression test.
pub(crate) fn reject_rowid_reassignment(
    params: &HashMap<String, String>,
    rowid_column: &str,
    rowid_str: &str,
) -> Result<(), String> {
    if let Some(new_rowid) = params.get(rowid_column)
        && new_rowid != rowid_str
    {
        return Err(format!(
            "changing the rowid column '{rowid_column}' in UPDATE is not supported \
             (old '{rowid_str}', new '{new_rowid}'): it identifies the remote resource and \
             cannot be reassigned."
        ));
    }
    Ok(())
}

/// Check whether the consumed row count has reached or exceeded the limit.
fn should_stop_scanning(consumed: i64, limit: Option<i64>) -> bool {
    limit.is_some_and(|l| consumed >= l)
}

/// Extract the effective row from a JSON value, optionally dereferencing an object path.
///
/// Used in iter_scan and build_column_key_map to apply object_path
/// (e.g., "/properties" for GeoJSON) to each row before column matching.
pub(crate) fn extract_effective_row<'a>(
    row: &'a JsonValue,
    object_path: Option<&str>,
) -> &'a JsonValue {
    object_path.map_or(row, |path| row.pointer(path).unwrap_or(row))
}

impl OpenApiFdw {
    fn init() {
        let instance = Self::default();
        // SAFETY: Wasm is single-threaded, no concurrent access possible.
        // Box::leak intentionally leaks memory to create a stable 'static pointer
        // that lives for the entire FDW lifetime (until Postgres unloads).
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        // SAFETY: INSTANCE is initialized in init() before any scan/modify
        // methods are called. Wasm is single-threaded, so only one &mut
        // reference exists at a time (no aliasing).
        unsafe {
            assert!(!INSTANCE.is_null(), "OpenApiFdw not initialized");
            &mut (*INSTANCE)
        }
    }

    /// Ensure `base_url` is populated before a scan or write.
    ///
    /// When only `spec_url`/`spec_json` is configured (a documented setup),
    /// `base_url` is derived from the spec's `servers`. Unlike
    /// `import_foreign_schema`, the scan/write lifecycle otherwise never fetches
    /// the spec, leaving `base_url` empty and producing scheme-less request URLs.
    /// Skips the network round-trip when the spec is already parsed.
    fn ensure_base_url(&mut self) -> Result<(), FdwError> {
        if !self.config.base_url.is_empty() {
            return Ok(());
        }
        if self.config.spec_url.is_none() && self.config.spec_json.is_none() {
            return Ok(());
        }
        if self.spec.is_none() {
            // fetch_spec derives base_url from the spec's servers when unset.
            self.fetch_spec()?;
        } else if let Some(url) = self.spec.as_ref().and_then(OpenApiSpec::base_url) {
            let url = url.trim_end_matches('/').to_string();
            validate_url(&url, "base_url (from spec servers)")?;
            self.config.base_url = url;
        }
        Ok(())
    }
}

impl Guest for OpenApiFdw {
    fn host_version_requirement() -> String {
        HOST_VERSION_REQUIREMENT.to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        let opts = ctx.get_options(&OptionsType::Server);

        // Get base_url (optional if spec_url provides servers)
        this.config.base_url = opts
            .get("base_url")
            .unwrap_or_default()
            .trim_end_matches('/')
            .to_string();

        // Validate base_url format if provided
        if !this.config.base_url.is_empty() {
            validate_url(&this.config.base_url, "base_url")?;
        }

        // Get spec_url / spec_json for import_foreign_schema
        this.config.spec_url = opts.get("spec_url");
        this.config.spec_json = opts.get("spec_json");

        // Validate mutual exclusivity
        if this.config.spec_url.is_some() && this.config.spec_json.is_some() {
            return Err("Cannot use both spec_url and spec_json. Choose one.".to_string());
        }

        // Whether to include an 'attrs' jsonb column in IMPORT FOREIGN SCHEMA output.
        // Default is true; only an explicit false-ish value disables it.
        this.config.include_attrs =
            parse_bool_flag_default_true(opts.get("include_attrs").as_deref());

        // Validate spec_url format if provided
        if let Some(ref spec_url) = this.config.spec_url {
            validate_url(spec_url, "spec_url")?;
        }

        this.config.configure_headers(&opts)?;
        this.config.configure_auth(&opts)?;

        // Pagination defaults (page_size=0 means no automatic limit parameter)
        this.config.page_size = match opts.get("page_size") {
            Some(s) => parse_usize_option(&s, "page_size")?,
            None => 0,
        };

        this.config.page_size_param = opts.require_or("page_size_param", DEFAULT_PAGE_SIZE_PARAM);
        this.config.cursor_param = opts.require_or("cursor_param", DEFAULT_CURSOR_PARAM);

        // Maximum pages per scan (default 1000, prevents infinite pagination loops)
        if let Some(s) = opts.get("max_pages") {
            let val = parse_usize_option(&s, "max_pages")?;
            if val == 0 {
                return Err("max_pages must be at least 1".to_string());
            }
            this.config.max_pages = val;
        }

        // Maximum response body size (default 50 MiB)
        if let Some(s) = opts.get("max_response_bytes") {
            this.config.max_response_bytes = parse_usize_option(&s, "max_response_bytes")?;
        }

        // Debug: emit HTTP details and scan stats via INFO when enabled
        this.config.debug = parse_bool_flag(opts.get("debug").as_deref());

        // Save server-level pagination defaults for restoration in begin_scan
        this.config.save_pagination_defaults();

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);

        // Get table options
        this.endpoint = opts.require("endpoint")?;
        this.rowid_col = opts
            .require_or("rowid_column", DEFAULT_ROWID_COLUMN)
            .to_lowercase();

        // HTTP method (default GET, case-insensitive)
        this.method = match opts.get("method") {
            Some(m) if m.eq_ignore_ascii_case("POST") => http::Method::Post,
            _ => http::Method::Get,
        };

        // Request body for POST endpoints
        this.request_body = opts.get("request_body").unwrap_or_default();
        this.response_path = opts.get("response_path");
        this.object_path = opts.get("object_path"); // e.g., "/properties" for GeoJSON
        this.cursor_path = opts.require_or("cursor_path", "");

        // Restore server-level pagination defaults before applying table overrides
        this.config.restore_pagination_defaults();

        // Override pagination params if specified at table level
        if let Some(param) = opts.get("cursor_param") {
            this.config.cursor_param = param;
        }
        if let Some(param) = opts.get("page_size_param") {
            this.config.page_size_param = param;
        }
        if let Some(size) = opts.get("page_size") {
            match size.parse() {
                Ok(parsed) => this.config.page_size = parsed,
                Err(e) => utils::report_warning(&format!(
                    "Invalid page_size '{}': {}. Using default value {}.",
                    size, e, this.config.page_size
                )),
            }
        }

        // Reset pagination and path param state
        this.pagination.reset();
        this.injected_params.clear();

        // Capture LIMIT for early pagination stop — but ONLY when every qual can
        // be honored remotely. A qual is honored remotely only if it is pushed
        // to the API AND re-injected into returned rows (so Postgres's local
        // re-check keeps every source row); build_query_params does that exactly
        // when qual_value_to_string is Some. Any other operator, an OR'd qual, or
        // a '=' whose value can't be stringified (numeric/date/timestamp/json) is
        // filtered locally and drops source rows, so early-stopping at
        // offset+count *source* rows would under-return. In those cases leave
        // src_limit None and let Postgres's Limit node terminate the scan.
        // saturating_add guards the i64 sum.
        let limit_pushdown_safe = ctx.get_quals().iter().all(|q| {
            qual_allows_limit_pushdown(
                &q.operator(),
                q.use_or(),
                Self::qual_value_to_string(q).is_some(),
            )
        });
        this.src_limit = if limit_pushdown_safe {
            ctx.get_limit()
                .map(|v| v.offset().saturating_add(v.count()))
        } else {
            None
        };
        this.consumed_row_cnt = 0;

        // Cache column metadata once to avoid WASM boundary crossings in iter_scan
        this.cached_columns = ctx
            .get_columns()
            .iter()
            .map(|col| {
                let name = col.name();
                let camel_name = to_camel_case(&name);
                let lower_name = name.to_lowercase();
                let alnum_name = normalize_to_alnum(&name);
                CachedColumn {
                    type_oid: col.type_oid(),
                    name,
                    camel_name,
                    lower_name,
                    alnum_name,
                }
            })
            .collect();

        if this.config.debug {
            this.scan_row_count = 0;
        }

        // Resolve base_url from the spec if only spec_url/spec_json was given,
        // otherwise request URLs would be scheme-less at scan time.
        this.ensure_base_url()?;

        // Make initial request
        this.make_request(ctx)?;
        this.pagination.record_first_page();

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // Check if we need to fetch more data
        if this.src_idx >= this.src_rows.len() {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.src_rows.len() as i64);

            // No more pages to fetch
            if this.pagination.is_exhausted() {
                return Ok(None);
            }

            // Check if limit is satisfied - stop pagination early
            if should_stop_scanning(this.consumed_row_cnt, this.src_limit) {
                return Ok(None);
            }

            // Pagination safety: detect loops and enforce page limit
            if this.pagination.exceeds_limit(this.config.max_pages) {
                utils::report_warning(&format!(
                    "Pagination stopped after {} pages (max_pages limit). \
                     Increase max_pages server option if needed.",
                    this.config.max_pages
                ));
                return Ok(None);
            }
            if let Some(reason) = this.pagination.detect_loop() {
                utils::report_warning(&format!("Pagination stopped: {reason}."));
                return Ok(None);
            }

            // Fetch next page
            this.pagination.advance();
            this.make_request(ctx)?;

            // If still no data after fetch, we're done
            if this.src_rows.is_empty() {
                return Ok(None);
            }
        }

        // Convert current row (apply object_path if set, e.g., "/properties" for GeoJSON)
        let src_row = &this.src_rows[this.src_idx];
        let effective_row = extract_effective_row(src_row, this.object_path.as_deref());
        for (col_idx, _) in this.cached_columns.iter().enumerate() {
            let cell = this.json_to_cell_cached(effective_row, col_idx)?;
            row.push(cell.as_ref());
        }

        this.src_idx += 1;
        this.consumed_row_cnt += 1;
        stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);
        if this.config.debug {
            this.scan_row_count += 1;
        }

        Ok(Some(0))
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.pagination.reset();
        this.consumed_row_cnt = 0;
        this.injected_params.clear();
        // Reset the debug row counter too, so a nested-loop re-scan doesn't
        // report a cumulative "Scan complete: N rows" count in end_scan.
        if this.config.debug {
            this.scan_row_count = 0;
        }
        this.make_request(ctx)?;
        this.pagination.record_first_page();
        Ok(())
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        if this.config.debug {
            utils::report_info(&format!(
                "[openapi_fdw] Scan complete: {} rows, {} columns",
                this.scan_row_count,
                this.cached_columns.len()
            ));
        }

        this.src_rows.clear();
        this.src_idx = 0;
        this.cached_columns.clear();
        this.column_key_map.clear();
        Ok(())
    }

    fn begin_modify(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);

        // Read into locals, not the scan fields: an UPDATE/DELETE statement
        // interleaves the foreign scan with modify on this singleton, and
        // clobbering scan state would corrupt in-flight pagination.
        let endpoint = opts.require("endpoint")?;
        let rowid_col = opts
            .require_or("rowid_column", DEFAULT_ROWID_COLUMN)
            .to_lowercase();

        // All write validation fires here, before any HTTP request: the
        // writable gate, bad verbs/enums/pointers, and the required
        // success_path predicate (see build_write_config).
        let cfg = write::write_config_from_options(&opts, &endpoint, &rowid_col)?;
        if !cfg.writable {
            return Err(
                "foreign table is read-only. Set the writable 'true' table option \
                 to enable data modify."
                    .to_string(),
            );
        }
        this.write = Some(cfg);

        // Resolve base_url from the spec's servers here too, not just in
        // begin_scan: a spec-only server (base_url advertised as optional when
        // spec_url/spec_json provides servers) that is written to before any
        // scan would otherwise build a scheme-less write URL. Runs after write
        // validation so a misconfigured or read-only table still fails first.
        this.ensure_base_url()?;
        Ok(())
    }

    fn insert(_ctx: &Context, row: &Row) -> FdwResult {
        let this = Self::this_mut();
        // Borrow the write config rather than cloning it per row: every
        // downstream call takes &self / &WriteConfig.
        let cfg = this
            .write
            .as_ref()
            .ok_or("write configuration is not initialized")?;
        let method = cfg.insert_method.ok_or(
            "INSERT is not enabled for this foreign table. \
             Set the insert_method table option to enable it.",
        )?;

        let cols = row.cols();
        let cells = row.cells();

        // Path params come from the row being written, never from scan quals
        // (the host keeps quals from a prior begin_scan that are stale here).
        let params = write::row_param_map(&cols, &cells);
        let (url, consumed) = this.build_write_url(
            &cfg.insert_endpoint,
            &params,
            None,
            RowidLocation::Url,
            &cfg.rowid_column,
            &cfg.rowid_param,
        )?;

        // INSERT has no rowid placement; a user-supplied id column stays in
        // the body like any other column.
        let body_map = write::build_body(&cols, &cells, None, &consumed)?;
        let body = write::wrap_envelope(body_map, cfg.body_root_path.as_deref(), cfg.body_wrap)
            .to_string();

        let safe_endpoint = cfg
            .insert_endpoint
            .split('?')
            .next()
            .unwrap_or(&cfg.insert_endpoint);
        this.execute_write(method, url, body, cfg, safe_endpoint)
    }

    fn update(_ctx: &Context, rowid: Cell, row: &Row) -> FdwResult {
        let this = Self::this_mut();
        // Borrow (not clone) the write config; downstream calls take &self.
        let cfg = this
            .write
            .as_ref()
            .ok_or("write configuration is not initialized")?;
        let method = cfg.update_method.ok_or(
            "UPDATE is not enabled for this foreign table. \
             Set the update_method table option to enable it.",
        )?;
        let rowid_str =
            write::cell_to_string(&rowid).ok_or("rowid column type is not supported for UPDATE")?;

        let cols = row.cols();
        let cells = row.cells();

        let mut params = write::row_param_map(&cols, &cells);
        // Reject an attempt to change the rowid itself: row_param_map carries the
        // row's new value for the rowid column, while `rowid_str` is the old one.
        reject_rowid_reassignment(&params, &cfg.rowid_column, &rowid_str)?;
        params.insert(cfg.rowid_column.clone(), rowid_str.clone());
        let (url, consumed) = this.build_write_url(
            &cfg.update_endpoint,
            &params,
            Some(&rowid_str),
            cfg.update_rowid_location,
            &cfg.rowid_column,
            &cfg.rowid_param,
        )?;

        // The rowid is placed per update_rowid_location; the row's rowid
        // column is always excluded from the generic body to avoid
        // double-emitting it.
        let mut body_map = write::build_body(&cols, &cells, Some(&cfg.rowid_column), &consumed)?;
        // Only place the rowid in the body when the URL template did not already
        // consume it as a {param} (mirrors build_write_url's Url/Query guards);
        // otherwise it would land in both the path and the body.
        if cfg.update_rowid_location == RowidLocation::Body && !consumed.contains(&cfg.rowid_column)
        {
            // Refuse to overwrite a distinct real column sharing rowid_body_key's
            // name. build_body already excludes rowid_column, so the default key
            // (== rowid_column) never collides; only a custom key can.
            if body_map.contains_key(&cfg.rowid_body_key) {
                return Err(format!(
                    "rowid_body_key '{}' collides with a column of the same name in \
                     the write body; choose a distinct rowid_body_key.",
                    cfg.rowid_body_key
                ));
            }
            body_map.insert(
                cfg.rowid_body_key.clone(),
                write::cell_to_json(&rowid, &cfg.rowid_column)?,
            );
        }
        let body = write::wrap_envelope(body_map, cfg.body_root_path.as_deref(), cfg.body_wrap)
            .to_string();

        let safe_endpoint = cfg
            .update_endpoint
            .split('?')
            .next()
            .unwrap_or(&cfg.update_endpoint);
        this.execute_write(method, url, body, cfg, safe_endpoint)
    }

    fn delete(_ctx: &Context, rowid: Cell) -> FdwResult {
        let this = Self::this_mut();
        // Borrow (not clone) the write config; downstream calls take &self.
        let cfg = this
            .write
            .as_ref()
            .ok_or("write configuration is not initialized")?;
        let method = cfg.delete_method.ok_or(
            "DELETE is not enabled for this foreign table. \
             Set the delete_method table option to enable it.",
        )?;
        let rowid_str =
            write::cell_to_string(&rowid).ok_or("rowid column type is not supported for DELETE")?;

        // Only the rowid is available for DELETE; any other {param} in the
        // endpoint template fails with the missing-parameter error.
        let mut params = HashMap::with_capacity(1);
        params.insert(cfg.rowid_column.clone(), rowid_str.clone());
        let (url, consumed) = this.build_write_url(
            &cfg.delete_endpoint,
            &params,
            Some(&rowid_str),
            cfg.delete_rowid_location,
            &cfg.rowid_column,
            &cfg.rowid_param,
        )?;

        // No body unless the rowid itself is body-placed (e.g. a POST-based
        // soft delete with an enveloped id) AND the URL template didn't already
        // consume it as a {param} (avoid double-placing it in path and body).
        let body = if cfg.delete_rowid_location == RowidLocation::Body
            && !consumed.contains(&cfg.rowid_column)
        {
            let mut body_map = serde_json::Map::new();
            body_map.insert(
                cfg.rowid_body_key.clone(),
                write::cell_to_json(&rowid, &cfg.rowid_column)?,
            );
            write::wrap_envelope(body_map, cfg.body_root_path.as_deref(), cfg.body_wrap).to_string()
        } else {
            String::default()
        };

        let safe_endpoint = cfg
            .delete_endpoint
            .split('?')
            .next()
            .unwrap_or(&cfg.delete_endpoint);
        this.execute_write(method, url, body, cfg, safe_endpoint)
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.write = None;
        Ok(())
    }

    fn import_foreign_schema(
        _ctx: &Context,
        stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        let this = Self::this_mut();

        // Fetch the spec if we haven't already
        if this.spec.is_none() {
            this.fetch_spec()?;
        }

        let spec = this
            .spec
            .as_ref()
            .ok_or("No OpenAPI spec available. Set spec_url or spec_json in server options.")?;

        // Determine filter based on import statement
        let (filter, exclude) = match stmt.list_type {
            ImportSchemaType::All => (None, false),
            ImportSchemaType::LimitTo => (Some(stmt.table_list.as_slice()), false),
            ImportSchemaType::Except => (Some(stmt.table_list.as_slice()), true),
        };

        let tables = generate_all_tables(
            spec,
            &stmt.server_name,
            filter,
            exclude,
            this.config.include_attrs,
        );

        Ok(tables)
    }
}

bindings::export!(OpenApiFdw with_types_in bindings);

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
