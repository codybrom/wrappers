//! OpenAPI Foreign Data Wrapper
//!
//! A generic Wasm FDW that dynamically parses OpenAPI 3.0+ specifications
//! and exposes API endpoints as PostgreSQL foreign tables.

#[allow(warnings)]
mod bindings;
mod schema;
mod spec;

use serde_json::{Map as JsonMap, Value as JsonValue};

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{
            Cell, Column, Context, FdwError, FdwResult, ImportForeignSchemaStmt, ImportSchemaType,
            OptionsType, Row, TypeOid, Value,
        },
        utils,
    },
};

use schema::generate_all_tables;
use spec::OpenApiSpec;

/// The OpenAPI FDW state
#[derive(Debug, Default)]
struct OpenApiFdw {
    // Configuration from server options
    base_url: String,
    headers: Vec<(String, String)>,
    spec: Option<OpenApiSpec>,
    spec_url: Option<String>,

    // Current operation state (from table options)
    endpoint: String,
    response_path: Option<String>,
    object_path: Option<String>,  // Extract nested object from each row (e.g., "/properties" for GeoJSON)
    rowid_col: String,

    // Pagination configuration
    cursor_param: String,
    cursor_path: String,
    page_size: usize,
    page_size_param: String,

    // Pagination state
    next_cursor: Option<String>,
    next_url: Option<String>,

    // Data buffers
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

static mut INSTANCE: *mut OpenApiFdw = std::ptr::null_mut::<OpenApiFdw>();
static FDW_NAME: &str = "OpenApiFdw";

impl OpenApiFdw {
    fn init_instance() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    /// Fetch and parse the OpenAPI spec
    fn fetch_spec(&mut self) -> Result<(), FdwError> {
        if let Some(ref url) = self.spec_url {
            let req = http::Request {
                method: http::Method::Get,
                url: url.clone(),
                headers: self.headers.clone(),
                body: String::default(),
            };
            let resp = http::get(&req)?;
            http::error_for_status(&resp)
                .map_err(|err| format!("Failed to fetch OpenAPI spec: {}: {}", err, resp.body))?;

            let spec_json: JsonValue =
                serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;
            self.spec = Some(OpenApiSpec::from_json(&spec_json)?);

            // Use base_url from spec if not explicitly set
            if self.base_url.is_empty() {
                if let Some(ref spec) = self.spec {
                    if let Some(url) = spec.base_url() {
                        self.base_url = url.trim_end_matches('/').to_string();
                    }
                }
            }

            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);
        }
        Ok(())
    }

    /// Build the URL for a request, handling pushdown and pagination
    fn build_url(&self, ctx: &Context) -> String {
        let quals = ctx.get_quals();

        // Check for ID pushdown (WHERE id = 'x') - case insensitive comparison
        let id_pushdown = quals.iter().find(|q| {
            q.field().to_lowercase() == self.rowid_col.to_lowercase() && q.operator() == "="
        });

        if let Some(id_qual) = id_pushdown {
            if let Value::Cell(Cell::String(id)) = id_qual.value() {
                // Direct resource access: /endpoint/{id}
                return format!("{}{}/{}", self.base_url, self.endpoint, id);
            }
        }

        // Build URL with pagination
        let url = if let Some(ref next_url) = self.next_url {
            next_url.clone()
        } else {
            let mut base = format!("{}{}", self.base_url, self.endpoint);

            // Add pagination cursor if we have one
            let mut params = Vec::new();

            if let Some(ref cursor) = self.next_cursor {
                params.push(format!("{}={}", self.cursor_param, cursor));
            }

            // Add page size if configured
            if self.page_size > 0 && !self.page_size_param.is_empty() {
                params.push(format!("{}={}", self.page_size_param, self.page_size));
            }

            // Add query params from quals (for supported fields)
            for qual in &quals {
                // Skip the rowid column for list queries
                if qual.field() == self.rowid_col {
                    continue;
                }

                // Only push down simple equality quals
                if qual.operator() == "=" {
                    if let Value::Cell(cell) = qual.value() {
                        let value = match cell {
                            Cell::String(s) => s,
                            Cell::I32(n) => n.to_string(),
                            Cell::I64(n) => n.to_string(),
                            Cell::Bool(b) => b.to_string(),
                            _ => continue,
                        };
                        params.push(format!("{}={}", qual.field(), value));
                    }
                }
            }

            if !params.is_empty() {
                base.push('?');
                base.push_str(&params.join("&"));
            }

            base
        };

        url
    }

    /// Make a request to the API
    fn make_request(&mut self, ctx: &Context) -> FdwResult {
        let url = self.build_url(ctx);

        let req = http::Request {
            method: http::Method::Get,
            url,
            headers: self.headers.clone(),
            body: String::default(),
        };

        let resp = http::get(&req)?;

        // Handle 404 as empty result (no matching resource)
        if resp.status_code == 404 {
            self.src_rows = Vec::new();
            self.src_idx = 0;
            self.next_cursor = None;
            self.next_url = None;
            return Ok(());
        }

        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

        let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

        // Extract data from response using response_path or auto-detect
        self.src_rows = self.extract_data(&resp_json)?;
        self.src_idx = 0;

        // Handle pagination
        self.handle_pagination(&resp_json);

        Ok(())
    }

    /// Extract the data array from the response
    fn extract_data(&self, resp: &JsonValue) -> Result<Vec<JsonValue>, FdwError> {
        // If response_path is specified, use it
        if let Some(ref path) = self.response_path {
            let data = resp
                .pointer(path)
                .ok_or_else(|| format!("Response path '{}' not found in response", path))?;

            return if data.is_array() {
                Ok(data.as_array().cloned().unwrap_or_default())
            } else if data.is_object() {
                Ok(vec![data.clone()])
            } else {
                Err("Response data is not an array or object".to_string())
            };
        }

        // Auto-detect: try common patterns
        // 1. Direct array response
        if resp.is_array() {
            return Ok(resp.as_array().cloned().unwrap_or_default());
        }

        // 2. Common wrapper patterns
        if let Some(obj) = resp.as_object() {
            // Try common data field names (including GeoJSON features)
            for key in &["data", "results", "items", "records", "entries", "features"] {
                if let Some(data) = obj.get(*key) {
                    if data.is_array() {
                        return Ok(data.as_array().cloned().unwrap_or_default());
                    } else if data.is_object() {
                        return Ok(vec![data.clone()]);
                    }
                }
            }

            // Single object response
            return Ok(vec![resp.clone()]);
        }

        Err("Unable to extract data from response".to_string())
    }

    /// Handle pagination from the response
    fn handle_pagination(&mut self, resp: &JsonValue) {
        self.next_cursor = None;
        self.next_url = None;

        // Try to get pagination cursor from response
        if !self.cursor_path.is_empty() {
            if let Some(cursor) = resp.pointer(&self.cursor_path) {
                if let Some(s) = cursor.as_str() {
                    if !s.is_empty() {
                        self.next_cursor = Some(s.to_string());
                        return;
                    }
                }
            }
        }

        // Try common pagination patterns
        if resp.as_object().is_some() {
            // Check for next URL
            for path in &[
                "/meta/pagination/next",
                "/pagination/next",
                "/links/next",
                "/next",
                "/_links/next/href",
            ] {
                if let Some(next) = resp.pointer(path) {
                    if let Some(url) = next.as_str() {
                        if !url.is_empty() {
                            self.next_url = Some(url.to_string());
                            return;
                        }
                    }
                }
            }

            // Check for has_more with cursor
            let has_more = resp
                .pointer("/meta/pagination/has_more")
                .or_else(|| resp.pointer("/has_more"))
                .or_else(|| resp.pointer("/pagination/has_more"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            if has_more {
                // Try to find next cursor
                for path in &[
                    "/meta/pagination/next_cursor",
                    "/pagination/next_cursor",
                    "/next_cursor",
                    "/cursor",
                ] {
                    if let Some(cursor) = resp.pointer(path) {
                        if let Some(s) = cursor.as_str() {
                            if !s.is_empty() {
                                self.next_cursor = Some(s.to_string());
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Convert a JSON value to a Cell based on the target column type
    fn json_to_cell(&self, src_row: &JsonValue, tgt_col: &Column) -> Result<Option<Cell>, FdwError> {
        let tgt_col_name = tgt_col.name();

        // Special handling for 'attrs' column - returns entire row as JSON
        if tgt_col_name == "attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        // Handle column name matching with multiple strategies:
        // 1. Exact match
        // 2. snake_case to camelCase conversion
        // 3. Case-insensitive match (PostgreSQL lowercases column names)
        let src = src_row.as_object().and_then(|obj| {
            obj.get(&tgt_col_name)
                .or_else(|| {
                    // Try camelCase version (snake_case to camelCase)
                    let camel = to_camel_case(&tgt_col_name);
                    obj.get(&camel)
                })
                .or_else(|| {
                    // Case-insensitive match for when PostgreSQL lowercases column names
                    obj.iter()
                        .find(|(k, _)| k.to_lowercase() == tgt_col_name.to_lowercase())
                        .map(|(_, v)| v)
                })
        });

        let src = match src {
            Some(v) if !v.is_null() => v,
            _ => return Ok(None),
        };

        // Type conversion based on target column type
        let cell = match tgt_col.type_oid() {
            TypeOid::Bool => src.as_bool().map(Cell::Bool),
            TypeOid::I8 => src.as_i64().map(|v| Cell::I8(v as i8)),
            TypeOid::I16 => src.as_i64().map(|v| Cell::I16(v as i16)),
            TypeOid::I32 => src.as_i64().map(|v| Cell::I32(v as i32)),
            TypeOid::I64 => src.as_i64().map(Cell::I64),
            TypeOid::F32 => src.as_f64().map(|v| Cell::F32(v as f32)),
            TypeOid::F64 => src.as_f64().map(Cell::F64),
            TypeOid::Numeric => src.as_f64().map(Cell::Numeric),
            TypeOid::String => {
                // Handle both string and non-string JSON values
                if let Some(s) = src.as_str() {
                    Some(Cell::String(s.to_owned()))
                } else {
                    Some(Cell::String(src.to_string()))
                }
            }
            TypeOid::Date => {
                if let Some(s) = src.as_str() {
                    let ts = time::parse_from_rfc3339(s)?;
                    Some(Cell::Date(ts / 1_000_000))
                } else {
                    None
                }
            }
            TypeOid::Timestamp => {
                if let Some(s) = src.as_str() {
                    let ts = time::parse_from_rfc3339(s)?;
                    Some(Cell::Timestamp(ts))
                } else {
                    None
                }
            }
            TypeOid::Timestamptz => {
                if let Some(s) = src.as_str() {
                    let ts = time::parse_from_rfc3339(s)?;
                    Some(Cell::Timestamptz(ts))
                } else {
                    None
                }
            }
            TypeOid::Json => Some(Cell::Json(src.to_string())),
            TypeOid::Uuid => src.as_str().map(|v| Cell::String(v.to_owned())),
            _ => Some(Cell::Json(src.to_string())),
        };

        Ok(cell)
    }

    /// Convert a Row to a JSON body for POST/PATCH requests
    fn row_to_body(&self, row: &Row) -> Result<String, FdwError> {
        let mut map = JsonMap::new();

        for (col_name, cell) in row.cols().iter().zip(row.cells().iter()) {
            // Skip the attrs column and empty cells
            if col_name == "attrs" {
                continue;
            }

            if let Some(cell) = cell {
                let value = match cell {
                    Cell::Bool(v) => JsonValue::Bool(*v),
                    Cell::I8(v) => JsonValue::Number((*v).into()),
                    Cell::I16(v) => JsonValue::Number((*v).into()),
                    Cell::I32(v) => JsonValue::Number((*v).into()),
                    Cell::I64(v) => JsonValue::Number((*v).into()),
                    Cell::F32(v) => serde_json::Number::from_f64(*v as f64)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null),
                    Cell::F64(v) => serde_json::Number::from_f64(*v)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null),
                    Cell::Numeric(v) => serde_json::Number::from_f64(*v)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null),
                    Cell::String(v) => JsonValue::String(v.clone()),
                    Cell::Date(v) => {
                        JsonValue::String(time::epoch_ms_to_rfc3339(v * 1_000_000)?)
                    }
                    Cell::Timestamp(v) | Cell::Timestamptz(v) => {
                        JsonValue::String(time::epoch_ms_to_rfc3339(*v)?)
                    }
                    Cell::Json(v) => serde_json::from_str(v).unwrap_or(JsonValue::Null),
                    Cell::Uuid(v) => JsonValue::String(v.clone()),
                    Cell::Other(v) => JsonValue::String(v.clone()),
                };
                map.insert(col_name.clone(), value);
            }
        }

        Ok(JsonValue::Object(map).to_string())
    }
}

/// Convert snake_case to camelCase
fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
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

impl Guest for OpenApiFdw {
    fn host_version_requirement() -> String {
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init_instance();
        let this = Self::this_mut();

        let opts = ctx.get_options(&OptionsType::Server);

        // Get base_url (optional if spec_url provides servers)
        this.base_url = opts
            .get("base_url")
            .unwrap_or_default()
            .trim_end_matches('/')
            .to_string();

        // Get spec_url for import_foreign_schema
        this.spec_url = opts.get("spec_url");

        // Set up authentication headers
        this.headers.push(("user-agent".to_owned(), "Wrappers OpenAPI FDW".to_string()));
        this.headers.push(("content-type".to_owned(), "application/json".to_string()));
        this.headers.push(("accept".to_owned(), "application/json".to_string()));

        // API Key authentication
        let api_key = opts.get("api_key").or_else(|| {
            opts.get("api_key_id")
                .and_then(|key_id| utils::get_vault_secret(&key_id))
        });

        if let Some(key) = api_key {
            let header_name = opts.require_or("api_key_header", "Authorization");
            let prefix = opts.get("api_key_prefix");

            let header_value = match (header_name.as_str(), prefix) {
                ("Authorization", None) => format!("Bearer {}", key),
                ("Authorization", Some(p)) => format!("{} {}", p, key),
                (_, Some(p)) => format!("{} {}", p, key),
                (_, None) => key,
            };

            this.headers.push((header_name.to_lowercase(), header_value));
        }

        // Bearer token authentication (alternative to api_key)
        let bearer_token = opts.get("bearer_token").or_else(|| {
            opts.get("bearer_token_id")
                .and_then(|token_id| utils::get_vault_secret(&token_id))
        });

        if let Some(token) = bearer_token {
            this.headers
                .push(("authorization".to_owned(), format!("Bearer {}", token)));
        }

        // Pagination defaults
        this.page_size = opts
            .get("page_size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        this.page_size_param = opts.require_or("page_size_param", "limit");
        this.cursor_param = opts.require_or("cursor_param", "after");

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);

        // Get table options
        this.endpoint = opts.require("endpoint")?;
        this.rowid_col = opts.require_or("rowid_column", "id");
        this.response_path = opts.get("response_path");
        this.object_path = opts.get("object_path");  // e.g., "/properties" for GeoJSON
        this.cursor_path = opts.require_or("cursor_path", "");

        // Override pagination params if specified at table level
        if let Some(param) = opts.get("cursor_param") {
            this.cursor_param = param;
        }
        if let Some(param) = opts.get("page_size_param") {
            this.page_size_param = param;
        }
        if let Some(size) = opts.get("page_size") {
            this.page_size = size.parse().unwrap_or(this.page_size);
        }

        // Reset pagination state
        this.next_cursor = None;
        this.next_url = None;

        // Make initial request
        this.make_request(ctx)?;

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // Check if we need to fetch more data
        if this.src_idx >= this.src_rows.len() {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.src_rows.len() as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.src_rows.len() as i64);

            // No more pages to fetch
            if this.next_cursor.is_none() && this.next_url.is_none() {
                return Ok(None);
            }

            // Fetch next page
            this.make_request(ctx)?;

            // If still no data after fetch, we're done
            if this.src_rows.is_empty() {
                return Ok(None);
            }
        }

        // Convert current row (apply object_path if set, e.g., "/properties" for GeoJSON)
        let src_row = &this.src_rows[this.src_idx];
        let effective_row = if let Some(ref path) = this.object_path {
            src_row.pointer(path).unwrap_or(src_row)
        } else {
            src_row
        };
        for tgt_col in ctx.get_columns() {
            let cell = this.json_to_cell(effective_row, &tgt_col)?;
            row.push(cell.as_ref());
        }

        this.src_idx += 1;

        Ok(Some(0))
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.next_cursor = None;
        this.next_url = None;
        this.make_request(ctx)
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        this.src_idx = 0;
        Ok(())
    }

    fn begin_modify(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);

        this.endpoint = opts.require("endpoint")?;
        this.rowid_col = opts.require("rowid_column")?;

        Ok(())
    }

    fn insert(_ctx: &Context, row: &Row) -> FdwResult {
        let this = Self::this_mut();

        let url = format!("{}{}", this.base_url, this.endpoint);
        let body = this.row_to_body(row)?;

        let req = http::Request {
            method: http::Method::Post,
            url,
            headers: this.headers.clone(),
            body,
        };

        let resp = http::post(&req)?;
        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

        stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);

        Ok(())
    }

    fn update(_ctx: &Context, rowid: Cell, row: &Row) -> FdwResult {
        let this = Self::this_mut();

        let id = match rowid {
            Cell::String(s) => s,
            Cell::I32(n) => n.to_string(),
            Cell::I64(n) => n.to_string(),
            _ => return Err("Invalid rowid column value type".to_string()),
        };

        let url = format!("{}{}/{}", this.base_url, this.endpoint, id);
        let body = this.row_to_body(row)?;

        let req = http::Request {
            method: http::Method::Patch,
            url,
            headers: this.headers.clone(),
            body,
        };

        let resp = http::patch(&req)?;
        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

        stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);

        Ok(())
    }

    fn delete(_ctx: &Context, rowid: Cell) -> FdwResult {
        let this = Self::this_mut();

        let id = match rowid {
            Cell::String(s) => s,
            Cell::I32(n) => n.to_string(),
            Cell::I64(n) => n.to_string(),
            _ => return Err("Invalid rowid column value type".to_string()),
        };

        let url = format!("{}{}/{}", this.base_url, this.endpoint, id);

        let req = http::Request {
            method: http::Method::Delete,
            url,
            headers: this.headers.clone(),
            body: String::default(),
        };

        let resp = http::delete(&req)?;
        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

        stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);

        Ok(())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
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
            .ok_or("No OpenAPI spec available. Set spec_url in server options.")?;

        // Determine filter based on import statement
        let (filter, exclude) = match stmt.list_type {
            ImportSchemaType::All => (None, false),
            ImportSchemaType::LimitTo => (Some(stmt.table_list.as_slice()), false),
            ImportSchemaType::Except => (Some(stmt.table_list.as_slice()), true),
        };

        let tables = generate_all_tables(spec, &stmt.server_name, filter, exclude);

        Ok(tables)
    }
}

bindings::export!(OpenApiFdw with_types_in bindings);
