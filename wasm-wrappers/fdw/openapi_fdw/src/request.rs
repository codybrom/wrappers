//! HTTP request building, URL construction, and API communication

use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::bindings::supabase::wrappers::{
    http, stats, time,
    types::{Cell, Context, FdwError, FdwResult, Qual, Value},
    utils,
};
use crate::config::ServerConfig;
use crate::spec::OpenApiSpec;
use crate::{FDW_NAME, OpenApiFdw};

const RETRY_AFTER_HEADER: &str = "retry-after";
pub(crate) const MAX_RETRY_DELAY_MS: u64 = 30_000;

/// Compute retry delay from a Retry-After header value (in seconds), capped to max_delay_ms.
pub(crate) fn retry_delay_from_header(secs: u64, max_delay_ms: u64) -> u64 {
    secs.saturating_mul(1000).min(max_delay_ms)
}

/// Compute exponential backoff delay for a retry attempt, capped to max_delay_ms.
pub(crate) fn exponential_backoff_delay(retry_count: u32, max_delay_ms: u64) -> u64 {
    1000u64.saturating_mul(1 << retry_count).min(max_delay_ms)
}

/// Extract the origin (scheme://authority) from a URL for same-origin comparison.
/// Returns everything up to (but not including) the first / after ://.
fn extract_origin(url: &str) -> &str {
    if let Some(scheme_end) = url.find("://") {
        let rest = &url[scheme_end + 3..];
        if let Some(slash) = rest.find('/') {
            &url[..scheme_end + 3 + slash]
        } else {
            url
        }
    } else {
        url
    }
}

/// Redact a query parameter value from a URL for safe logging.
/// Replaces the value of the named parameter with [REDACTED].
///
/// The match must sit on a query-parameter boundary (immediately after `?` or
/// `&`). Without that anchor, a parameter whose name merely *ends with*
/// `param_name` (e.g. `sortkey` vs `key`) would be redacted, leaving the real
/// secret in cleartext. EVERY boundary-anchored occurrence is redacted, not
/// just the first: a secret can appear more than once in a URL (e.g. a
/// pagination `next_url` that echoes the request query string and then has the
/// api_key appended again), and redacting only the first would leak the rest.
pub(crate) fn redact_query_param(url: &str, param_name: &str) -> String {
    let needle = format!("{}=", urlencoding::encode(param_name));
    let bytes = url.as_bytes();
    let mut out = String::with_capacity(url.len());
    let mut copied = 0; // end of what has been copied into `out`
    let mut search_from = 0;
    while let Some(rel) = url[search_from..].find(&needle) {
        let start = search_from + rel;
        let value_start = start + needle.len();
        if start == 0 || matches!(bytes[start - 1], b'?' | b'&') {
            let value_end = url[value_start..]
                .find('&')
                .map_or(url.len(), |i| value_start + i);
            out.push_str(&url[copied..value_start]);
            out.push_str("[REDACTED]");
            copied = value_end;
            search_from = value_end;
        } else {
            // A non-boundary match (e.g. `sortkey=` when redacting `key`): skip
            // past it without redacting, and keep scanning for a real boundary
            // occurrence later in the URL.
            search_from = value_start;
        }
    }
    out.push_str(&url[copied..]);
    out
}

/// Human-readable HTTP method name for debug logging.
pub(crate) fn method_label(method: http::Method) -> &'static str {
    match method {
        http::Method::Get => "GET",
        http::Method::Post => "POST",
        http::Method::Put => "PUT",
        http::Method::Patch => "PATCH",
        http::Method::Delete => "DELETE",
    }
}

/// Whether the guest-side retry loop may safely re-send this method. POST and
/// PATCH are not idempotent, so re-sending a body-bearing request could compound
/// side effects; only safe verbs (GET/PUT/DELETE) are retried on the read path.
pub(crate) fn method_is_idempotent(method: http::Method) -> bool {
    !matches!(method, http::Method::Post | http::Method::Patch)
}

impl OpenApiFdw {
    /// Fetch and parse the OpenAPI spec
    pub(crate) fn fetch_spec(&mut self) -> Result<(), FdwError> {
        if let Some(ref url) = self.config.spec_url {
            // Apply query-string auth to the spec download too — with
            // api_key_location = 'query' the key lives only in the query string,
            // so a spec_url behind the same auth would otherwise 401 at IMPORT.
            let mut spec_url = url.clone();
            self.append_api_key_query(&mut spec_url);
            let req = http::Request {
                method: http::Method::Get,
                url: spec_url,
                headers: self.config.headers.clone(),
                body: String::default(),
            };
            let resp = http::get(&req)?;
            http::error_for_status(&resp).map_err(|_| {
                // Discard opaque error body — may contain URL with credentials
                format!("Failed to fetch OpenAPI spec (HTTP {})", resp.status_code)
            })?;

            if resp.body.len() > self.config.max_response_bytes {
                return Err(format!(
                    "OpenAPI spec too large: {} bytes (limit: {} bytes). \
                     Increase max_response_bytes server option if needed.",
                    resp.body.len(),
                    self.config.max_response_bytes
                ));
            }

            // Try JSON first, fall back to YAML (many OpenAPI specs are published as YAML)
            let spec_json: JsonValue = match serde_json::from_str(&resp.body) {
                Ok(v) => v,
                Err(json_err) => {
                    serde_yaml_ng::from_str::<JsonValue>(&resp.body).map_err(|yaml_err| {
                        format!(
                            "Failed to parse OpenAPI spec as JSON ({json_err}) \
                             or YAML ({yaml_err})"
                        )
                    })?
                }
            };
            let spec = OpenApiSpec::from_json(spec_json)?;

            // Use base_url from spec if not explicitly set
            if self.config.base_url.is_empty() {
                if let Some(url) = spec.base_url() {
                    self.config.base_url = url.trim_end_matches('/').to_string();
                    crate::validate_url(&self.config.base_url, "base_url (from spec servers)")?;
                }
            }

            self.spec = Some(spec);
            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);
        } else if let Some(ref raw_json) = self.config.spec_json {
            if raw_json.len() > self.config.max_response_bytes {
                return Err(format!(
                    "OpenAPI spec_json too large: {} bytes (limit: {} bytes). \
                     Increase max_response_bytes server option if needed.",
                    raw_json.len(),
                    self.config.max_response_bytes
                ));
            }

            let spec_json: JsonValue =
                serde_json::from_str(raw_json).map_err(|e| format!("Invalid spec_json: {e}"))?;
            let spec = OpenApiSpec::from_json(spec_json)?;

            if self.config.base_url.is_empty() {
                if let Some(url) = spec.base_url() {
                    self.config.base_url = url.trim_end_matches('/').to_string();
                    crate::validate_url(&self.config.base_url, "base_url (from spec servers)")?;
                }
            }

            self.spec = Some(spec);
        }
        Ok(())
    }

    /// Extract a qual value as a string
    pub(crate) fn qual_value_to_string(qual: &Qual) -> Option<String> {
        if qual.operator() != "=" {
            return None;
        }
        if let Value::Cell(cell) = qual.value() {
            match cell {
                Cell::String(s) => Some(s),
                Cell::I8(n) => Some(n.to_string()),
                Cell::I16(n) => Some(n.to_string()),
                Cell::I32(n) => Some(n.to_string()),
                Cell::I64(n) => Some(n.to_string()),
                Cell::F32(n) => Some(n.to_string()),
                Cell::F64(n) => Some(n.to_string()),
                Cell::Bool(b) => Some(b.to_string()),
                Cell::Uuid(u) => Some(u),
                _ => None,
            }
        } else {
            None
        }
    }

    /// Resolve a relative or absolute pagination URL against the base URL and endpoint.
    ///
    /// Handles four forms of next_url:
    /// - Absolute URLs (http://..., https://...) -- validated against base_url origin
    /// - Query-only (?page=2) -- resolves against base_url + endpoint
    /// - Absolute paths (/items?page=2) -- resolves against base_url
    /// - Bare relative paths (page/2) -- resolves against base_url/
    ///
    /// Returns an error if an absolute pagination URL points to a different origin
    /// than base_url, which would leak authentication credentials to a third party.
    pub(crate) fn resolve_pagination_url(&self, next_url: &str) -> Result<String, String> {
        if next_url.starts_with("http://") || next_url.starts_with("https://") {
            let next_origin = extract_origin(next_url);
            let base_origin = extract_origin(&self.config.base_url);
            if !next_origin.eq_ignore_ascii_case(base_origin) {
                return Err(format!(
                    "Pagination URL origin mismatch: API returned '{next_origin}' \
                     but base_url is '{base_origin}'. Cross-origin pagination URLs are \
                     rejected to prevent credential leakage. If this API legitimately \
                     uses a different host for pagination, set base_url to match \
                     the pagination host."
                ));
            }
            Ok(next_url.to_string())
        } else if next_url.starts_with('?') {
            // Use resolved_endpoint (post path-param substitution) if available,
            // falling back to the template for endpoints without path params.
            let ep = if self.resolved_endpoint.is_empty() {
                &self.endpoint
            } else {
                &self.resolved_endpoint
            };
            let endpoint_base = ep.split('?').next().unwrap_or(ep);
            Ok(format!("{}{endpoint_base}{next_url}", self.config.base_url))
        } else if next_url.starts_with('/') {
            // Use only the origin (scheme://host) to avoid duplicating any
            // path prefix that base_url may contain (e.g. /v1).
            Ok(format!(
                "{}{next_url}",
                extract_origin(&self.config.base_url)
            ))
        } else {
            Ok(format!("{}/{next_url}", self.config.base_url))
        }
    }

    /// Substitute path parameters in endpoint template from a pre-built
    /// name -> value map (keyed by both original and lowercase names).
    ///
    /// The read path builds the map from WHERE-clause quals (see build_url);
    /// the write path builds it from row columns/cells. Taking a plain map
    /// instead of quals keeps stale scan quals structurally unable to leak
    /// into write URLs (the write path never calls ctx.get_quals()).
    ///
    /// Writes substituted values into injected so they can be re-injected
    /// into result rows (ensuring PostgreSQL's post-filter passes).
    ///
    /// Returns (resolved_endpoint, path_params_used) where path_params_used
    /// contains lowercase names of parameters that were substituted.
    ///
    /// # Errors
    /// Returns an error if required path parameters are missing from params.
    pub(crate) fn substitute_path_params(
        endpoint: &str,
        params: &HashMap<String, String>,
        injected: &mut HashMap<String, String>,
    ) -> Result<(String, Vec<String>), String> {
        if !endpoint.contains('{') {
            return Ok((endpoint.to_string(), Vec::new()));
        }

        let mut endpoint = endpoint.to_string();
        let mut path_params_used: Vec<String> = Vec::new();
        let mut missing_params: Vec<String> = Vec::new();

        // Find all {param} patterns and substitute
        while let Some(start) = endpoint.find('{') {
            if let Some(end) = endpoint[start..].find('}') {
                let param_name = &endpoint[start + 1..start + end];
                let param_lower = param_name.to_lowercase();

                // Try to find a matching value (case-insensitive)
                let value = params.get(&param_lower).or_else(|| params.get(param_name));

                if let Some(val) = value {
                    path_params_used.push(param_lower.clone());
                    // Store the path param for injection into rows (unencoded for PostgreSQL filter)
                    injected.insert(param_lower, val.clone());
                    endpoint = format!(
                        "{}{}{}",
                        &endpoint[..start],
                        urlencoding::encode(val),
                        &endpoint[start + end + 1..]
                    );
                } else {
                    // Track missing parameter and remove the {param} placeholder to continue
                    // parsing. This is safe because OpenAPI path params are always separated
                    // by '/' (e.g., /{a}/{b}), so removing one doesn't mangle the next.
                    missing_params.push(param_name.to_string());
                    endpoint = format!("{}{}", &endpoint[..start], &endpoint[start + end + 1..]);
                }
            } else {
                return Err(format!("Unclosed '{{' in endpoint template: {endpoint}"));
            }
        }

        // Return error if any required path parameters are missing
        if !missing_params.is_empty() {
            return Err(format!(
                "Missing required path parameter(s) in WHERE clause: {}. \
                 Add WHERE {} to your query.",
                missing_params.join(", "),
                missing_params
                    .iter()
                    .map(|p| format!("{p} = '<value>'"))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ));
        }

        Ok((endpoint, path_params_used))
    }

    /// Append the configured query-string API key to an already-built URL,
    /// choosing the correct `?`/`&` separator. Centralizing this ensures no
    /// request path (pagination, single-resource rowid pushdown, spec fetch)
    /// can silently omit query-auth credentials.
    pub(crate) fn append_api_key_query(&self, url: &mut String) {
        if let Some((ref param_name, ref param_value)) = self.config.api_key_query {
            url.push(if url.contains('?') { '&' } else { '?' });
            url.push_str(&urlencoding::encode(param_name));
            url.push('=');
            url.push_str(&urlencoding::encode(param_value));
        }
    }

    /// Build the URL for single-resource (rowid) access: `base/endpoint/{id}`.
    ///
    /// Splits any static query string off the endpoint so the id is appended to
    /// the path rather than the query (e.g. `/search?type=active` → the id joins
    /// `/search`, not the value of `type`), trims a trailing `/` to avoid a
    /// doubled slash, and appends the query-string API key so the request stays
    /// authenticated.
    pub(crate) fn build_single_resource_url(&self, endpoint: &str, id: &str) -> String {
        let (path, query) = match endpoint.split_once('?') {
            Some((p, q)) => (p.trim_end_matches('/'), Some(q)),
            None => (endpoint.trim_end_matches('/'), None),
        };
        let mut url = format!(
            "{}{}/{}",
            self.config.base_url,
            path,
            urlencoding::encode(id)
        );
        if let Some(q) = query {
            url.push('?');
            url.push_str(q);
        }
        self.append_api_key_query(&mut url);
        url
    }

    /// Build query parameters from pagination state, quals, and API key.
    ///
    /// Returns (url_params, injected_entries) where injected_entries are
    /// qual values to merge into self.injected_params for row injection.
    /// Excludes path parameters and rowid column.
    pub(crate) fn build_query_params(
        &self,
        quals: &[Qual],
        path_params_used: &[String],
    ) -> (Vec<String>, Vec<(String, String)>) {
        // Pre-allocate for cursor + page_size + quals + api_key
        let mut params = Vec::with_capacity(quals.len() + 3);
        let mut injected_entries = Vec::new();

        // Add pagination cursor if we have one
        if let Some(cursor) = self.pagination.next.as_ref().and_then(|t| t.as_cursor()) {
            params.push(format!(
                "{}={}",
                urlencoding::encode(&self.config.cursor_param),
                urlencoding::encode(cursor)
            ));
        }

        // Add the page number for page-number pagination. Absent on the first
        // request of a scan, so the API's own default first page applies.
        if let Some(page) = self.pagination.next.as_ref().and_then(|t| t.as_page()) {
            params.push(format!(
                "{}={page}",
                urlencoding::encode(&self.config.page_param)
            ));
        }

        // Add page size if configured, reduced by LIMIT when available.
        // `src_limit` is only Some when the LIMIT can be honored remotely (no
        // locally-filtered quals — see begin_scan), so capping here is safe.
        if self.config.page_size > 0 && !self.config.page_size_param.is_empty() {
            let effective_size = match self.src_limit {
                // usize::try_from guards the 32-bit wasm target, where a raw
                // `as usize` would truncate a LIMIT above u32::MAX.
                Some(limit) if limit > 0 => self
                    .config
                    .page_size
                    .min(usize::try_from(limit).unwrap_or(usize::MAX)),
                _ => self.config.page_size,
            };
            params.push(format!(
                "{}={}",
                urlencoding::encode(&self.config.page_size_param),
                effective_size
            ));
        }

        // Add remaining quals as query params (exclude path params and rowid)
        for qual in quals {
            let field_lower = qual.field().to_lowercase();

            // Skip if used as path param
            if path_params_used.contains(&field_lower) {
                continue;
            }

            // Skip the rowid column
            if field_lower == self.rowid_col {
                continue;
            }

            if let Some(value) = Self::qual_value_to_string(qual) {
                // Track for injection back into rows
                // (so PostgreSQL's WHERE filter passes even if the API doesn't echo it back)
                injected_entries.push((field_lower, value.clone()));
                params.push(format!(
                    "{}={}",
                    urlencoding::encode(&qual.field()),
                    urlencoding::encode(&value)
                ));
            }
        }

        // Add API key as query parameter if configured
        if let Some((ref param_name, ref param_value)) = self.config.api_key_query {
            params.push(format!(
                "{}={}",
                urlencoding::encode(param_name),
                urlencoding::encode(param_value)
            ));
        }

        (params, injected_entries)
    }

    /// Build the URL for a request, handling path parameters and pagination.
    ///
    /// Updates self.injected_params in place (avoids cloning on pagination).
    ///
    /// Supports endpoint templates like:
    /// - /users/{user_id}/posts
    /// - /projects/{org}/{repo}/issues
    /// - /resources/{type}/{id}
    ///
    /// Path parameters are substituted from WHERE clause quals.
    ///
    /// # Errors
    /// Returns an error if required path parameters are missing from the WHERE clause.
    pub(crate) fn build_url(&mut self, ctx: &Context) -> Result<String, String> {
        // Use next_url for pagination if available (injected_params unchanged)
        if let Some(next_url) = self.pagination.next.as_ref().and_then(|t| t.as_url()) {
            let mut url = self.resolve_pagination_url(next_url)?;
            self.append_api_key_query(&mut url);
            return Ok(url);
        }

        let quals = ctx.get_quals();

        // Build a map of qual field -> value for path parameter substitution
        // Pre-allocate for 2 entries per qual (original + lowercase key)
        let mut qual_params: HashMap<String, String> = HashMap::with_capacity(quals.len() * 2);
        for qual in &quals {
            if let Some(value) = Self::qual_value_to_string(qual) {
                // Store both original and lowercase versions for flexible matching
                qual_params.insert(qual.field().to_lowercase(), value.clone());
                qual_params.insert(qual.field(), value);
            }
        }

        // Substitute path parameters (no self borrow — takes &mut injected_params directly)
        let (endpoint, path_params_used) =
            Self::substitute_path_params(&self.endpoint, &qual_params, &mut self.injected_params)?;

        // Store resolved endpoint for pagination (query-only URLs need the
        // substituted path, not the raw template with {param} placeholders).
        self.resolved_endpoint = endpoint.clone();

        // Check for rowid pushdown for single-resource access
        // Only if endpoint doesn't already have path params and rowid qual exists
        if path_params_used.is_empty() {
            if let Some(id_qual) = quals
                .iter()
                .find(|q| q.field().to_lowercase() == self.rowid_col && q.operator() == "=")
            {
                if let Some(id) = Self::qual_value_to_string(id_qual) {
                    self.injected_params
                        .insert(self.rowid_col.clone(), id.clone());
                    return Ok(self.build_single_resource_url(&endpoint, &id));
                }
            }
        }

        // Build query parameters
        let (params, injected_entries) = self.build_query_params(&quals, &path_params_used);
        self.injected_params.extend(injected_entries);

        // Assemble final URL
        let mut url = format!("{}{}", self.config.base_url, endpoint);
        if !params.is_empty() {
            let separator = if url.contains('?') { '&' } else { '?' };
            url.push(separator);
            url.push_str(&params.join("&"));
        }

        Ok(url)
    }

    /// Assemble request headers: configured headers plus the dynamic
    /// session token (auth_token_setting), if one resolves at request time.
    pub(crate) fn build_request_headers(&self) -> Vec<(String, String)> {
        let mut headers = self.config.headers.clone();
        if let Some(ref setting_name) = self.config.auth_token_setting
            && let Some(token) = utils::query_setting(setting_name)
        {
            ServerConfig::apply_session_token(&mut headers, &token, &self.config.auth_token_prefix);
        }
        headers
    }

    /// Dispatch a request to the host exactly once, by verb.
    ///
    /// The write path calls this directly with no guest retry loop: the host's
    /// HTTP middleware already retries transient failures up to 3 times, and
    /// wrapping writes in the read path's retry loop would compound that to up
    /// to nine re-sends of a single non-idempotent POST/PATCH.
    pub(crate) fn send_once(req: &http::Request) -> Result<http::Response, FdwError> {
        match req.method {
            http::Method::Get => http::get(req),
            http::Method::Post => http::post(req),
            http::Method::Put => http::put(req),
            http::Method::Patch => http::patch(req),
            http::Method::Delete => http::delete(req),
        }
    }

    /// Make a request to the API with automatic rate limit handling
    pub(crate) fn make_request(&mut self, ctx: &Context) -> FdwResult {
        let url = self.build_url(ctx)?;

        let req = http::Request {
            method: self.method,
            url,
            headers: self.build_request_headers(),
            body: self.request_body.clone(),
        };

        // Retry loop for transient errors (HTTP 429 rate limit, 502/503 server errors)
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 3;

        // Only the guest-side loop retries idempotent verbs. POST/PATCH (e.g. a
        // POST-as-query read) are not idempotent, and the host HTTP middleware
        // already retries transient failures; retrying here too would compound
        // to several re-sends of one body-bearing request. The write path avoids
        // this entirely via send_once.
        let idempotent = method_is_idempotent(self.method);

        let resp = loop {
            let resp = Self::send_once(&req)?;

            let is_retryable = idempotent && matches!(resp.status_code, 429 | 502 | 503);
            if is_retryable {
                if retry_count >= MAX_RETRIES {
                    let hint = if resp.status_code == 429 {
                        " Consider adding a page_size option to reduce request frequency."
                    } else {
                        ""
                    };
                    return Err(format!(
                        "API request failed with HTTP {} after {MAX_RETRIES} retries.{hint}",
                        resp.status_code
                    ));
                }

                // Try to get retry delay from Retry-After header (case-insensitive),
                // capped to prevent absurdly long waits from malicious/buggy servers
                let delay_ms = resp
                    .headers
                    .iter()
                    .find(|h| h.0.to_lowercase() == RETRY_AFTER_HEADER)
                    .and_then(|h| h.1.parse::<u64>().ok())
                    .map(|secs| retry_delay_from_header(secs, MAX_RETRY_DELAY_MS))
                    .unwrap_or_else(|| exponential_backoff_delay(retry_count, MAX_RETRY_DELAY_MS));

                time::sleep(delay_ms);
                retry_count += 1;
                continue;
            }

            break resp;
        };

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

        // Handle 404 as empty result (no matching resource)
        if resp.status_code == 404 {
            self.src_rows = Vec::new();
            self.src_idx = 0;
            self.pagination.clear_next();
            return Ok(());
        }

        http::error_for_status(&resp).map_err(|_| {
            // Discard the opaque error body from error_for_status — it may
            // contain the full request URL, which leaks API key query params
            // when api_key_location = 'query'.
            format!(
                "HTTP {} error from API endpoint ({})",
                resp.status_code,
                self.endpoint.split('?').next().unwrap_or(&self.endpoint)
            )
        })?;

        if resp.body.len() > self.config.max_response_bytes {
            return Err(format!(
                "Response body too large: {} bytes (limit: {} bytes). \
                 Increase max_response_bytes server option if needed.",
                resp.body.len(),
                self.config.max_response_bytes
            ));
        }

        let mut resp_json: JsonValue =
            serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

        // Handle pagination before extracting data (borrows resp_json).
        // Headers are needed for RFC 8288 Link-header pagination (e.g., GitHub).
        self.handle_pagination(&resp_json, &resp.headers);

        // Extract data by taking ownership (avoids cloning the array)
        self.src_rows = self.extract_data(&mut resp_json)?;
        self.src_idx = 0;

        // Build column key map for O(1) lookups during iter_scan
        self.build_column_key_map();

        // Debug: warn once if object_path doesn't match response structure
        if self.config.debug {
            if let Some(ref path) = self.object_path {
                if let Some(first_row) = self.src_rows.first() {
                    if first_row.pointer(path).is_none() {
                        utils::report_info(&format!(
                            "[openapi_fdw] object_path '{path}' not found in response. \
                             Falling back to full row object."
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "request_tests.rs"]
mod tests;
