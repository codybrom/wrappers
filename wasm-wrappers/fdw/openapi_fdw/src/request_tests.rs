use super::*;
use crate::config::ServerConfig;
use crate::pagination::PaginationState;

// --- resolve_pagination_url tests ---

fn make_fdw_for_url(base_url: &str, endpoint: &str) -> OpenApiFdw {
    OpenApiFdw {
        config: ServerConfig {
            base_url: base_url.to_string(),
            ..Default::default()
        },
        endpoint: endpoint.to_string(),
        resolved_endpoint: endpoint.to_string(),
        ..Default::default()
    }
}

#[test]
fn test_resolve_pagination_url_absolute_https() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw
        .resolve_pagination_url("https://api.example.com/items?page=2&limit=10")
        .unwrap();
    assert_eq!(url, "https://api.example.com/items?page=2&limit=10");
}

#[test]
fn test_resolve_pagination_url_absolute_http() {
    let fdw = make_fdw_for_url("http://mockserver:1080", "/items");
    let url = fdw
        .resolve_pagination_url("http://mockserver:1080/items?page=2")
        .unwrap();
    assert_eq!(url, "http://mockserver:1080/items?page=2");
}

#[test]
fn test_resolve_pagination_url_query_only() {
    // "?page=2" should resolve against base_url + endpoint
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw.resolve_pagination_url("?page=2").unwrap();
    assert_eq!(url, "https://api.example.com/items?page=2");
}

#[test]
fn test_resolve_pagination_url_query_only_strips_existing_query() {
    // If endpoint already has query params, only the path part is used
    let fdw = make_fdw_for_url("https://api.example.com", "/items?status=active");
    let url = fdw.resolve_pagination_url("?page=2").unwrap();
    assert_eq!(url, "https://api.example.com/items?page=2");
}

#[test]
fn test_resolve_pagination_url_absolute_path() {
    // "/items?page=2" should resolve against base_url
    let fdw = make_fdw_for_url("https://api.example.com", "/old-endpoint");
    let url = fdw
        .resolve_pagination_url("/items?page=2&limit=50")
        .unwrap();
    assert_eq!(url, "https://api.example.com/items?page=2&limit=50");
}

#[test]
fn test_resolve_pagination_url_bare_relative() {
    // "page/2" should resolve against base_url/
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw.resolve_pagination_url("page/2").unwrap();
    assert_eq!(url, "https://api.example.com/page/2");
}

#[test]
fn test_resolve_pagination_url_empty_string() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw.resolve_pagination_url("").unwrap();
    assert_eq!(url, "https://api.example.com/");
}

#[test]
fn test_resolve_pagination_url_trailing_slash_base() {
    // base_url is already trimmed of trailing slash in init()
    let fdw = make_fdw_for_url("https://api.example.com", "/v2/items");
    let url = fdw.resolve_pagination_url("/v2/items?offset=100").unwrap();
    assert_eq!(url, "https://api.example.com/v2/items?offset=100");
}

// --- Cross-origin pagination rejection ---

#[test]
fn test_resolve_pagination_url_cross_origin_rejected() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let result = fdw.resolve_pagination_url("https://evil.com/exfiltrate?token=abc");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("origin mismatch"));
    assert!(err.contains("credential leakage"));
}

#[test]
fn test_resolve_pagination_url_cross_origin_different_subdomain() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let result = fdw.resolve_pagination_url("https://cdn.example.com/items?page=2");
    assert!(result.is_err());
}

#[test]
fn test_resolve_pagination_url_cross_origin_different_port() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let result = fdw.resolve_pagination_url("https://api.example.com:8443/items?page=2");
    assert!(result.is_err());
}

#[test]
fn test_resolve_pagination_url_cross_origin_http_vs_https() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let result = fdw.resolve_pagination_url("http://api.example.com/items?page=2");
    assert!(result.is_err());
}

#[test]
fn test_resolve_pagination_url_same_origin_with_path() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw
        .resolve_pagination_url("https://api.example.com/v2/items?page=2")
        .unwrap();
    assert_eq!(url, "https://api.example.com/v2/items?page=2");
}

#[test]
fn test_resolve_pagination_url_same_origin_with_port() {
    let fdw = make_fdw_for_url("http://mockserver:1080", "/items");
    let url = fdw
        .resolve_pagination_url("http://mockserver:1080/next?cursor=abc")
        .unwrap();
    assert_eq!(url, "http://mockserver:1080/next?cursor=abc");
}

#[test]
fn test_resolve_pagination_url_same_origin_case_insensitive() {
    let fdw = make_fdw_for_url("https://API.Example.COM", "/items");
    let url = fdw
        .resolve_pagination_url("https://api.example.com/items?page=2")
        .unwrap();
    assert_eq!(url, "https://api.example.com/items?page=2");
}

// --- Pagination bug fix regression tests ---

#[test]
fn test_resolve_pagination_url_query_only_with_path_params() {
    // Bug fix: query-only pagination should use resolved_endpoint (post-substitution),
    // not the raw template with {param} placeholders
    let fdw = OpenApiFdw {
        config: ServerConfig {
            base_url: "https://api.example.com".to_string(),
            ..Default::default()
        },
        endpoint: "/pets/{pet_id}/toys".to_string(),
        resolved_endpoint: "/pets/123/toys".to_string(),
        ..Default::default()
    };
    let url = fdw.resolve_pagination_url("?page=2").unwrap();
    assert_eq!(url, "https://api.example.com/pets/123/toys?page=2");
}

#[test]
fn test_resolve_pagination_url_absolute_path_with_base_path() {
    // Bug fix: absolute-path pagination should use only the origin, not the
    // full base_url, to avoid duplicating the path prefix
    let fdw = make_fdw_for_url("https://api.example.com/v1", "/items");
    let url = fdw.resolve_pagination_url("/v1/items?page=2").unwrap();
    assert_eq!(url, "https://api.example.com/v1/items?page=2");
}

#[test]
fn test_resolve_pagination_url_absolute_path_different_path() {
    // Absolute path that differs from base_url path — uses origin only
    let fdw = make_fdw_for_url("https://api.example.com/v1", "/items");
    let url = fdw.resolve_pagination_url("/v2/items?page=2").unwrap();
    assert_eq!(url, "https://api.example.com/v2/items?page=2");
}

// --- extract_origin tests ---

#[test]
fn test_extract_origin_https() {
    assert_eq!(
        extract_origin("https://api.example.com/items?page=2"),
        "https://api.example.com"
    );
}

#[test]
fn test_extract_origin_with_port() {
    assert_eq!(
        extract_origin("http://localhost:8080/api/v1"),
        "http://localhost:8080"
    );
}

#[test]
fn test_extract_origin_no_path() {
    assert_eq!(
        extract_origin("https://api.example.com"),
        "https://api.example.com"
    );
}

#[test]
fn test_extract_origin_no_scheme() {
    assert_eq!(
        extract_origin("api.example.com/items"),
        "api.example.com/items"
    );
}

#[test]
fn test_extract_origin_trailing_slash() {
    assert_eq!(
        extract_origin("https://api.example.com/"),
        "https://api.example.com"
    );
}

// --- redact_query_param tests ---

#[test]
fn test_redact_query_param_present() {
    let url = "https://api.example.com/items?api_key=SECRET123&page=2";
    let redacted = redact_query_param(url, "api_key");
    assert_eq!(
        redacted,
        "https://api.example.com/items?api_key=[REDACTED]&page=2"
    );
    assert!(!redacted.contains("SECRET123"));
}

#[test]
fn test_redact_query_param_at_end() {
    let url = "https://api.example.com/items?page=2&api_key=SECRET123";
    let redacted = redact_query_param(url, "api_key");
    assert_eq!(
        redacted,
        "https://api.example.com/items?page=2&api_key=[REDACTED]"
    );
}

#[test]
fn test_redact_query_param_only_param() {
    let url = "https://api.example.com/items?api_key=SECRET123";
    let redacted = redact_query_param(url, "api_key");
    assert_eq!(redacted, "https://api.example.com/items?api_key=[REDACTED]");
}

#[test]
fn test_redact_query_param_not_present() {
    let url = "https://api.example.com/items?page=2&limit=10";
    let redacted = redact_query_param(url, "api_key");
    assert_eq!(redacted, url);
}

#[test]
fn test_redact_query_param_no_query_string() {
    let url = "https://api.example.com/items";
    let redacted = redact_query_param(url, "api_key");
    assert_eq!(redacted, url);
}

#[test]
fn test_redact_query_param_encoded_name() {
    // urlencoding::encode("api key") = "api%20key"
    let url = "https://api.example.com/items?api%20key=SECRET&page=2";
    let redacted = redact_query_param(url, "api key");
    assert_eq!(
        redacted,
        "https://api.example.com/items?api%20key=[REDACTED]&page=2"
    );
}

#[test]
fn test_redact_query_param_prefix_collision() {
    // An earlier param whose name merely *ends with* the key's name
    // ('sortkey' vs 'key') must NOT be redacted in place of the real secret,
    // which is appended last. Only a boundary-anchored ('?'/'&') match counts.
    let url = "https://api.example.com/items?sortkey=5&key=SUPERSECRET";
    let redacted = redact_query_param(url, "key");
    assert_eq!(
        redacted,
        "https://api.example.com/items?sortkey=5&key=[REDACTED]"
    );
    assert!(!redacted.contains("SUPERSECRET"));
    assert!(redacted.contains("sortkey=5"));
}

#[test]
fn test_redact_query_param_first_param_boundary() {
    // The key as the first query param (preceded by '?') is still redacted, and
    // a later param sharing the suffix is untouched.
    let url = "https://api.example.com/items?key=SECRET&sortkey=5";
    let redacted = redact_query_param(url, "key");
    assert_eq!(
        redacted,
        "https://api.example.com/items?key=[REDACTED]&sortkey=5"
    );
    assert!(!redacted.contains("SECRET"));
}

#[test]
fn test_redact_query_param_redacts_all_occurrences() {
    // The secret can appear more than once (e.g. a pagination next_url that
    // echoes the request query string and then re-appends the api_key). EVERY
    // boundary-anchored occurrence must be redacted, not just the first.
    let url = "https://api.example.com/items?api_key=SECRET&cursor=abc&api_key=SECRET";
    let redacted = redact_query_param(url, "api_key");
    assert_eq!(
        redacted,
        "https://api.example.com/items?api_key=[REDACTED]&cursor=abc&api_key=[REDACTED]"
    );
    assert!(!redacted.contains("SECRET"));
}

// --- build_single_resource_url: rowid pushdown URL construction ---

fn make_fdw_for_single_resource(
    base_url: &str,
    api_key_query: Option<(String, String)>,
) -> OpenApiFdw {
    OpenApiFdw {
        config: ServerConfig {
            base_url: base_url.to_string(),
            api_key_query,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[test]
fn test_single_resource_url_basic() {
    let fdw = make_fdw_for_single_resource("https://api.example.com", None);
    assert_eq!(
        fdw.build_single_resource_url("/items", "123"),
        "https://api.example.com/items/123"
    );
}

#[test]
fn test_single_resource_url_encodes_path_traversal() {
    // Drives the real production path: ../admin must be percent-encoded so it
    // cannot escape /items.
    let fdw = make_fdw_for_single_resource("https://api.example.com", None);
    assert_eq!(
        fdw.build_single_resource_url("/items", "../admin"),
        "https://api.example.com/items/..%2Fadmin"
    );
}

#[test]
fn test_single_resource_url_encodes_query_injection() {
    let fdw = make_fdw_for_single_resource("https://api.example.com", None);
    assert_eq!(
        fdw.build_single_resource_url("/items", "123?admin=true"),
        "https://api.example.com/items/123%3Fadmin%3Dtrue"
    );
}

#[test]
fn test_single_resource_url_trims_trailing_slash() {
    // A trailing slash on the endpoint must not produce a doubled slash.
    let fdw = make_fdw_for_single_resource("https://api.example.com", None);
    assert_eq!(
        fdw.build_single_resource_url("/items/", "5"),
        "https://api.example.com/items/5"
    );
}

#[test]
fn test_single_resource_url_preserves_static_query() {
    // A static query string stays a query; the id joins the path, not the
    // value of the last query param.
    let fdw = make_fdw_for_single_resource("https://api.example.com", None);
    assert_eq!(
        fdw.build_single_resource_url("/search?type=active", "5"),
        "https://api.example.com/search/5?type=active"
    );
}

#[test]
fn test_single_resource_url_appends_api_key_query() {
    // The query-auth key must ride along on the single-resource path, or the
    // request goes out unauthenticated.
    let fdw = make_fdw_for_single_resource(
        "https://api.example.com",
        Some(("api_key".to_string(), "secret123".to_string())),
    );
    assert_eq!(
        fdw.build_single_resource_url("/items", "123"),
        "https://api.example.com/items/123?api_key=secret123"
    );
}

#[test]
fn test_single_resource_url_api_key_after_static_query() {
    // The api_key joins with '&' when a static query is already present.
    let fdw = make_fdw_for_single_resource(
        "https://api.example.com",
        Some(("api_key".to_string(), "secret123".to_string())),
    );
    assert_eq!(
        fdw.build_single_resource_url("/search?type=active", "5"),
        "https://api.example.com/search/5?type=active&api_key=secret123"
    );
}

// --- Retry delay tests (using production functions) ---

#[test]
fn test_retry_delay_from_header_normal_value() {
    // Normal Retry-After: 5 seconds → 5000ms, well under cap
    assert_eq!(retry_delay_from_header(5, MAX_RETRY_DELAY_MS), 5000);
}

#[test]
fn test_retry_delay_from_header_large_value() {
    // Absurdly large Retry-After: 999999 seconds → capped to 30s
    assert_eq!(retry_delay_from_header(999_999, MAX_RETRY_DELAY_MS), 30_000);
}

#[test]
fn test_retry_delay_from_header_u64_max() {
    // u64::MAX seconds → saturating_mul prevents overflow, then capped
    assert_eq!(
        retry_delay_from_header(u64::MAX, MAX_RETRY_DELAY_MS),
        30_000
    );
}

#[test]
fn test_retry_delay_from_header_zero() {
    // Retry-After: 0 → 0ms (immediate retry)
    assert_eq!(retry_delay_from_header(0, MAX_RETRY_DELAY_MS), 0);
}

#[test]
fn test_exponential_backoff_first_retry() {
    // retry_count=0 → 1000ms
    assert_eq!(exponential_backoff_delay(0, MAX_RETRY_DELAY_MS), 1000);
}

#[test]
fn test_exponential_backoff_second_retry() {
    // retry_count=1 → 2000ms
    assert_eq!(exponential_backoff_delay(1, MAX_RETRY_DELAY_MS), 2000);
}

#[test]
fn test_exponential_backoff_third_retry() {
    // retry_count=2 → 4000ms
    assert_eq!(exponential_backoff_delay(2, MAX_RETRY_DELAY_MS), 4000);
}

#[test]
fn test_exponential_backoff_capped() {
    // retry_count=10 would be 1024s, but capped to 30s
    assert_eq!(exponential_backoff_delay(10, MAX_RETRY_DELAY_MS), 30_000);
}

// --- build_query_params: LIMIT-to-page_size optimization ---

fn make_fdw_for_page_size(page_size: usize, src_limit: Option<i64>) -> OpenApiFdw {
    OpenApiFdw {
        config: ServerConfig {
            page_size,
            page_size_param: "per_page".to_string(),
            ..Default::default()
        },
        src_limit,
        ..Default::default()
    }
}

fn get_page_size_param(fdw: &OpenApiFdw) -> Option<String> {
    let (params, _) = fdw.build_query_params(&[], &[]);
    params.iter().find(|p| p.starts_with("per_page=")).cloned()
}

#[test]
fn test_page_size_reduced_by_limit() {
    // LIMIT 5 with page_size=30 → per_page=5
    let fdw = make_fdw_for_page_size(30, Some(5));
    assert_eq!(get_page_size_param(&fdw), Some("per_page=5".to_string()));
}

#[test]
fn test_page_size_not_increased_by_limit() {
    // LIMIT 50 with page_size=30 → per_page=30 (limit larger than page_size)
    let fdw = make_fdw_for_page_size(30, Some(50));
    assert_eq!(get_page_size_param(&fdw), Some("per_page=30".to_string()));
}

#[test]
fn test_page_size_unchanged_without_limit() {
    // No LIMIT → per_page=30
    let fdw = make_fdw_for_page_size(30, None);
    assert_eq!(get_page_size_param(&fdw), Some("per_page=30".to_string()));
}

#[test]
fn test_page_size_zero_no_param() {
    // page_size=0 → no per_page param regardless of LIMIT
    let fdw = make_fdw_for_page_size(0, Some(5));
    assert_eq!(get_page_size_param(&fdw), None);
}

// --- fetch_spec with spec_json tests ---

const MINIMAL_SPEC_JSON: &str = r#"{
    "openapi": "3.0.0",
    "info": { "title": "Test", "version": "1.0" },
    "servers": [{ "url": "https://api.example.com" }],
    "paths": {
        "/items": {
            "get": {
                "responses": { "200": { "description": "OK" } }
            }
        }
    }
}"#;

#[test]
fn test_fetch_spec_from_spec_json() {
    let mut fdw = OpenApiFdw {
        config: ServerConfig {
            spec_json: Some(MINIMAL_SPEC_JSON.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    fdw.fetch_spec().unwrap();
    assert!(fdw.spec.is_some());
    assert_eq!(fdw.config.base_url, "https://api.example.com");
}

#[test]
fn test_fetch_spec_from_spec_json_preserves_explicit_base_url() {
    let mut fdw = OpenApiFdw {
        config: ServerConfig {
            base_url: "https://custom.example.com".to_string(),
            spec_json: Some(MINIMAL_SPEC_JSON.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    fdw.fetch_spec().unwrap();
    assert!(fdw.spec.is_some());
    assert_eq!(fdw.config.base_url, "https://custom.example.com");
}

#[test]
fn test_fetch_spec_from_spec_json_invalid_json() {
    let mut fdw = OpenApiFdw {
        config: ServerConfig {
            spec_json: Some("{ not valid json".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    let err = fdw.fetch_spec().unwrap_err();
    assert!(err.contains("Invalid spec_json"));
}

#[test]
fn test_fetch_spec_from_spec_json_too_large() {
    let mut fdw = OpenApiFdw {
        config: ServerConfig {
            spec_json: Some("x".repeat(200)),
            max_response_bytes: 100,
            ..Default::default()
        },
        ..Default::default()
    };
    let err = fdw.fetch_spec().unwrap_err();
    assert!(err.contains("spec_json too large"));
    assert!(err.contains("200 bytes"));
    assert!(err.contains("limit: 100 bytes"));
}

#[test]
fn test_fetch_spec_from_spec_json_rejects_non_http_base_url() {
    let spec_with_bad_server = r#"{
        "openapi": "3.0.0",
        "info": { "title": "Test", "version": "1.0" },
        "servers": [{ "url": "file:///etc/passwd" }],
        "paths": {}
    }"#;
    let mut fdw = OpenApiFdw {
        config: ServerConfig {
            spec_json: Some(spec_with_bad_server.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    let err = fdw.fetch_spec().unwrap_err();
    assert!(err.contains("base_url (from spec servers)"));
    assert!(err.contains("Must start with http://"));
}

#[test]
fn test_fetch_spec_neither_url_nor_json() {
    let mut fdw = OpenApiFdw::default();
    // Neither spec_url nor spec_json set → succeeds but spec stays None
    fdw.fetch_spec().unwrap();
    assert!(fdw.spec.is_none());
}

// --- Fix 1: api_key_query appended to URL-based pagination ---

#[test]
fn test_resolve_pagination_url_appends_api_key_query() {
    let fdw = OpenApiFdw {
        config: ServerConfig {
            base_url: "https://api.example.com".to_string(),
            api_key_query: Some(("api_key".to_string(), "secret123".to_string())),
            ..Default::default()
        },
        endpoint: "/items".to_string(),
        pagination: PaginationState {
            next: Some(crate::pagination::PaginationToken::Url(
                "https://api.example.com/items?page=2".to_string(),
            )),
            ..Default::default()
        },
        ..Default::default()
    };

    // Drive the real production helper that build_url uses for URL-based
    // pagination, rather than reimplementing the append inline.
    let next_url = fdw.pagination.next.as_ref().unwrap().as_url().unwrap();
    let mut url = fdw.resolve_pagination_url(next_url).unwrap();
    fdw.append_api_key_query(&mut url);
    assert_eq!(
        url,
        "https://api.example.com/items?page=2&api_key=secret123"
    );
}

#[test]
fn test_resolve_pagination_url_no_api_key_unchanged() {
    let fdw = make_fdw_for_url("https://api.example.com", "/items");
    let url = fdw
        .resolve_pagination_url("https://api.example.com/items?page=2")
        .unwrap();
    // No api_key_query configured → URL unchanged
    assert_eq!(url, "https://api.example.com/items?page=2");
}

// --- Fix 7: Unclosed '{' in endpoint template ---

#[test]
fn test_substitute_path_params_unclosed_brace_error() {
    let params = std::collections::HashMap::new();
    let mut injected = std::collections::HashMap::new();
    let result = OpenApiFdw::substitute_path_params("/items/{id", &params, &mut injected);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Unclosed '{'"));
    assert!(err.contains("/items/{id"));
}

#[test]
fn test_substitute_path_params_unclosed_brace_after_valid() {
    let params = std::collections::HashMap::new();
    let mut injected = std::collections::HashMap::new();
    // First param is valid, second is unclosed
    let result =
        OpenApiFdw::substitute_path_params("/users/{user_id}/posts/{title", &params, &mut injected);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unclosed '{'"));
}

// --- Map-based path parameter substitution ---
//
// substitute_path_params takes a pre-built name -> value map. The read path
// builds it from WHERE-clause quals (build_url); the write path builds it
// from row columns/cells (write::row_param_map). The map-only signature makes
// it structurally impossible for stale scan quals to drive a write URL: the
// write hooks never call ctx.get_quals().

fn params_of(entries: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
    entries
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn test_substitute_path_params_from_map_basic() {
    let params = params_of(&[("user_id", "u-1")]);
    let mut injected = std::collections::HashMap::new();
    let (endpoint, used) =
        OpenApiFdw::substitute_path_params("/users/{user_id}/posts", &params, &mut injected)
            .unwrap();
    assert_eq!(endpoint, "/users/u-1/posts");
    assert_eq!(used, vec!["user_id"]);
    assert_eq!(injected.get("user_id"), Some(&"u-1".to_string()));
}

#[test]
fn test_substitute_path_params_from_map_case_insensitive() {
    // Lowercase map key matches a differently-cased placeholder
    let params = params_of(&[("user_id", "u-1")]);
    let mut injected = std::collections::HashMap::new();
    let (endpoint, _) =
        OpenApiFdw::substitute_path_params("/users/{User_Id}/posts", &params, &mut injected)
            .unwrap();
    assert_eq!(endpoint, "/users/u-1/posts");
}

#[test]
fn test_substitute_path_params_from_map_urlencodes() {
    let params = params_of(&[("id", "a/b c")]);
    let mut injected = std::collections::HashMap::new();
    let (endpoint, _) =
        OpenApiFdw::substitute_path_params("/x/{id}", &params, &mut injected).unwrap();
    assert_eq!(endpoint, "/x/a%2Fb%20c");
}

#[test]
fn test_substitute_path_params_from_map_missing_errors() {
    let params = std::collections::HashMap::new();
    let mut injected = std::collections::HashMap::new();
    let err =
        OpenApiFdw::substitute_path_params("/users/{user_id}", &params, &mut injected).unwrap_err();
    assert!(err.contains("Missing required path parameter"));
    assert!(err.contains("user_id"));
}

#[test]
fn test_substitute_path_params_from_map_no_braces_passthrough() {
    let params = params_of(&[("anything", "x")]);
    let mut injected = std::collections::HashMap::new();
    let (endpoint, used) =
        OpenApiFdw::substitute_path_params("/items", &params, &mut injected).unwrap();
    assert_eq!(endpoint, "/items");
    assert!(used.is_empty());
    assert!(injected.is_empty());
}

#[test]
fn test_substitute_path_params_from_map_multiple() {
    let params = params_of(&[("org", "supabase"), ("repo", "wrappers")]);
    let mut injected = std::collections::HashMap::new();
    let (endpoint, used) =
        OpenApiFdw::substitute_path_params("/projects/{org}/{repo}/issues", &params, &mut injected)
            .unwrap();
    assert_eq!(endpoint, "/projects/supabase/wrappers/issues");
    assert_eq!(used, vec!["org", "repo"]);
}

#[test]
fn test_substitute_path_params_row_map_wins_not_stale_quals() {
    // Stale-qual regression: the value used is whatever map the caller
    // passes. The write path passes a row-derived map, so a stale qual value
    // ("FROM_QUAL", left over in host state from a prior scan) can never
    // reach the URL — only the row value ("FROM_ROW") can.
    let row_map = params_of(&[("user_id", "FROM_ROW")]);
    let mut injected = std::collections::HashMap::new();
    let (endpoint, _) =
        OpenApiFdw::substitute_path_params("/users/{user_id}/x", &row_map, &mut injected).unwrap();
    assert_eq!(endpoint, "/users/FROM_ROW/x");
    assert!(!endpoint.contains("FROM_QUAL"));
    assert_eq!(injected.get("user_id"), Some(&"FROM_ROW".to_string()));
}

// --- HTTP method labels (debug logger) ---

#[test]
fn test_method_label_all_verbs() {
    assert_eq!(method_label(http::Method::Get), "GET");
    assert_eq!(method_label(http::Method::Post), "POST");
    assert_eq!(method_label(http::Method::Put), "PUT");
    assert_eq!(method_label(http::Method::Patch), "PATCH");
    assert_eq!(method_label(http::Method::Delete), "DELETE");
}

// --- retry idempotency gate ---

#[test]
fn test_method_is_idempotent() {
    // The guest-side retry loop may re-send safe verbs (GET/PUT/DELETE) but must
    // not re-send POST/PATCH, whose bodies could compound side effects.
    assert!(method_is_idempotent(http::Method::Get));
    assert!(method_is_idempotent(http::Method::Put));
    assert!(method_is_idempotent(http::Method::Delete));
    assert!(!method_is_idempotent(http::Method::Post));
    assert!(!method_is_idempotent(http::Method::Patch));
}

// --- in-band error detection (check_response_error) ---

fn fdw_with_error_opts(path: &str, value: &str, msg_path: Option<&str>) -> OpenApiFdw {
    OpenApiFdw {
        error_path: path.to_string(),
        error_value: value.to_string(),
        error_message_path: msg_path.map(ToOwned::to_owned),
        ..Default::default()
    }
}

#[test]
fn test_error_check_disabled_by_default() {
    let fdw = OpenApiFdw::default();
    let resp = serde_json::json!({"status": "error", "message": "nope"});
    assert!(fdw.check_response_error(&resp).is_ok());
}

#[test]
fn test_error_check_matches_configured_value() {
    // Zoho's shape: a 2xx carrying {code, message, status: "error"}.
    let fdw = fdw_with_error_opts("/status", "error", Some("/message"));
    let resp = serde_json::json!({
        "code": "RATE_LIMIT_EXCEEDED",
        "message": "API rate limit exceeded.",
        "status": "error"
    });
    let err = fdw.check_response_error(&resp).unwrap_err();
    assert!(err.contains("API rate limit exceeded."), "got: {err}");
}

#[test]
fn test_error_check_ignores_non_matching_value() {
    let fdw = fdw_with_error_opts("/status", "error", Some("/message"));
    let resp = serde_json::json!({"status": "success", "data": []});
    assert!(fdw.check_response_error(&resp).is_ok());
}

#[test]
fn test_error_check_absent_path_is_ok() {
    let fdw = fdw_with_error_opts("/status", "error", None);
    let resp = serde_json::json!({"data": [{"id": 1}]});
    assert!(fdw.check_response_error(&resp).is_ok());
}

#[test]
fn test_error_check_without_value_treats_any_truthy_as_error() {
    let fdw = fdw_with_error_opts("/errors", "", None);
    assert!(
        fdw.check_response_error(&serde_json::json!({"errors": ["boom"]}))
            .is_err()
    );
    // null and false are not errors
    assert!(
        fdw.check_response_error(&serde_json::json!({"errors": null}))
            .is_ok()
    );
    assert!(
        fdw.check_response_error(&serde_json::json!({"errors": false}))
            .is_ok()
    );
}

#[test]
fn test_error_check_falls_back_to_the_flag_value_as_detail() {
    // No error_message_path configured: the raised error still carries
    // something identifiable rather than a bare "error".
    let fdw = fdw_with_error_opts("/code", "", None);
    let err = fdw
        .check_response_error(&serde_json::json!({"code": "TOO_MANY_REQUESTS"}))
        .unwrap_err();
    assert!(err.contains("TOO_MANY_REQUESTS"), "got: {err}");
}
