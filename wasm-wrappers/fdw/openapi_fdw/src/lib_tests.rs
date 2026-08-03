use super::*;
use config::{DEFAULT_MAX_PAGES, DEFAULT_MAX_RESPONSE_BYTES};

// --- Cross-cutting default tests ---

#[test]
fn test_max_response_bytes_default() {
    let fdw = OpenApiFdw::default();
    assert_eq!(fdw.config.max_response_bytes, DEFAULT_MAX_RESPONSE_BYTES);
}

#[test]
fn test_pagination_safety_defaults() {
    let fdw = OpenApiFdw::default();
    assert_eq!(fdw.config.max_pages, DEFAULT_MAX_PAGES);
    assert_eq!(fdw.pagination.pages_fetched, 0);
    assert!(fdw.pagination.previous.is_none());
    assert!(fdw.pagination.next.is_none());
}

// --- validate_url ---

#[test]
fn test_validate_url_https() {
    assert!(validate_url("https://api.example.com", "base_url").is_ok());
}

#[test]
fn test_validate_url_http() {
    assert!(validate_url("http://localhost:8080", "base_url").is_ok());
}

#[test]
fn test_validate_url_no_scheme() {
    let err = validate_url("api.example.com", "base_url").unwrap_err();
    assert!(err.contains("Invalid base_url"));
    assert!(err.contains("api.example.com"));
    assert!(err.contains("http://"));
}

#[test]
fn test_validate_url_ftp_scheme() {
    let err = validate_url("ftp://files.example.com", "spec_url").unwrap_err();
    assert!(err.contains("Invalid spec_url"));
}

#[test]
fn test_validate_url_empty_string() {
    let err = validate_url("", "base_url").unwrap_err();
    assert!(err.contains("Invalid base_url"));
}

// --- ensure_base_url ---

#[test]
fn test_ensure_base_url_derives_from_spec_servers() {
    // A spec-only server (no explicit base_url) must resolve base_url from the
    // spec's servers before a request is built. The read path does this in
    // begin_scan; begin_modify does it for writes so a bare INSERT on a
    // spec-configured writable table doesn't build a scheme-less URL.
    let mut fdw = OpenApiFdw::default();
    fdw.config.base_url = String::new();
    fdw.config.spec_json = Some("provided".to_string());
    fdw.spec = Some(
        OpenApiSpec::from_str(
            r#"{"openapi":"3.0.0","info":{"title":"t"},
               "servers":[{"url":"https://api.example.com/v1/"}]}"#,
        )
        .unwrap(),
    );

    fdw.ensure_base_url().unwrap();

    // Derived from the spec's first server, trailing slash trimmed.
    assert_eq!(fdw.config.base_url, "https://api.example.com/v1");
}

#[test]
fn test_ensure_base_url_keeps_explicit_base_url() {
    // An explicit base_url is authoritative and never overridden by the spec.
    let mut fdw = OpenApiFdw::default();
    fdw.config.base_url = "https://explicit.example.com".to_string();
    fdw.config.spec_json = Some("provided".to_string());
    fdw.spec = Some(
        OpenApiSpec::from_str(
            r#"{"openapi":"3.0.0","info":{"title":"t"},
               "servers":[{"url":"https://api.example.com/v1"}]}"#,
        )
        .unwrap(),
    );

    fdw.ensure_base_url().unwrap();

    assert_eq!(fdw.config.base_url, "https://explicit.example.com");
}

#[test]
fn test_ensure_base_url_noop_without_spec_config() {
    // No base_url and no spec configured: nothing to resolve, stays empty
    // (a plain base_url-less table that never reaches a request).
    let mut fdw = OpenApiFdw::default();
    fdw.config.base_url = String::new();

    fdw.ensure_base_url().unwrap();

    assert!(fdw.config.base_url.is_empty());
}

// --- qual_allows_limit_pushdown ---

#[test]
fn test_qual_allows_limit_pushdown_equality_no_or_pushable() {
    // A non-OR '=' qual with a stringifiable value is pushed down and
    // re-injected, so Postgres keeps every source row: an early LIMIT is safe.
    assert!(qual_allows_limit_pushdown("=", false, true));
}

#[test]
fn test_qual_allows_limit_pushdown_rejects_or_equality() {
    // An OR'd equality is filtered locally, so a source-side LIMIT could
    // under-return; not pushdown-safe even with a pushable value.
    assert!(!qual_allows_limit_pushdown("=", true, true));
}

#[test]
fn test_qual_allows_limit_pushdown_rejects_non_equality() {
    // Any operator other than '=' is filtered locally and drops source rows.
    for op in ["<", ">", ">=", "<=", "<>", "!=", "~~", "like", "in"] {
        assert!(
            !qual_allows_limit_pushdown(op, false, true),
            "operator {op} must not be pushdown-safe"
        );
    }
}

#[test]
fn test_qual_allows_limit_pushdown_rejects_unpushable_value() {
    // A '=' qual whose value cannot be stringified (numeric/date/timestamp/json)
    // is neither pushed to the API nor injected, so it is filtered locally and
    // an early source-side LIMIT would under-return. Not pushdown-safe.
    assert!(!qual_allows_limit_pushdown("=", false, false));
}

// --- reject_rowid_reassignment ---

#[test]
fn test_reject_rowid_reassignment_absent_is_ok() {
    // The row carries no new value for the rowid column: nothing to reject.
    let params = std::collections::HashMap::new();
    assert!(reject_rowid_reassignment(&params, "id", "42").is_ok());
}

#[test]
fn test_reject_rowid_reassignment_same_value_is_ok() {
    // The row restates the same rowid value: harmless, allowed.
    let params = std::collections::HashMap::from([("id".to_string(), "42".to_string())]);
    assert!(reject_rowid_reassignment(&params, "id", "42").is_ok());
}

#[test]
fn test_reject_rowid_reassignment_changed_value_errors() {
    // The row assigns a different rowid: rejected, and the error names the
    // column plus both the old and new values.
    let params = std::collections::HashMap::from([("id".to_string(), "99".to_string())]);
    let err = reject_rowid_reassignment(&params, "id", "42").unwrap_err();
    assert!(err.contains("id"));
    assert!(err.contains("42"));
    assert!(err.contains("99"));
}

// --- parse_usize_option ---

#[test]
fn test_parse_usize_option_valid() {
    assert_eq!(parse_usize_option("100", "page_size").unwrap(), 100);
}

#[test]
fn test_parse_usize_option_zero() {
    assert_eq!(parse_usize_option("0", "page_size").unwrap(), 0);
}

#[test]
fn test_parse_usize_option_large() {
    assert_eq!(
        parse_usize_option("52428800", "max_response_bytes").unwrap(),
        52_428_800
    );
}

#[test]
fn test_parse_usize_option_negative() {
    let err = parse_usize_option("-1", "max_pages").unwrap_err();
    assert!(err.contains("Invalid value for 'max_pages'"));
    assert!(err.contains("-1"));
}

#[test]
fn test_parse_usize_option_not_a_number() {
    let err = parse_usize_option("abc", "page_size").unwrap_err();
    assert!(err.contains("Invalid value for 'page_size'"));
    assert!(err.contains("abc"));
}

#[test]
fn test_parse_usize_option_float() {
    let err = parse_usize_option("3.14", "page_size").unwrap_err();
    assert!(err.contains("Invalid value for 'page_size'"));
}

// --- parse_bool_flag ---

#[test]
fn test_parse_bool_flag_true() {
    assert!(parse_bool_flag(Some("true")));
}

#[test]
fn test_parse_bool_flag_one() {
    assert!(parse_bool_flag(Some("1")));
}

#[test]
fn test_parse_bool_flag_false() {
    assert!(!parse_bool_flag(Some("false")));
}

#[test]
fn test_parse_bool_flag_zero() {
    assert!(!parse_bool_flag(Some("0")));
}

#[test]
fn test_parse_bool_flag_none() {
    assert!(!parse_bool_flag(None));
}

#[test]
fn test_parse_bool_flag_random_string() {
    assert!(!parse_bool_flag(Some("banana")));
}

#[test]
fn test_parse_bool_flag_case_insensitive_and_aliases() {
    // Case-insensitive, with yes/on aliases, so `writable 'TRUE'` is not
    // silently read as false.
    assert!(parse_bool_flag(Some("TRUE")));
    assert!(parse_bool_flag(Some("True")));
    assert!(parse_bool_flag(Some("YES")));
    assert!(parse_bool_flag(Some("on")));
    assert!(parse_bool_flag(Some(" 1 ")));
    assert!(!parse_bool_flag(Some("off")));
    assert!(!parse_bool_flag(Some("FALSE")));
}

#[test]
fn test_parse_bool_flag_default_true() {
    use crate::parse_bool_flag_default_true;
    // Defaults to true; only explicit false-ish values (case-insensitive) disable it.
    assert!(parse_bool_flag_default_true(None));
    assert!(parse_bool_flag_default_true(Some("true")));
    assert!(parse_bool_flag_default_true(Some("anything")));
    assert!(!parse_bool_flag_default_true(Some("false")));
    assert!(!parse_bool_flag_default_true(Some("FALSE")));
    assert!(!parse_bool_flag_default_true(Some("0")));
    assert!(!parse_bool_flag_default_true(Some("no")));
    assert!(!parse_bool_flag_default_true(Some("off")));
}

// --- should_stop_scanning ---

#[test]
fn test_should_stop_scanning_no_limit() {
    assert!(!should_stop_scanning(100, None));
}

#[test]
fn test_should_stop_scanning_below_limit() {
    assert!(!should_stop_scanning(5, Some(10)));
}

#[test]
fn test_should_stop_scanning_at_limit() {
    assert!(should_stop_scanning(10, Some(10)));
}

#[test]
fn test_should_stop_scanning_above_limit() {
    assert!(should_stop_scanning(15, Some(10)));
}

#[test]
fn test_should_stop_scanning_zero_consumed() {
    assert!(!should_stop_scanning(0, Some(10)));
}

// --- extract_effective_row ---

#[test]
fn test_extract_effective_row_no_path() {
    let row = serde_json::json!({"name": "Alice"});
    let result = extract_effective_row(&row, None);
    assert_eq!(result, &row);
}

#[test]
fn test_extract_effective_row_with_path() {
    let row = serde_json::json!({"properties": {"name": "Alice"}, "type": "Feature"});
    let result = extract_effective_row(&row, Some("/properties"));
    assert_eq!(result, &serde_json::json!({"name": "Alice"}));
}

#[test]
fn test_extract_effective_row_missing_path() {
    let row = serde_json::json!({"name": "Alice"});
    let result = extract_effective_row(&row, Some("/nonexistent"));
    // Falls back to the original row when path doesn't exist
    assert_eq!(result, &row);
}

#[test]
fn test_extract_effective_row_nested_path() {
    let row = serde_json::json!({"a": {"b": {"c": 42}}});
    let result = extract_effective_row(&row, Some("/a/b"));
    assert_eq!(result, &serde_json::json!({"c": 42}));
}
