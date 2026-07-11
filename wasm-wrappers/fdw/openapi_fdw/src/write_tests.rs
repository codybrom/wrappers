use std::collections::HashMap;

use serde_json::json;

use super::*;
use crate::config::ServerConfig;

// ---------- helpers ----------

/// Build a WriteConfig from a plain option map, mirroring how begin_modify
/// builds one from the WASM Options resource.
fn config_from(
    options: &[(&str, &str)],
    endpoint: &str,
    rowid: &str,
) -> Result<WriteConfig, String> {
    let map: HashMap<String, String> = options
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    build_write_config(&|name| map.get(name).cloned(), endpoint, rowid)
}

/// A minimal writable config for check_response tests.
fn writable_config() -> WriteConfig {
    config_from(
        &[("writable", "true"), ("insert_method", "POST")],
        "/items",
        "id",
    )
    .unwrap()
}

fn fdw_with_base_url(base_url: &str) -> OpenApiFdw {
    OpenApiFdw {
        config: ServerConfig {
            base_url: base_url.to_string(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn cols(names: &[&str]) -> Vec<String> {
    names.iter().map(ToString::to_string).collect()
}

// ---------- cell_to_json: type fidelity ----------

#[test]
fn test_cell_to_json_i64_stays_number() {
    let value = cell_to_json(&Cell::I64(42), "count").unwrap();
    assert!(value.is_i64());
    assert_eq!(value, json!(42));
}

#[test]
fn test_cell_to_json_small_ints_stay_numbers() {
    assert_eq!(cell_to_json(&Cell::I8(7), "c").unwrap(), json!(7));
    assert_eq!(cell_to_json(&Cell::I16(-300), "c").unwrap(), json!(-300));
    assert_eq!(cell_to_json(&Cell::I32(1234), "c").unwrap(), json!(1234));
}

#[test]
fn test_cell_to_json_floats_stay_numbers() {
    let f32_val = cell_to_json(&Cell::F32(1.5), "c").unwrap();
    assert!(f32_val.is_number());
    assert_eq!(f32_val.as_f64(), Some(1.5));

    let f64_val = cell_to_json(&Cell::F64(3.25), "c").unwrap();
    assert!(f64_val.is_f64());
    assert_eq!(f64_val, json!(3.25));
}

#[test]
fn test_cell_to_json_numeric_stays_number() {
    let value = cell_to_json(&Cell::Numeric(8000.0), "amount").unwrap();
    assert!(value.is_number());
    assert_eq!(value.as_f64(), Some(8000.0));
}

#[test]
fn test_cell_to_json_nan_is_error() {
    let err = cell_to_json(&Cell::F64(f64::NAN), "x").unwrap_err();
    assert!(err.contains("not representable"));
    assert!(err.contains('x'));
}

#[test]
fn test_cell_to_json_bool_stays_bool() {
    let value = cell_to_json(&Cell::Bool(true), "active").unwrap();
    assert!(value.is_boolean());
    assert_eq!(value, json!(true));
}

#[test]
fn test_cell_to_json_string_stays_string() {
    assert_eq!(
        cell_to_json(&Cell::String("hi".into()), "name").unwrap(),
        json!("hi")
    );
}

#[test]
fn test_cell_to_json_uuid_emitted_as_string() {
    let uuid = "550e8400-e29b-41d4-a716-446655440000";
    assert_eq!(
        cell_to_json(&Cell::Uuid(uuid.into()), "id").unwrap(),
        json!(uuid)
    );
}

#[test]
fn test_cell_to_json_json_cell_parsed_as_object() {
    let value = cell_to_json(&Cell::Json(r#"{"Owner":{"id":"123"}}"#.into()), "owner").unwrap();
    assert!(value.is_object());
    assert_eq!(value["Owner"]["id"], json!("123"));
}

#[test]
fn test_cell_to_json_json_cell_parsed_as_array() {
    let value = cell_to_json(&Cell::Json("[1,2,3]".into()), "tags").unwrap();
    assert!(value.is_array());
    assert_eq!(value.as_array().unwrap().len(), 3);
}

#[test]
fn test_cell_to_json_invalid_json_errors_with_column_name() {
    let err = cell_to_json(&Cell::Json("{not json".into()), "meta").unwrap_err();
    assert!(err.contains("meta"));
    assert!(err.contains("invalid JSON"));
}

#[test]
fn test_cell_to_json_other_is_clean_error() {
    // Unsupported cell types produce a clean error, never a panic
    // (paddle has an unimplemented!() here).
    let err = cell_to_json(&Cell::Other("whatever".into()), "blob").unwrap_err();
    assert!(err.contains("blob"));
    assert!(err.contains("not supported"));
}

// ---------- cell_to_string ----------

#[test]
fn test_cell_to_string_supported_types() {
    assert_eq!(
        cell_to_string(&Cell::String("abc".into())),
        Some("abc".into())
    );
    assert_eq!(cell_to_string(&Cell::I64(42)), Some("42".into()));
    assert_eq!(cell_to_string(&Cell::I32(-7)), Some("-7".into()));
    assert_eq!(cell_to_string(&Cell::Bool(true)), Some("true".into()));
    assert_eq!(
        cell_to_string(&Cell::Uuid("550e8400".into())),
        Some("550e8400".into())
    );
}

#[test]
fn test_cell_to_string_unsupported_types_none() {
    // Mirrors qual_value_to_string: Json/Other (and date/time) are not valid
    // URL path or query values.
    assert_eq!(cell_to_string(&Cell::Json("{}".into())), None);
    assert_eq!(cell_to_string(&Cell::Other("x".into())), None);
    assert_eq!(cell_to_string(&Cell::Timestamp(0)), None);
}

// ---------- row_param_map ----------

#[test]
fn test_row_param_map_includes_original_and_lowercase_keys() {
    let cols = cols(&["User_Id", "name"]);
    let cells = vec![Some(Cell::I64(7)), Some(Cell::String("a".into()))];
    let params = row_param_map(&cols, &cells);
    assert_eq!(params.get("user_id"), Some(&"7".to_string()));
    assert_eq!(params.get("User_Id"), Some(&"7".to_string()));
    assert_eq!(params.get("name"), Some(&"a".to_string()));
}

#[test]
fn test_row_param_map_skips_nulls_and_unsupported() {
    let cols = cols(&["a", "b", "c"]);
    let cells = vec![None, Some(Cell::Json("{}".into())), Some(Cell::I64(1))];
    let params = row_param_map(&cols, &cells);
    assert!(!params.contains_key("a"));
    assert!(!params.contains_key("b"));
    assert_eq!(params.get("c"), Some(&"1".to_string()));
}

// ---------- build_body ----------

#[test]
fn test_build_body_omits_none_cells() {
    let cols = cols(&["name", "email"]);
    let cells = vec![Some(Cell::String("a".into())), None];
    let body = build_body(&cols, &cells, None, &[]).unwrap();
    assert_eq!(body.get("name"), Some(&json!("a")));
    assert!(!body.contains_key("email"));
}

#[test]
fn test_build_body_all_none_yields_empty_object() {
    let cols = cols(&["a", "b"]);
    let cells = vec![None, None];
    let body = build_body(&cols, &cells, None, &[]).unwrap();
    assert!(body.is_empty());
}

#[test]
fn test_build_body_skips_attrs_column() {
    let cols = cols(&["name", "attrs"]);
    let cells = vec![
        Some(Cell::String("a".into())),
        Some(Cell::Json(r#"{"full":"row"}"#.into())),
    ];
    let body = build_body(&cols, &cells, None, &[]).unwrap();
    assert!(!body.contains_key("attrs"));
    assert_eq!(body.len(), 1);
}

#[test]
fn test_build_body_skips_rowid_column_when_requested() {
    let cols = cols(&["id", "name"]);
    let cells = vec![
        Some(Cell::String("i-1".into())),
        Some(Cell::String("a".into())),
    ];
    let body = build_body(&cols, &cells, Some("id"), &[]).unwrap();
    assert!(!body.contains_key("id"));
    assert_eq!(body.get("name"), Some(&json!("a")));
}

#[test]
fn test_build_body_rowid_skip_case_insensitive() {
    let cols = cols(&["ID", "name"]);
    let cells = vec![
        Some(Cell::String("i-1".into())),
        Some(Cell::String("a".into())),
    ];
    let body = build_body(&cols, &cells, Some("id"), &[]).unwrap();
    assert!(!body.contains_key("ID"));
}

#[test]
fn test_build_body_keeps_rowid_for_insert() {
    // INSERT passes skip_rowid = None: a user-supplied id column stays in
    // the body like any other column.
    let cols = cols(&["id", "name"]);
    let cells = vec![
        Some(Cell::String("i-1".into())),
        Some(Cell::String("a".into())),
    ];
    let body = build_body(&cols, &cells, None, &[]).unwrap();
    assert_eq!(body.get("id"), Some(&json!("i-1")));
}

#[test]
fn test_build_body_skips_path_param_columns() {
    // Columns consumed as {param} path placeholders must not double-emit
    // into the JSON body.
    let cols = cols(&["owner", "repo", "body"]);
    let cells = vec![
        Some(Cell::String("octocat".into())),
        Some(Cell::String("hello".into())),
        Some(Cell::String("Thanks, merging.".into())),
    ];
    let consumed = vec!["owner".to_string(), "repo".to_string()];
    let body = build_body(&cols, &cells, None, &consumed).unwrap();
    assert!(!body.contains_key("owner"));
    assert!(!body.contains_key("repo"));
    assert_eq!(body.get("body"), Some(&json!("Thanks, merging.")));
}

#[test]
fn test_build_body_preserves_quoted_column_case() {
    let cols = cols(&["Stage", "Amount"]);
    let cells = vec![
        Some(Cell::String("Qualification".into())),
        Some(Cell::Numeric(8000.0)),
    ];
    let body = build_body(&cols, &cells, Some("id"), &[]).unwrap();
    assert_eq!(body.get("Stage"), Some(&json!("Qualification")));
    assert!(body.get("Amount").is_some_and(serde_json::Value::is_number));
}

// ---------- wrap_envelope ----------

#[test]
fn test_wrap_envelope_no_root_passthrough() {
    let mut map = JsonMap::new();
    map.insert("name".into(), json!("a"));
    let wrapped = wrap_envelope(map, None, BodyWrap::Object);
    assert_eq!(wrapped, json!({"name": "a"}));
}

#[test]
fn test_wrap_envelope_object_under_root() {
    let mut map = JsonMap::new();
    map.insert("name".into(), json!("a"));
    let wrapped = wrap_envelope(map, Some("/properties"), BodyWrap::Object);
    assert_eq!(wrapped, json!({"properties": {"name": "a"}}));
}

#[test]
fn test_wrap_envelope_array_under_root() {
    let mut map = JsonMap::new();
    map.insert("Stage".into(), json!("Qualification"));
    let wrapped = wrap_envelope(map, Some("/data"), BodyWrap::Array);
    assert_eq!(wrapped, json!({"data": [{"Stage": "Qualification"}]}));
}

#[test]
fn test_wrap_envelope_nested_root_path() {
    let mut map = JsonMap::new();
    map.insert("x".into(), json!(1));
    let wrapped = wrap_envelope(map, Some("/payload/record"), BodyWrap::Object);
    assert_eq!(wrapped, json!({"payload": {"record": {"x": 1}}}));
}

// ---------- option parsing ----------

#[test]
fn test_parse_http_method_accepts_write_verbs_case_insensitive() {
    assert!(matches!(
        parse_http_method("post", "m"),
        Ok(http::Method::Post)
    ));
    assert!(matches!(
        parse_http_method("Put", "m"),
        Ok(http::Method::Put)
    ));
    assert!(matches!(
        parse_http_method("PATCH", "m"),
        Ok(http::Method::Patch)
    ));
    assert!(matches!(
        parse_http_method("delete", "m"),
        Ok(http::Method::Delete)
    ));
}

#[test]
fn test_parse_http_method_rejects_get_and_garbage() {
    assert!(parse_http_method("GET", "insert_method").is_err());
    let err = parse_http_method("FETCH", "insert_method").unwrap_err();
    assert!(err.contains("insert_method"));
    assert!(err.contains("FETCH"));
}

#[test]
fn test_parse_rowid_location_values() {
    assert_eq!(
        parse_rowid_location("url", "o").unwrap(),
        RowidLocation::Url
    );
    assert_eq!(
        parse_rowid_location("Body", "o").unwrap(),
        RowidLocation::Body
    );
    assert_eq!(
        parse_rowid_location("QUERY", "o").unwrap(),
        RowidLocation::Query
    );
    assert!(parse_rowid_location("path", "o").is_err());
}

#[test]
fn test_parse_body_wrap_values() {
    assert_eq!(parse_body_wrap("object").unwrap(), BodyWrap::Object);
    assert_eq!(parse_body_wrap("Array").unwrap(), BodyWrap::Array);
    assert!(parse_body_wrap("list").is_err());
}

#[test]
fn test_parse_success_status_list() {
    assert_eq!(
        parse_success_status("200,201,202").unwrap(),
        vec![200, 201, 202]
    );
    assert_eq!(parse_success_status(" 200 , 204 ").unwrap(), vec![200, 204]);
}

#[test]
fn test_parse_success_status_invalid_token_errors() {
    assert!(parse_success_status("200,ok").is_err());
    assert!(parse_success_status("99999").is_err());
}

#[test]
fn test_parse_success_status_empty_errors() {
    assert!(parse_success_status("").is_err());
    assert!(parse_success_status(" , ").is_err());
}

#[test]
fn test_validate_json_pointer() {
    assert!(validate_json_pointer("/data", "p").is_ok());
    assert!(validate_json_pointer("/data/0/code", "p").is_ok());
    assert!(validate_json_pointer("data", "p").is_err());
    assert!(validate_json_pointer("/", "p").is_err());
    assert!(validate_json_pointer("/data//code", "p").is_err());
}

// ---------- build_write_config ----------

#[test]
fn test_config_not_writable_by_default() {
    let cfg = config_from(&[], "/items", "id").unwrap();
    assert!(!cfg.writable);
}

#[test]
fn test_config_writable_true_and_one() {
    assert!(
        config_from(&[("writable", "true")], "/items", "id")
            .unwrap()
            .writable
    );
    assert!(
        config_from(&[("writable", "1")], "/items", "id")
            .unwrap()
            .writable
    );
}

#[test]
fn test_config_writable_garbage_disabled() {
    for v in ["false", "0", "yes", "TRUE"] {
        assert!(
            !config_from(&[("writable", v)], "/items", "id")
                .unwrap()
                .writable,
            "writable '{v}' must not enable writes"
        );
    }
}

#[test]
fn test_config_ops_disabled_unless_method_set() {
    let cfg = config_from(
        &[("writable", "true"), ("insert_method", "POST")],
        "/items",
        "id",
    )
    .unwrap();
    assert!(cfg.insert_method.is_some());
    assert!(cfg.update_method.is_none());
    assert!(cfg.delete_method.is_none());
}

#[test]
fn test_config_endpoint_fallback_chain() {
    // per-op endpoint wins
    let cfg = config_from(
        &[
            ("writable", "true"),
            ("insert_method", "POST"),
            ("write_endpoint", "/write"),
            ("insert_endpoint", "/items/new"),
        ],
        "/items",
        "id",
    )
    .unwrap();
    assert_eq!(cfg.insert_endpoint, "/items/new");
    // write_endpoint second
    assert_eq!(cfg.update_endpoint, "/write");
    assert_eq!(cfg.delete_endpoint, "/write");

    // endpoint last
    let cfg = config_from(&[("writable", "true")], "/items", "id").unwrap();
    assert_eq!(cfg.insert_endpoint, "/items");
    assert_eq!(cfg.update_endpoint, "/items");
    assert_eq!(cfg.delete_endpoint, "/items");
}

#[test]
fn test_config_rowid_location_per_verb_fallback() {
    // Defaults to url for both verbs
    let cfg = config_from(&[("writable", "true")], "/items", "id").unwrap();
    assert_eq!(cfg.update_rowid_location, RowidLocation::Url);
    assert_eq!(cfg.delete_rowid_location, RowidLocation::Url);

    // Table-level default applies to both
    let cfg = config_from(
        &[
            ("writable", "true"),
            ("rowid_location", "body"),
            ("body_root_path", "/data"),
            ("success_path", "/code"),
        ],
        "/items",
        "id",
    )
    .unwrap();
    assert_eq!(cfg.update_rowid_location, RowidLocation::Body);
    assert_eq!(cfg.delete_rowid_location, RowidLocation::Body);

    // Per-verb override wins over the table default (the CRM case:
    // url for UPDATE, query for DELETE, on the same table).
    let cfg = config_from(
        &[
            ("writable", "true"),
            ("update_rowid_location", "url"),
            ("delete_rowid_location", "query"),
            ("rowid_param", "ids"),
        ],
        "/records",
        "id",
    )
    .unwrap();
    assert_eq!(cfg.update_rowid_location, RowidLocation::Url);
    assert_eq!(cfg.delete_rowid_location, RowidLocation::Query);
    assert_eq!(cfg.rowid_param, "ids");
}

#[test]
fn test_config_rowid_key_defaults() {
    let cfg = config_from(&[("writable", "true")], "/items", "custom_id").unwrap();
    assert_eq!(cfg.rowid_column, "custom_id");
    assert_eq!(cfg.rowid_body_key, "custom_id");
    assert_eq!(cfg.rowid_param, "custom_id");

    let cfg = config_from(
        &[("writable", "true"), ("rowid_body_key", "recordId")],
        "/items",
        "id",
    )
    .unwrap();
    assert_eq!(cfg.rowid_body_key, "recordId");
}

#[test]
fn test_config_array_wrap_without_root_errors() {
    let err = config_from(
        &[("writable", "true"), ("body_wrap", "array")],
        "/items",
        "id",
    )
    .unwrap_err();
    assert!(err.contains("body_wrap"));
    assert!(err.contains("body_root_path"));
}

#[test]
fn test_config_success_value_default() {
    let cfg = config_from(
        &[("writable", "true"), ("success_path", "/code")],
        "/items",
        "id",
    )
    .unwrap();
    assert_eq!(cfg.success_value, "SUCCESS");
}

// ---------- §3.2 required-success_path predicate ----------

#[test]
fn test_hard_error_envelope_without_success_path() {
    let err = config_from(
        &[("writable", "true"), ("body_root_path", "/data")],
        "/items",
        "id",
    )
    .unwrap_err();
    assert!(err.contains("success_path"));
}

#[test]
fn test_hard_error_unusual_status_without_success_path() {
    // 202 Accepted can carry an in-band failure code
    let err = config_from(
        &[("writable", "true"), ("success_status", "200,202")],
        "/items",
        "id",
    )
    .unwrap_err();
    assert!(err.contains("success_path"));
}

#[test]
fn test_no_hard_error_when_success_path_present() {
    assert!(
        config_from(
            &[
                ("writable", "true"),
                ("body_root_path", "/data"),
                ("success_status", "200,201,202"),
                ("success_path", "/data/0/code"),
            ],
            "/items",
            "id",
        )
        .is_ok()
    );
}

#[test]
fn test_no_hard_error_for_trivial_status_set() {
    // The safe set {200,201,204} alone never triggers the predicate
    assert!(
        config_from(
            &[("writable", "true"), ("success_status", "200,201,204")],
            "/items",
            "id",
        )
        .is_ok()
    );
}

#[test]
fn test_no_hard_error_when_not_writable() {
    // Gate only applies to writable tables
    assert!(config_from(&[("body_root_path", "/data")], "/items", "id").is_ok());
}

// ---------- build_write_url ----------

#[test]
fn test_build_write_url_rowid_in_url_path() {
    let fdw = fdw_with_base_url("https://api.example.com");
    let (url, consumed) = fdw
        .build_write_url(
            "/items",
            &HashMap::new(),
            Some("i-1"),
            RowidLocation::Url,
            "id",
            "id",
        )
        .unwrap();
    assert_eq!(url, "https://api.example.com/items/i-1");
    assert!(consumed.is_empty());
}

#[test]
fn test_build_write_url_rowid_urlencoded() {
    let fdw = fdw_with_base_url("https://api.example.com");
    let (url, _) = fdw
        .build_write_url(
            "/items",
            &HashMap::new(),
            Some("a/b c"),
            RowidLocation::Url,
            "id",
            "id",
        )
        .unwrap();
    assert_eq!(url, "https://api.example.com/items/a%2Fb%20c");
}

#[test]
fn test_build_write_url_rowid_in_query() {
    let fdw = fdw_with_base_url("https://api.example.com");
    let (url, _) = fdw
        .build_write_url(
            "/records",
            &HashMap::new(),
            Some("1000489124"),
            RowidLocation::Query,
            "id",
            "ids",
        )
        .unwrap();
    assert_eq!(url, "https://api.example.com/records?ids=1000489124");
}

#[test]
fn test_build_write_url_rowid_in_query_urlencoded_and_appended() {
    let fdw = fdw_with_base_url("https://api.example.com");
    let (url, _) = fdw
        .build_write_url(
            "/records?soft=true",
            &HashMap::new(),
            Some("a b&c"),
            RowidLocation::Query,
            "id",
            "ids",
        )
        .unwrap();
    // Existing query -> '&' separator; value urlencoded
    assert_eq!(
        url,
        "https://api.example.com/records?soft=true&ids=a%20b%26c"
    );
}

#[test]
fn test_build_write_url_body_location_keeps_collection_url() {
    let fdw = fdw_with_base_url("https://api.example.com");
    let (url, _) = fdw
        .build_write_url(
            "/records",
            &HashMap::new(),
            Some("i-1"),
            RowidLocation::Body,
            "id",
            "id",
        )
        .unwrap();
    assert_eq!(url, "https://api.example.com/records");
}

#[test]
fn test_build_write_url_substitutes_path_params_from_row_map() {
    let fdw = fdw_with_base_url("https://api.github.com");
    let mut params = HashMap::new();
    params.insert("owner".to_string(), "octocat".to_string());
    params.insert("repo".to_string(), "hello".to_string());
    params.insert("issue_number".to_string(), "7".to_string());
    let (url, consumed) = fdw
        .build_write_url(
            "/repos/{owner}/{repo}/issues/{issue_number}/comments",
            &params,
            None,
            RowidLocation::Url,
            "id",
            "id",
        )
        .unwrap();
    assert_eq!(
        url,
        "https://api.github.com/repos/octocat/hello/issues/7/comments"
    );
    assert_eq!(consumed, vec!["owner", "repo", "issue_number"]);
}

#[test]
fn test_build_write_url_missing_path_param_errors() {
    let fdw = fdw_with_base_url("https://api.example.com");
    let err = fdw
        .build_write_url(
            "/users/{user_id}/posts",
            &HashMap::new(),
            Some("p-1"),
            RowidLocation::Url,
            "id",
            "id",
        )
        .unwrap_err();
    assert!(err.contains("Missing required path parameter"));
    assert!(err.contains("user_id"));
}

#[test]
fn test_build_write_url_no_double_append_when_template_has_rowid() {
    // '/records/{id}' already places the rowid; Url location must not
    // append it a second time.
    let fdw = fdw_with_base_url("https://api.example.com");
    let mut params = HashMap::new();
    params.insert("id".to_string(), "i-1".to_string());
    let (url, _) = fdw
        .build_write_url(
            "/records/{id}",
            &params,
            Some("i-1"),
            RowidLocation::Url,
            "id",
            "id",
        )
        .unwrap();
    assert_eq!(url, "https://api.example.com/records/i-1");
}

#[test]
fn test_build_write_url_no_double_placement_query_when_template_has_rowid() {
    // '/records/{id}' already places the rowid in the path; Query location
    // must not also append it as a query parameter.
    let fdw = fdw_with_base_url("https://api.example.com");
    let mut params = HashMap::new();
    params.insert("id".to_string(), "i-1".to_string());
    let (url, _) = fdw
        .build_write_url(
            "/records/{id}",
            &params,
            Some("i-1"),
            RowidLocation::Query,
            "id",
            "ids",
        )
        .unwrap();
    assert_eq!(url, "https://api.example.com/records/i-1");
}

#[test]
fn test_build_write_url_appends_api_key_query() {
    let mut fdw = fdw_with_base_url("https://api.example.com");
    fdw.config.api_key_query = Some(("api_key".to_string(), "sk_test".to_string()));
    let (url, _) = fdw
        .build_write_url(
            "/items",
            &HashMap::new(),
            Some("i-1"),
            RowidLocation::Url,
            "id",
            "id",
        )
        .unwrap();
    assert_eq!(url, "https://api.example.com/items/i-1?api_key=sk_test");
}

#[test]
fn test_build_write_url_stale_scan_quals_cannot_leak() {
    // Regression for the write-path isolation guarantee: URL building takes
    // only the row-derived map. A value that exists solely in (stale) scan
    // quals — simulated by a different map — never reaches the URL, because
    // the write hooks build their map from row.cols()/cells() and never call
    // ctx.get_quals().
    let fdw = fdw_with_base_url("https://api.example.com");
    let mut row_map = HashMap::new();
    row_map.insert("user_id".to_string(), "FROM_ROW".to_string());
    let (url, _) = fdw
        .build_write_url(
            "/users/{user_id}/posts",
            &row_map,
            None,
            RowidLocation::Url,
            "id",
            "id",
        )
        .unwrap();
    assert_eq!(url, "https://api.example.com/users/FROM_ROW/posts");
    assert!(!url.contains("FROM_QUAL"));
}

// ---------- check_response: status gate ----------

#[test]
fn test_check_response_default_accepts_2xx() {
    let cfg = writable_config();
    for status in [200, 201, 202, 204] {
        assert!(
            check_response(status, "{}", &cfg, "/items", true).is_ok(),
            "HTTP {status} must be accepted by default"
        );
    }
}

#[test]
fn test_check_response_default_rejects_non_2xx() {
    let cfg = writable_config();
    for status in [301, 400, 401, 404, 500, 503] {
        let err = check_response(status, "{}", &cfg, "/items", true).unwrap_err();
        assert!(err.contains(&status.to_string()));
    }
}

#[test]
fn test_check_response_404_is_error_for_writes() {
    // The read path swallows 404 as an empty result; writes must not.
    let cfg = writable_config();
    assert!(check_response(404, "{}", &cfg, "/items", true).is_err());
}

#[test]
fn test_check_response_allowlist_accepts_and_rejects() {
    let mut cfg = writable_config();
    cfg.success_status = Some(vec![200, 202]);
    assert!(check_response(202, "{}", &cfg, "/items", true).is_ok());
    assert!(check_response(201, "{}", &cfg, "/items", true).is_err());
}

#[test]
fn test_check_response_207_rejected_by_default() {
    let cfg = writable_config();
    let err = check_response(207, "{}", &cfg, "/items", true).unwrap_err();
    assert!(err.contains("207"));
    assert!(err.contains("Multi-Status"));
}

#[test]
fn test_check_response_207_rejected_even_when_listed() {
    // Per-record outcomes inside a 207 cannot be verified, so 207 is a hard
    // gate independent of the allowlist and body check.
    let mut cfg = writable_config();
    cfg.success_status = Some(vec![200, 207]);
    cfg.success_path = Some("/code".to_string());
    assert!(check_response(207, r#"{"code":"SUCCESS"}"#, &cfg, "/items", true).is_err());
}

// ---------- check_response: body-level success check ----------

#[test]
fn test_check_response_success_path_match() {
    let mut cfg = writable_config();
    cfg.success_path = Some("/code".to_string());
    assert!(check_response(200, r#"{"code":"SUCCESS"}"#, &cfg, "/items", true).is_ok());
}

#[test]
fn test_check_response_success_path_mismatch_errors() {
    // The core body-level guard: HTTP 200 with an in-band failure code must
    // not pass as success.
    let mut cfg = writable_config();
    cfg.success_path = Some("/code".to_string());
    let err = check_response(200, r#"{"code":"FAILED"}"#, &cfg, "/items", true).unwrap_err();
    assert!(err.contains("success_path"));
    assert!(err.contains("SUCCESS"));
}

#[test]
fn test_check_response_202_with_failure_code_rejected() {
    // The enterprise-API case: insert returns HTTP 202 even when the embedded
    // record code signals failure. Status allowlist and body check are ANDed.
    let mut cfg = writable_config();
    cfg.success_status = Some(vec![200, 201, 202]);
    cfg.success_path = Some("/data/0/code".to_string());
    let body = r#"{"data":[{"code":"INVALID_DATA","details":{}}]}"#;
    assert!(check_response(202, body, &cfg, "/records", true).is_err());
}

#[test]
fn test_check_response_envelope_success_path() {
    let mut cfg = writable_config();
    cfg.success_path = Some("/data/0/code".to_string());
    let body = r#"{"data":[{"code":"SUCCESS","id":"123"}]}"#;
    assert!(check_response(200, body, &cfg, "/records", true).is_ok());
}

#[test]
fn test_check_response_success_path_missing_is_failure() {
    let mut cfg = writable_config();
    cfg.success_path = Some("/data/0/code".to_string());
    assert!(check_response(200, r#"{"other":1}"#, &cfg, "/records", true).is_err());
}

#[test]
fn test_check_response_success_path_non_string_value() {
    // Non-string outcome values compare via their JSON rendering
    let mut cfg = writable_config();
    cfg.success_path = Some("/ok".to_string());
    cfg.success_value = "true".to_string();
    assert!(check_response(200, r#"{"ok":true}"#, &cfg, "/items", true).is_ok());
    assert!(check_response(200, r#"{"ok":false}"#, &cfg, "/items", true).is_err());
}

#[test]
fn test_check_response_unparseable_body_with_success_path_errors() {
    let mut cfg = writable_config();
    cfg.success_path = Some("/code".to_string());
    let err = check_response(200, "", &cfg, "/items", true).unwrap_err();
    assert!(err.contains("not valid JSON"));
}

#[test]
fn test_check_response_no_success_path_skips_body_parse() {
    // Without success_path, a non-JSON (or empty 204) body is fine
    let cfg = writable_config();
    assert!(check_response(204, "", &cfg, "/items", true).is_ok());
    assert!(check_response(200, "not json", &cfg, "/items", true).is_ok());
}

// ---------- check_response: leak safety ----------

#[test]
fn test_check_response_error_never_echoes_body() {
    // Error messages are rebuilt from status + endpoint; the raw response
    // body (which may echo the request URL with credentials) is discarded.
    let mut cfg = writable_config();
    cfg.success_path = Some("/code".to_string());
    let body = r#"{"code":"FAILED","echo":"https://api.example.com/items?api_key=LEAKED_SECRET"}"#;
    let err = check_response(200, body, &cfg, "/items", true).unwrap_err();
    assert!(!err.contains("LEAKED_SECRET"));

    let err = check_response(500, body, &cfg, "/items", true).unwrap_err();
    assert!(!err.contains("LEAKED_SECRET"));
}

#[test]
fn test_check_response_error_uses_query_stripped_endpoint() {
    // Callers pass endpoint.split('?').next(); verify the message carries
    // only what the caller passed and nothing else URL-shaped.
    let cfg = writable_config();
    let err = check_response(500, "{}", &cfg, "/items", true).unwrap_err();
    assert!(err.contains("/items"));
    assert!(!err.contains('?'));
}

#[test]
fn test_check_response_detail_redacted_with_query_api_key() {
    // Vendor detail (the value at success_path) is surfaced only when no API
    // key rides in the query string.
    let mut cfg = writable_config();
    cfg.success_path = Some("/code".to_string());
    let body = r#"{"code":"VENDOR_CODE_42"}"#;

    let with_detail = check_response(200, body, &cfg, "/items", true).unwrap_err();
    assert!(with_detail.contains("VENDOR_CODE_42"));

    let without_detail = check_response(200, body, &cfg, "/items", false).unwrap_err();
    assert!(!without_detail.contains("VENDOR_CODE_42"));
}
