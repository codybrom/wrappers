//! Schema generation and type mapping for OpenAPI FDW
//!
//! This module handles mapping OpenAPI types to PostgreSQL types
//! and generating CREATE FOREIGN TABLE statements.

use std::collections::HashMap;

use crate::response::WRAPPER_KEYS;
use crate::spec::{EndpointInfo, OpenApiSpec, Schema};

/// Maps OpenAPI schema types to PostgreSQL type names
pub fn openapi_to_pg_type(schema: &Schema, spec: &OpenApiSpec) -> &'static str {
    // Resolve $ref if present; otherwise borrow the original (no clone).
    let owned;
    let resolved = if schema.reference.is_some() {
        owned = spec.resolve_schema(schema);
        &owned
    } else {
        schema
    };

    match resolved.schema_type.as_deref() {
        Some("string") => match resolved.format.as_deref() {
            Some("date") => "date",
            Some("date-time") => "timestamptz",
            // time and bytea are not supported by the WIT type-oid interface,
            // so we map them to text (the FDW casts values via JSON at runtime)
            Some("time") => "text",
            Some("byte") | Some("binary") => "text",
            Some("uuid") => "uuid",
            _ => "text",
        },
        Some("integer") => match resolved.format.as_deref() {
            Some("int32") => "integer",
            // Stripe uses format: "unix-time" for epoch seconds
            Some("unix-time") => "timestamptz",
            // int64 and others default to bigint for safety
            _ => "bigint",
        },
        Some("number") => match resolved.format.as_deref() {
            Some("float") => "real",
            // double and others default to double precision
            _ => "double precision",
        },
        Some("boolean") => "boolean",
        // array, object, and unknown types default to jsonb
        _ => "jsonb",
    }
}

/// Column definition for a foreign table
#[derive(Debug)]
pub struct ColumnDef {
    pub name: String,
    pub pg_type: &'static str,
    pub nullable: bool,
}

/// Find the schema describing a single record in a response, plus the JSON
/// pointer that reaches it (used to emit a deterministic `response_path`).
///
/// Mirrors the runtime extraction in `response.rs::extract_data`, so schema
/// generation and query-time data extraction agree on the row shape:
/// - a top-level array                         -> its items schema (no wrapper)
/// - an object with a wrapper-key property      -> that array's items schema,
///   whose value is an array (`data`/`results`/...) plus the pointer `/<key>`
/// - otherwise                                 -> the object itself (no wrapper)
///
/// Previously schema generation modeled the envelope object itself as the row
/// while the runtime unwrapped the nested array — producing all-NULL rows and a
/// rowid derived from a wrapper field for the most common REST list shape.
fn find_record_schema(schema: &Schema, spec: &OpenApiSpec) -> (Schema, Option<String>) {
    let resolved = spec.resolve_schema(schema);

    // Top-level array: the record is its items.
    if resolved.schema_type.as_deref() == Some("array") {
        let item = resolved
            .items
            .as_ref()
            .map(|s| spec.resolve_schema(s))
            .unwrap_or_default();
        return (item, None);
    }

    // Envelope object: a wrapper-key property whose resolved value is an array
    // holds the records (e.g. {data: [Charge]}). WRAPPER_KEYS precedence matches
    // runtime auto-detection so both modules pick the same field.
    for key in WRAPPER_KEYS {
        if let Some(prop) = resolved.properties.get(*key) {
            let prop_resolved = spec.resolve_schema(prop);
            if prop_resolved.schema_type.as_deref() == Some("array") {
                let item = prop_resolved
                    .items
                    .as_ref()
                    .map(|s| spec.resolve_schema(s))
                    .unwrap_or_default();
                return (item, Some(format!("/{key}")));
            }
        }
    }

    // Known limitation: only ARRAY-valued wrapper keys are unwrapped above. An
    // object-valued "pure envelope" (e.g. {data: {id, name}} where every sibling
    // is metadata) is NOT unwrapped here, though the runtime extract_data does
    // unwrap it. For that shape — rare for a collection endpoint — the generated
    // columns model the whole envelope and disagree with runtime extraction;
    // set the table's columns and response_path manually. (Faithfully mirroring
    // the runtime here would require replicating its pure-envelope metadata-key
    // heuristic at the schema level.)
    //
    // Otherwise the response object is itself the record.
    (resolved, None)
}

/// Extract column definitions from an OpenAPI response schema (unwrapping the
/// record envelope). Production code calls `find_record_schema` +
/// `columns_from_record` directly so it can reuse the record's envelope path in
/// the same pass; this convenience wrapper is retained for the schema tests.
#[cfg(test)]
pub(crate) fn extract_columns(
    schema: &Schema,
    spec: &OpenApiSpec,
    include_attrs: bool,
) -> Vec<ColumnDef> {
    let (record, _path) = find_record_schema(schema, spec);
    columns_from_record(&record, spec, include_attrs)
}

/// Build column definitions from an already-resolved record (object) schema.
fn columns_from_record(record: &Schema, spec: &OpenApiSpec, include_attrs: bool) -> Vec<ColumnDef> {
    let mut columns = Vec::new();

    // Check if this is an object with properties
    if !record.properties.is_empty() {
        // Track seen names to detect collisions after sanitization
        let mut seen: HashMap<String, usize> = HashMap::new();

        let mut sorted_props: Vec<_> = record.properties.iter().collect();
        sorted_props.sort_by_key(|(name, _)| *name);

        for (name, prop_schema) in sorted_props {
            // Skip writeOnly properties (e.g., "password") — not returned in GET responses
            if prop_schema.write_only {
                continue;
            }
            let pg_type = openapi_to_pg_type(prop_schema, spec);
            let nullable = !record.required.contains(name) || prop_schema.nullable;
            let base_name = sanitize_column_name(name);

            // Deduplicate: if this sanitized name was already used, append a suffix
            let count = seen.entry(base_name.clone()).or_insert(0);
            let final_name = if *count > 0 {
                format!("{base_name}_{count}")
            } else {
                base_name
            };
            *count += 1;

            columns.push(ColumnDef {
                name: final_name,
                pg_type,
                nullable,
            });
        }
    }

    // Sort columns alphabetically, but put 'id' first if present
    columns.sort_by(|a, b| match (a.name.as_str(), b.name.as_str()) {
        ("id", _) => std::cmp::Ordering::Less,
        (_, "id") => std::cmp::Ordering::Greater,
        _ => a.name.cmp(&b.name),
    });

    // Add an 'attrs' column for the full JSON response, unless disabled or already exists
    if include_attrs && !columns.iter().any(|c| c.name == "attrs") {
        columns.push(ColumnDef {
            name: "attrs".to_string(),
            pg_type: "jsonb",
            nullable: true,
        });
    }

    columns
}

/// Sanitize a column name for PostgreSQL (converts camelCase to snake_case)
///
/// Handles consecutive uppercase (acronyms) correctly:
/// - clusterIP becomes cluster_ip (not cluster_i_p)
/// - HTMLParser becomes html_parser (not h_t_m_l_parser)
/// - getHTTPSUrl becomes get_https_url
fn sanitize_column_name(name: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = name.chars().collect();

    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() && i > 0 {
            let prev = chars[i - 1];
            let next_is_lower = chars.get(i + 1).is_some_and(|n| n.is_lowercase());

            // Insert '_' before an uppercase letter when:
            // 1. Previous char is lowercase/digit (start of new word: "cluster|I|P")
            // 2. Previous char is uppercase but next is lowercase (end of acronym: "HTM|L|Parser")
            if prev.is_lowercase()
                || prev.is_ascii_digit()
                || (prev.is_uppercase() && next_is_lower)
            {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());
        } else if c.is_alphanumeric() || c == '_' {
            result.push(c.to_ascii_lowercase());
        } else {
            result.push('_');
        }
    }

    // PostgreSQL identifiers cannot start with a digit
    if result.starts_with(|c: char| c.is_ascii_digit()) {
        result.insert(0, '_');
    }

    result
}

/// Quote a PostgreSQL identifier (table name, column name, etc.)
/// Doubles any internal double quotes and wraps in double quotes.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Generate a CREATE FOREIGN TABLE statement for an endpoint (table name
/// derived from its path).
pub fn generate_foreign_table(
    endpoint: &EndpointInfo,
    spec: &OpenApiSpec,
    server_name: &str,
    include_attrs: bool,
) -> String {
    generate_foreign_table_named(
        endpoint,
        &endpoint.table_name(),
        spec,
        server_name,
        include_attrs,
    )
}

/// Generate a CREATE FOREIGN TABLE statement using an explicit (possibly
/// de-duplicated) table name.
fn generate_foreign_table_named(
    endpoint: &EndpointInfo,
    table_name: &str,
    spec: &OpenApiSpec,
    server_name: &str,
    include_attrs: bool,
) -> String {
    // Resolve the record schema and its envelope path in a SINGLE pass.
    // find_record_schema resolves $refs and clones, so calling it once here —
    // rather than once inside extract_columns (which discards the path) and
    // again to recover the path — halves the per-table cost. The columns come
    // from the record; response_path pins runtime extraction to the very
    // envelope these columns were derived from, instead of re-guessing the
    // wrapper key at query time.
    let (columns, response_path) = match endpoint.response_schema.as_ref() {
        Some(schema) => {
            let (record, path) = find_record_schema(schema, spec);
            (columns_from_record(&record, spec, include_attrs), path)
        }
        None => {
            // Default columns if no schema is available.
            let mut cols = vec![ColumnDef {
                name: "id".to_string(),
                pg_type: "text",
                nullable: false,
            }];
            if include_attrs {
                cols.push(ColumnDef {
                    name: "attrs".to_string(),
                    pg_type: "jsonb",
                    nullable: true,
                });
            }
            (cols, None)
        }
    };

    let column_defs: Vec<String> = columns
        .iter()
        .map(|col| {
            let not_null = if col.nullable { "" } else { " NOT NULL" };
            format!(
                "    {} {}{}",
                quote_identifier(&col.name),
                col.pg_type,
                not_null
            )
        })
        .collect();

    // Determine rowid_column: only when a column literally named 'id' exists.
    // The old alphabetical fallback could pick a non-unique field (amount,
    // status, name); the read path then treats any '=' on it as a
    // single-resource path lookup, and UPDATE/DELETE would target the wrong
    // remote resource. Tables without 'id' require an explicit rowid_column.
    let rowid_col: Option<&str> = columns
        .iter()
        .find(|c| c.name == "id")
        .map(|c| c.name.as_str());

    // Escape single quotes in option values for SQL
    let escaped_endpoint = endpoint.path.replace('\'', "''");

    let mut option_parts = vec![format!("    endpoint '{escaped_endpoint}'")];

    // Pin the response envelope path so runtime extraction matches the columns
    // generated here (rather than re-guessing the wrapper key at query time).
    if let Some(ref path) = response_path {
        let escaped_path = path.replace('\'', "''");
        option_parts.push(format!("    response_path '{escaped_path}'"));
    }

    if endpoint.method != "GET" {
        option_parts.push(format!("    method '{}'", endpoint.method));
    }

    if let Some(rowid) = rowid_col {
        let escaped_rowid = rowid.replace('\'', "''");
        option_parts.push(format!("    rowid_column '{escaped_rowid}'"));
    }

    let options = option_parts.join(",\n");

    format!(
        r"CREATE FOREIGN TABLE IF NOT EXISTS {} (
{}
)
SERVER {} OPTIONS (
{}
)",
        quote_identifier(table_name),
        column_defs.join(",\n"),
        quote_identifier(server_name),
        options
    )
}

/// Generate CREATE FOREIGN TABLE statements for all endpoints in a spec.
///
/// Only GET endpoints are auto-imported. A POST endpoint may be a create, and a
/// plain SELECT on such a table would trigger a remote side effect; the
/// Operation model also can't send a request body, so an auto-imported
/// POST-as-search table wouldn't work anyway. Create POST-backed tables
/// manually with the intended method/request_body options.
pub fn generate_all_tables(
    spec: &OpenApiSpec,
    server_name: &str,
    filter: Option<&[String]>,
    exclude: bool,
    include_attrs: bool,
) -> Vec<String> {
    let endpoints = spec.endpoints();

    // De-duplicate generated table names: distinct paths can normalize to the
    // same identifier (e.g. /a-b, /a.b, /a_b → a_b). Without a suffix, the
    // second `CREATE FOREIGN TABLE IF NOT EXISTS` would be a silent no-op.
    let mut used: HashMap<String, usize> = HashMap::new();

    endpoints
        .iter()
        .filter(|e| e.method != "POST")
        .filter(|e| {
            let table_name = e.table_name();
            match filter {
                None => true,
                Some(list) if exclude => !list.iter().any(|n| n == &table_name),
                Some(list) => list.iter().any(|n| n == &table_name),
            }
        })
        .map(|e| {
            let base = e.table_name();
            let count = used.entry(base.clone()).or_insert(0);
            let ddl = if *count == 0 {
                // First (and usually only) endpoint for this name: use the base.
                generate_foreign_table(e, spec, server_name, include_attrs)
            } else {
                generate_foreign_table_named(
                    e,
                    &format!("{base}_{count}"),
                    spec,
                    server_name,
                    include_attrs,
                )
            };
            *count += 1;
            ddl
        })
        .collect()
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
