# OpenAPI WASM Foreign Data Wrapper

This is a WASM-based Foreign Data Wrapper (FDW) for integrating any OpenAPI 3.0+ compliant REST API into PostgreSQL through Supabase Wrappers.

## Overview

The OpenAPI FDW allows querying REST APIs directly from PostgreSQL using SQL. It dynamically parses OpenAPI specifications and creates foreign tables for API endpoints.

### Key Features

- **Dynamic Schema Import**: Automatically generate foreign tables from OpenAPI specs
- **Path Parameter Substitution**: Use WHERE clauses to inject path parameters
- **Automatic Pagination**: Cursor-based and URL-based pagination support
- **Flexible Response Parsing**: Auto-detect or configure data extraction paths
- **Rate Limiting**: Automatic retry with backoff for 429 responses
- **Limit Pushdown**: Stop pagination early when LIMIT is satisfied

## Architecture

```
+------------------------------------------+
| PostgreSQL        (SQL queries)          |
+------------------------------------------+
| Supabase Wrappers (WASM host interface)  |
+------------------------------------------+
| OpenAPI FDW       (Generic API handling) |
|   |-- Spec Parser (OpenAPI 3.0+ parsing) |
|   |-- Schema Gen  (Table generation)     |
|   |-- Pagination  (Cursor/URL-based)     |
+------------------------------------------+
| Any REST API      (HTTP endpoints)       |
+------------------------------------------+
```

## Project Structure

- **src/lib.rs**: Core FDW implementation
- **src/spec.rs**: OpenAPI specification parsing with allOf/oneOf/anyOf support
- **src/schema.rs**: Schema generation and PostgreSQL type mapping
- **wit/world.wit**: WebAssembly interface definitions

## Usage

### Server Setup

```sql
CREATE SERVER my_api_server
FOREIGN DATA WRAPPER wasm_wrapper
OPTIONS (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/...',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.1.0',
    fdw_package_checksum '...',
    base_url 'https://api.example.com/v1',
    api_key_id '<vault_secret_id>'
);
```

### Import Schema from OpenAPI Spec

```sql
-- Import all endpoints as tables
IMPORT FOREIGN SCHEMA openapi
FROM SERVER my_api_server
INTO public
OPTIONS (spec_url 'https://api.example.com/openapi.json');
```

### Manual Table Definition

```sql
CREATE FOREIGN TABLE users (
    id text,
    name text,
    email text,
    created_at timestamptz,
    attrs jsonb
)
SERVER my_api_server
OPTIONS (
    endpoint '/users',
    rowid_column 'id'
);
```

### Query Examples

```sql
-- Get all users
SELECT * FROM users;

-- Get a specific user by ID (uses rowid pushdown)
SELECT * FROM users WHERE id = '123';

-- With LIMIT (stops pagination early)
SELECT * FROM users LIMIT 10;
```

### Path Parameter Substitution

For APIs with path parameters like `/users/{user_id}/posts`:

```sql
CREATE FOREIGN TABLE user_posts (
    id text,
    user_id text,
    title text,
    body text,
    attrs jsonb
)
SERVER my_api_server
OPTIONS (
    endpoint '/users/{user_id}/posts',
    rowid_column 'id'
);

-- Query automatically substitutes user_id from WHERE clause
SELECT * FROM user_posts WHERE user_id = '123';
-- Results in: GET /users/123/posts
```

### GeoJSON APIs

For GeoJSON APIs where data is nested in `features[].properties`:

```sql
CREATE FOREIGN TABLE weather_alerts (
    id text,
    event text,
    headline text,
    description text,
    severity text,
    attrs jsonb
)
SERVER weather_api
OPTIONS (
    endpoint '/alerts/active',
    response_path '/features',
    object_path '/properties',
    rowid_column 'id'
);
```

## Configuration Options

### Server Options

| Option | Description | Required |
|--------|-------------|----------|
| base_url | Base URL for API requests | Yes* |
| spec_url | URL to OpenAPI spec (for import_foreign_schema) | No |
| api_key | API key for authentication | No |
| api_key_id | Vault key ID for API key (alternative to api_key) | No |
| api_key_header | Header name for API key (default: Authorization) | No |
| api_key_prefix | Prefix for API key value | No |
| bearer_token | Bearer token for auth | No |
| bearer_token_id | Vault key ID for bearer token | No |
| headers | Custom headers as JSON object | No |
| user_agent | Custom User-Agent header | No |
| accept | Accept header for content negotiation | No |
| page_size | Default page size for pagination | No |
| page_size_param | Query param name for page size (default: limit) | No |
| cursor_param | Query param name for cursor (default: after) | No |

*Required unless spec_url provides a server URL

### Table Options

| Option | Description | Required |
|--------|-------------|----------|
| endpoint | API endpoint path (e.g., /users) | Yes |
| rowid_column | Column used for single-resource access (default: id) | No |
| response_path | JSON pointer to extract data (e.g., /data) | No |
| object_path | JSON pointer for nested object extraction (e.g., /properties) | No |
| cursor_path | JSON pointer for pagination cursor | No |
| cursor_param | Override server cursor_param | No |
| page_size_param | Override server page_size_param | No |
| page_size | Override server page_size | No |

## Query Pushdown

| Feature | Description |
|---------|-------------|
| WHERE id = | Single resource access via rowid_column |
| WHERE {param} = | Path parameter substitution |
| WHERE field = | Query parameters for non-path fields |
| LIMIT | Stop pagination early when limit is reached |

## Pagination

The FDW automatically handles pagination through multiple strategies:

1. **Cursor-based**: Uses `cursor_param` and `cursor_path` options
2. **URL-based**: Follows `next` links in response metadata
3. **Auto-detection**: Tries common patterns (`/meta/pagination/next`, `/links/next`, etc.)

## Rate Limiting

The FDW automatically handles HTTP 429 (Too Many Requests) responses:

- Parses `Retry-After` header for delay duration
- Falls back to exponential backoff (1s, 2s, 4s) if header is missing
- Retries up to 3 times before failing
- Rate limit handling is per-request (resets between pagination pages)

## Type Mapping

| OpenAPI Type | PostgreSQL Type |
|--------------|-----------------|
| string | text |
| string (date) | date |
| string (date-time) | timestamptz |
| integer (int32) | integer |
| integer (int64) | bigint |
| number (float) | real |
| number (double) | double precision |
| boolean | boolean |
| array | jsonb |
| object | jsonb |

## Development

### Building

```bash
cd wasm-wrappers/fdw/openapi_fdw
cargo component build --release --target wasm32-unknown-unknown
```

### Running Tests

```bash
# Unit tests
cargo test

# Integration tests (from wrappers directory)
cd wrappers
cargo pgrx test --features "wasm_fdw pg16"
```

## Limitations

- Read-only (no INSERT/UPDATE/DELETE support)
- Only GET endpoints are supported
- Authentication limited to API key and Bearer token
- No OAuth2 flow support (use pre-obtained tokens)
