---
source:
documentation: https://spec.openapis.org/
author: Cody Bromley(https://github.com/codybrom)
tags:
  - wasm
  - api
  - community
---

# OpenAPI

[OpenAPI](https://www.openapis.org/) is a specification for describing HTTP APIs. The OpenAPI Wrapper is a generic WebAssembly (Wasm) foreign data wrapper that can connect to any REST API with an OpenAPI 3.0+ specification.

This wrapper allows you to query any REST API endpoint as a PostgreSQL foreign table, with support for path parameters, pagination, POST-for-read endpoints, automatic schema import, and data modification (INSERT / UPDATE / DELETE) on tables that opt in.

## Available Versions

| Version | Wasm Package URL | Checksum | Required Wrappers Version |
| ------- | ---------------- | -------- | ------------------------- |
| 0.3.0 | `https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.3.0/openapi_fdw.wasm` | `tbd` | >=0.6.2 |
| 0.2.1 | `https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.1/openapi_fdw.wasm` | `12c902f3089e18142a1d8d35c66b9ceb85c193224229687bd929aff6b44cddde` | >=0.6.2 |
| 0.2.0 | `https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm` | `f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa` | >=0.5.0 |
| 0.1.4 | `https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.1.4/openapi_fdw.wasm` | `dd434f8565b060b181d1e69e1e4d5c8b9c3ac5ca444056d3c2fb939038d308fe` | >=0.5.0 |

## Preparation

Before you can query an API, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the OpenAPI Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your API key in Vault and retrieve the created `key_id`
select vault.create_secret(
  'your-api-key',
  'my_api',
  'API key for My API'
);
```

### Connecting to an API

We need to provide Postgres with the credentials to access the API and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server my_api_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
        fdw_package_name 'supabase:openapi-fdw',
        fdw_package_version '0.2.0',
        fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
        base_url 'https://api.example.com/v1',
        api_key_id '<key_ID>'  -- The Key ID from Vault
      );
    ```

=== "Without Vault"

    ```sql
    create server my_api_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
        fdw_package_name 'supabase:openapi-fdw',
        fdw_package_version '0.2.0',
        fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
        base_url 'https://api.example.com/v1',
        api_key 'your-api-key'
      );
    ```

### Server Options

| Option | Required | Description |
| ------ | :------: | ----------- |
| `fdw_package_*` | Yes | Standard Wasm FDW package metadata. See [Available Versions](#available-versions). |
| `base_url` | Yes* | Base URL for the API (e.g., `https://api.example.com/v1`). *Optional if `spec_url` or `spec_json` provides servers. |
| `spec_url` | No | URL to the OpenAPI specification (JSON or YAML). Required for `IMPORT FOREIGN SCHEMA`. Mutually exclusive with `spec_json`. |
| `spec_json` | No | Inline OpenAPI 3.0+ JSON spec for `IMPORT FOREIGN SCHEMA`. Mutually exclusive with `spec_url`. Useful when the API doesn't publish a spec URL. |
| `api_key` | No | API key for authentication. |
| `api_key_id` | No | Vault secret key ID storing the API key. Use instead of `api_key`. |
| `api_key_header` | No | Header name for API key (default: `Authorization`). |
| `api_key_prefix` | No | Prefix for API key value (default: `Bearer` for Authorization header). |
| `api_key_location` | No | Where to send the API key: `header` (default), `query`, or `cookie`. |
| `bearer_token` | No | Bearer token for authentication (alternative to `api_key`). |
| `bearer_token_id` | No | Vault secret key ID storing the bearer token. |
| `auth_token_setting` | No | Name of a Postgres session variable (GUC) to read the auth token from at request time, e.g. `app.api_token`. When set and non-empty it overrides any static credential for that request. See [Per-request credentials](#per-request-credentials-session-variables). |
| `auth_token_prefix` | No | Prefix for the `auth_token_setting` value in the Authorization header (default: `Bearer`). Set to an empty string to send the raw token. |
| `user_agent` | No | Custom User-Agent header value. |
| `accept` | No | Custom Accept header for content negotiation (e.g., `application/geo+json`). |
| `headers` | No | Custom headers as JSON object (e.g., `'{"X-Custom": "value"}'`). |
| `include_attrs` | No | Include `attrs` jsonb column in `IMPORT FOREIGN SCHEMA` output (default: `'true'`). Set to `'false'` to omit. |
| `page_size` | No | Default page size for pagination (0 = no automatic limit). |
| `page_size_param` | No | Query parameter name for page size (default: `limit`). |
| `cursor_param` | No | Query parameter name for pagination cursor (default: `after`). |
| `page_param` | No | Query parameter to increment for page-number pagination (e.g. `page`). Opt-in; requires `has_more_path` on the table. |
| `max_pages` | No | Maximum pages per scan to prevent infinite pagination loops (default: `1000`). |
| `max_response_bytes` | No | Maximum response body size in bytes (default: `52428800` / 50 MiB). |
| `debug` | No | Emit HTTP request details and scan stats via PostgreSQL INFO messages when set to `'true'` or `'1'`. |

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists openapi;
```

## Creating Foreign Tables

### Manual Table Creation

Create foreign tables manually by specifying the endpoint and columns:

```sql
create foreign table openapi.users (
  id text,
  name text,
  email text,
  created_at timestamptz,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/users',
  rowid_column 'id'
);
```

### Table Options

| Option | Required | Description |
| ------ | :------: | ----------- |
| `endpoint` | Yes | API endpoint path (e.g., `/users`, `/users/{user_id}/posts`). |
| `rowid_column` | No | Column used as row identifier for single-resource access and modifications (default: `id`). Optional for data scan, required for data modify. |
| `response_path` | No | JSON pointer to extract data array from response (e.g., `/data`, `/results`). |
| `object_path` | No | JSON pointer to extract nested object from each row (e.g., `/properties` for GeoJSON). |
| `cursor_path` | No | JSON pointer to pagination cursor in response. |
| `cursor_param` | No | Override server-level cursor parameter name. |
| `has_more_path` | No | JSON pointer to the boolean saying another page exists (e.g. `/info/more_records`). Required with `page_param`. |
| `error_path` | No | JSON pointer to an in-band error indicator in an otherwise successful (2xx) response, e.g. `/status`. Opt-in; disabled when unset. |
| `error_value` | No | Value at `error_path` that means "error", e.g. `error`. When unset, any non-null, non-`false` value counts. |
| `error_message_path` | No | JSON pointer to the API's own error message, surfaced in the raised error, e.g. `/message`. |
| `page_param` | No | Override the server-level page parameter name. |
| `page_size_param` | No | Override server-level page size parameter name. |
| `page_size` | No | Override server-level page size. |
| `method` | No | HTTP method for this endpoint. Use `POST` for read-via-POST endpoints (default: `GET`). |
| `request_body` | No | Request body string for POST endpoints. |

#### Write Options

These options enable and shape INSERT / UPDATE / DELETE; see [Data Modify](#data-modify-insert-update-delete). All are optional and writes stay disabled until `writable` is `'true'`.

| Option | Default | Description |
| ------ | ------- | ----------- |
| `writable` | `false` | Capability gate. DML on a table without `writable 'true'` errors before any HTTP request. |
| `insert_method` | unset → INSERT disabled | HTTP verb for INSERT (`POST`/`PUT`/`PATCH`). Presence enables INSERT. |
| `update_method` | unset → UPDATE disabled | HTTP verb for UPDATE (`PUT`/`PATCH`/`POST`). Presence enables UPDATE. |
| `delete_method` | unset → DELETE disabled | HTTP verb for DELETE (`DELETE`, or `POST` for soft-delete). Presence enables DELETE. |
| `write_endpoint` | falls back to `endpoint` | Path template for writes when it differs from the read endpoint. Per-op `insert_endpoint`/`update_endpoint`/`delete_endpoint` are also accepted, falling back to `write_endpoint` then `endpoint`. |
| `rowid_location` | `url` | Rowid placement for UPDATE/DELETE: `url` → `.../{rowid}` path suffix, `body` → injected under `rowid_body_key`, `query` → `...?{rowid_param}={rowid}`. |
| `update_rowid_location`, `delete_rowid_location` | fall back to `rowid_location` | Per-verb overrides, for APIs that mix placements on one resource (e.g. `url` for UPDATE but `query` for DELETE). |
| `rowid_body_key` | = `rowid_column` | JSON key the rowid is written under when the effective rowid location is `body`. |
| `rowid_param` | = `rowid_column` | Query parameter name when the effective rowid location is `query` (e.g. `ids`). |
| `body_root_path` | unset → bare body | JSON pointer wrapping the body. `/data` plus `body_wrap 'array'` gives `{"data":[{...}]}`. |
| `body_wrap` | `object` | `object` → `{root:{...}}`; `array` → `{root:[{...}]}`. `array` without `body_root_path` is an error. |
| `success_path` | unset | JSON pointer to the per-record outcome in a 2xx response body (e.g. `/data/0/code`). See [Body-level success checking](#body-level-success-checking). |
| `success_value` | `SUCCESS` | Expected value at `success_path`. |
| `success_status` | unset → accept any 2xx except 207 | HTTP status allowlist for writes (e.g. `200,201,202`). HTTP 207 is always rejected. |

### Automatic Schema Import

If you provide a `spec_url` or `spec_json` in the server options, you can automatically import table definitions:

```sql
-- Import all endpoints
import foreign schema openapi
  from server my_api_server
  into api;

-- Import specific endpoints only
import foreign schema openapi
  limit to ("users", "orders")
  from server my_api_server
  into api;

-- Import all except specific endpoints
import foreign schema openapi
  except ("internal_endpoint")
  from server my_api_server
  into api;
```

!!! note
    `IMPORT FOREIGN SCHEMA` only generates tables for non-parameterized GET endpoints (e.g., `/users`, `/orders`). Endpoints with path parameters like `/users/{user_id}/posts` are skipped because they require WHERE clause values at query time. Create these tables manually using the `endpoint` option with `{param}` placeholders — see [Path Parameters](#path-parameters) for examples.

!!! warning "Changed in 0.3.0"
    Two `IMPORT FOREIGN SCHEMA` behaviors changed. Both make generated tables safer, and both can require action on an existing setup:

    **`rowid_column` is emitted only when the record has an `id` field.** Earlier versions fell back to "the first non-`attrs`, non-`jsonb` column" in alphabetical order, which could select a non-unique field such as `amount`, `name` or `status`. That is actively dangerous: the read path treats any `=` filter on the rowid column as a [single-resource lookup](#single-resource-access), and `UPDATE`/`DELETE` address the remote resource by it — so a wrong choice silently targets the wrong record. Tables whose record has no `id` field are now imported without a `rowid_column`; add it explicitly:

    ```sql
    alter foreign table api.orders options (add rowid_column 'order_number');
    ```

    Re-import after upgrading and check for tables that lost the option, particularly any table you write to.

    **POST endpoints are no longer imported.** A POST endpoint is usually a create, and a plain `SELECT` on such a table would trigger a remote side effect. (The `Operation` model also cannot send a request body, so an auto-imported POST-as-search table would not have worked.) Create POST-backed tables manually with the intended `method` and `request_body` options — see [POST-for-Read Endpoints](#post-for-read-endpoints).

!!! note "Envelope handling during import"
    When a response schema is an envelope whose wrapper key (`data`, `results`, `items`, ...) holds an **array**, the generated columns come from the record inside it and a matching `response_path` is emitted, so import and query time agree on the row shape.

    An envelope whose wrapper key holds a single **object** (e.g. `{"data": {...}, "meta": {...}}`) is not unwrapped during import, though `extract_data` does unwrap it at query time. For that shape the generated columns describe the envelope rather than the record. It is rare on a collection endpoint; if you hit it, set the table's columns and `response_path` manually.

## Path Parameters

The OpenAPI FDW supports path parameter substitution. Define parameters in the endpoint template using `{param_name}` syntax, and provide values via WHERE clauses:

```sql
-- Endpoint template with path parameter
create foreign table openapi.user_posts (
  user_id text,
  id text,
  title text,
  body text,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/users/{user_id}/posts',
  rowid_column 'id'
);

-- Query with path parameter - generates GET /users/123/posts
select * from openapi.user_posts where user_id = '123';
```

### Multiple Path Parameters

```sql
create foreign table openapi.project_issues (
  org text,
  repo text,
  id text,
  title text,
  status text,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/projects/{org}/{repo}/issues',
  rowid_column 'id'
);

-- Generates GET /projects/acme/widgets/issues
select * from openapi.project_issues where org = 'acme' and repo = 'widgets';
```

## Query Pushdown

### Single Resource Access

When filtering by the `rowid_column`, the FDW automatically requests a single resource:

```sql
-- Generates GET /users/user-123
select * from openapi.users where id = 'user-123';
```

### Query Parameters

Other WHERE clause filters are passed as query parameters:

```sql
-- Generates GET /users?status=active
select * from openapi.users where status = 'active';
```

Columns used as query or path parameters always return the value from the WHERE clause, even if the API response contains the same field with different casing. This ensures PostgreSQL's post-filter always passes.

### LIMIT Pushdown

When your query includes a `LIMIT`, the FDW uses it as the `page_size` for the first API request, reducing unnecessary data transfer:

```sql
-- Sends GET /users?limit=5 (uses LIMIT as page_size)
select * from openapi.users limit 5;
```

## POST-for-Read Endpoints

Some APIs use POST requests for read operations (e.g., search or query endpoints). Use the `method` and `request_body` table options:

```sql
create foreign table openapi.search_results (
  id text,
  title text,
  score real,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/search',
  method 'POST',
  request_body '{"query": "openapi", "limit": 50}'
);

select id, title, score from openapi.search_results;
```

## Data Modify (INSERT, UPDATE, DELETE)

Foreign tables can opt in to data modification with the `writable 'true'` option plus a per-operation HTTP verb. Each operation is enabled independently:

| Operation | Enabled by | Disabled behavior |
| --------- | ---------- | ----------------- |
| INSERT | `insert_method` | statement errors |
| UPDATE | `update_method` | statement errors |
| DELETE | `delete_method` | statement errors |
| TRUNCATE | — | not supported |

`rowid_column` is required for any data modify (it identifies the record for UPDATE/DELETE), and all misconfiguration is rejected before any HTTP request is made.

Writes are strictly per-row: a statement affecting N rows issues N HTTP requests. The JSON body is built from the row's columns with faithful types (numbers stay JSON numbers, booleans stay booleans, `jsonb` columns pass through as nested JSON). Null columns are omitted, which keeps `PATCH` bodies sparse. The `attrs` catch-all column and columns consumed as `{param}` path placeholders are never sent in the body.

### Basic example

A plain JSON API with the record id in the URL path (the GitHub style):

```sql
create foreign table openapi.gh_pulls (
  number bigint,
  title text,
  state text,
  attrs jsonb
)
server github_server
options (
  endpoint '/repos/octocat/hello/pulls',
  rowid_column 'number',
  writable 'true',
  update_method 'PATCH'
);

update openapi.gh_pulls set title = 'New title', state = 'closed' where number = 42;
-- PATCH .../pulls/42   body: {"title":"New title","state":"closed"}  (number in URL, not body)
```

INSERT against a parameterized endpoint substitutes `{param}` placeholders from the inserted row's columns, and excludes those columns from the body:

```sql
create foreign table openapi.gh_comments (
  owner text,
  repo text,
  issue_number bigint,
  body text
)
server github_server
options (
  endpoint '/repos/{owner}/{repo}/issues/{issue_number}/comments',
  rowid_column 'id',
  writable 'true',
  insert_method 'POST'
);

insert into openapi.gh_comments (owner, repo, issue_number, body)
values ('octocat', 'hello', 7, 'Thanks, merging.');
-- POST /repos/octocat/hello/issues/7/comments   body: {"body":"Thanks, merging."}
```

!!! note "Path parameters on UPDATE and DELETE"

    INSERT can substitute any `{param}` placeholder from the inserted row, as above. UPDATE and DELETE cannot: PostgreSQL hands the FDW only the SET columns plus the rowid, so the only placeholder a write endpoint can fill from the row is the rowid column itself (as in `/records/{id}`). Any other path parameter must be written statically into the endpoint option (as in the `gh_pulls` example above), or included in the SET list.

!!! warning "Filter UPDATE and DELETE by the rowid column"

    The scan that selects rows to modify optimistically pushes equality filters as query parameters and injects the filtered value back into returned rows so PostgreSQL's re-check passes. If the API ignores the parameter (for example `where title = '...'` against an endpoint with no `title` filter), the scan matches **every** row, and the statement modifies all of them. Filter writes by `rowid_column` or by parameters the API genuinely supports; to filter locally on other fields, use the `attrs` column (`attrs->>'title' = '...'`), which is never pushed down or injected.

### Rowid placement

`rowid_location` controls where the record id goes on UPDATE/DELETE: appended to the URL path (`url`, the default), injected into the JSON body under `rowid_body_key` (`body`), or sent as a query parameter named `rowid_param` (`query`). Per-verb overrides (`update_rowid_location`, `delete_rowid_location`) express APIs that mix placements on a single resource.

### Body envelope

`body_root_path` and `body_wrap` wrap the record for envelope-style APIs. A full example for a CRM-style API that wants `PUT /records/{id}` with an array envelope, signals per-record success inside HTTP 200/202 bodies, and deletes via a query parameter:

```sql
create foreign table openapi.crm_records (
  id text,
  "Stage" text,
  "Amount" numeric,
  "Owner" jsonb,
  attrs jsonb
)
server crm_server
options (
  endpoint '/records',
  rowid_column 'id',
  writable 'true',
  insert_method 'POST',
  update_method 'PUT',
  update_rowid_location 'url',                    -- PUT /records/{id}
  delete_method 'DELETE',
  delete_rowid_location 'query', rowid_param 'ids',  -- DELETE /records?ids=<id>
  body_root_path '/data', body_wrap 'array',      -- {"data":[ {...} ]}
  success_path '/data/0/code', success_value 'SUCCESS',
  success_status '200,201,202'                    -- insert returns 202
);

update openapi.crm_records set "Stage" = 'Qualification', "Amount" = 8000
where id = '1000000000000489124';
-- PUT /records/1000...  body: {"data":[{"Stage":"Qualification","Amount":8000}]}
-- HTTP 200 with /data/0/code == "SUCCESS" → ok; any other code → statement error
```

Nested object fields (like `"Owner"` above) are written through `jsonb` columns, which pass into the body as parsed JSON.

### Body-level success checking

Some APIs return HTTP 2xx and signal per-record failure inside the response body (e.g. HTTP 202 with a record code other than `SUCCESS`). A status check alone would silently treat those failed writes as successful. Set `success_path` (and optionally `success_value`) so the FDW verifies the outcome inside every 2xx write response and raises a statement error on failure.

When a writable table's options signal such an API — an explicit `success_status` containing codes beyond `200`/`201`/`204`, or a `body_root_path` envelope — `success_path` is required, and the statement errors before any HTTP request if it's missing. HTTP 207 Multi-Status responses are always rejected, since per-record outcomes inside them cannot be verified.

An empty response body (for example `204 No Content`, common on DELETE) carries no failure signal, so the `success_path` body check is skipped for it and only the HTTP status is checked — a `success_path` configured for enveloped UPDATEs does not force DELETE responses to carry a body.

!!! warning "Writes are not transactional"

    Writes cannot be rolled back. Each affected row issues an immediate HTTP request. An UPDATE over 500 rows that fails on row 300 leaves rows 1–299 permanently written on the remote API and aborts with a SQL error; `ROLLBACK` cannot undo HTTP calls, and `RETURNING` is unavailable to even report which rows succeeded. A `statement_timeout` firing mid-sequence leaves the same half-mutated state. Prefer small batches.

!!! warning "Transient failures can double-apply POST/PATCH"

    The Wrappers host retries transient HTTP failures (5xx, timeouts) up to 3 times, including on non-idempotent POST/PATCH requests. A write the remote API already applied before returning a transient error may be silently re-sent — a double-create or double-apply. The FDW itself never re-sends a write, so at most the host's retries apply. Prefer APIs with server-side idempotency where duplicates matter.

### Write limitations

- **No `RETURNING`** — rejected at statement planning. Server-assigned ids returned only in the response body are not visible to SQL; re-select to observe them.
- **The rowid column cannot be changed** — `UPDATE ... SET <rowid_column> = ...` does not send the new value: the Wrappers framework strips the rowid column from the updated row before it reaches the FDW, and the request is still keyed and placed by the pre-update rowid. Change a record's identifier out of band, not through the foreign table.
- **No batching** — a 100-row statement issues 100 requests, not one bulk call. Each request is a blocking round-trip, so large statements hold the connection; be mindful of `statement_timeout`.
- **JSON bodies only** — form-encoded write bodies (`application/x-www-form-urlencoded`) are not supported.
- **`IMPORT FOREIGN SCHEMA` stays read-only** — imported tables never carry write options; add `writable 'true'` and the per-op options manually.

!!! note "Privileges"

    `writable` is a capability flag, not a privilege boundary. Writes execute under the server's single credential, so granting INSERT/UPDATE/DELETE on a writable foreign table effectively grants use of that credential; row-level security does not constrain FDW writes. Apply least-privilege `GRANT`s.

## Debug Mode

Enable debug mode to see HTTP request details and scan statistics in PostgreSQL INFO messages:

```sql
create server debug_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
    base_url 'https://api.example.com',
    debug 'true'
  );
```

Debug output includes:

- HTTP method and URL for each request
- Response status code and body size
- Total rows fetched and pages retrieved
- Pagination details
- A configured `response_path` that was not found in the response (extraction fell back to auto-detection)
- Columns that read as `NULL` because the value could not be converted to the column's Postgres type
- Columns that read as `NULL` because the JSON key matched only case-insensitively or after normalization (see below)

!!! tip "Diagnosing a column that is unexpectedly all NULL"
    Column-to-key matching is resolved once, from the first row of the first page. A column absent from that row is retried per row using only the exact and camelCase key lookups, so a key that appears in later rows under a different case or with punctuation (`User_Name`, `@id`) is not selected and the column reads `NULL`.

    Debug mode names the key it found, so the fix is usually to rename the column to match the API key exactly, or to add an explicit `response_path` if the wrong object is being scanned. Note that debug mode only reports — it never changes which value is selected.

## Pagination

The FDW automatically handles pagination. It supports:

1. **Cursor-based pagination** - Uses `cursor_param` and `cursor_path`
2. **Page-number pagination** - Uses `page_param` and `has_more_path`, for APIs that send neither a cursor nor a next URL
3. **URL-based pagination** - Follows `next` links in response body (e.g., `/links/next`, `/meta/pagination/next`)
4. **`Link` header pagination** - Follows [RFC 8288](https://datatracker.ietf.org/doc/html/rfc8288) `Link: <...>; rel="next"` response headers (GitHub, GitLab, and most REST APIs)
5. **Offset-based pagination** - Auto-detected from common patterns

### Configuring Pagination

```sql
create server paginated_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
    base_url 'https://openapi.example.com',
    page_size '100',
    page_size_param 'limit',
    cursor_param 'cursor'
  );
```

```sql
create foreign table openapi.items (
  id text,
  name text,
  attrs jsonb
)
server paginated_api
options (
  endpoint '/items',
  cursor_path '/meta/next_cursor'
);
```

#### Page-number pagination

Some APIs send neither a cursor nor a next URL: you advance by incrementing a
page number, and a boolean elsewhere in the response tells you whether to keep
going. Zoho is the common example, with `info.more_records`.

```sql
create server zoho_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.1/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.1',
    fdw_package_checksum '12c902f3089e18142a1d8d35c66b9ceb85c193224229687bd929aff6b44cddde',
    base_url 'https://www.zohoapis.com',
    page_param 'page',
    page_size_param 'per_page',
    page_size '200'
  );

create foreign table zoho.records (
  id text,
  name text,
  attrs jsonb
)
server zoho_api
options (
  endpoint '/crm/v2/Deals',
  response_path '/data',
  has_more_path '/info/more_records'
);
```

The first request carries no page parameter, so the API's own default first
page applies; subsequent requests send `page=2`, `page=3`, and so on.

!!! warning
    `page_param` and `has_more_path` are two halves of one mechanism and must be
    set together — either alone is rejected at scan start. Without a stop signal
    the scan would silently return only the first page, which is the failure
    this option exists to prevent. `page_param` also cannot be combined with
    `cursor_path`.

    Page mode is exclusive: once `page_param` is set, `Link` headers and
    next-URL/cursor auto-detection are not consulted, so a stray `next` field in
    a response cannot redirect paging.

    Scanning still stops on any of: the flag reading false, a page returning no
    rows, a satisfied `LIMIT`, or the `max_pages` server option (default 1000).

## GeoJSON Support

For APIs that return GeoJSON, use `object_path` to extract properties:

```sql
create foreign table openapi.locations (
  id text,
  name text,
  category text,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/locations',
  response_path '/features',
  object_path '/properties'
);
```

## Supported Data Types

| Postgres Type | JSON Type |
| ------------- | --------- |
| text | string |
| boolean | boolean |
| smallint* | number |
| integer | number |
| bigint | number |
| real | number |
| double precision | number |
| numeric* | number |
| date | string (ISO 8601) |
| timestamp* | string (ISO 8601) |
| timestamptz | string (ISO 8601) |
| jsonb | object/array |
| uuid | string |

\* Types marked with an asterisk work when you define tables manually, but `IMPORT FOREIGN SCHEMA` won't generate columns with these types automatically.

### The `attrs` Column

Any foreign table can include an `attrs` column of type `jsonb` to capture the entire raw JSON response for each row:

```sql
create foreign table openapi.users (
  id text,
  name text,
  attrs jsonb  -- Contains full JSON object
)
server my_api_server
options (endpoint '/users');
```

### Errors reported inside a 2xx response

Some APIs report failure in the response body rather than the status line --
rate limiting and rejected requests both arrive that way. Without configuration
such a response extracts to zero rows and reads as an empty result, which is
wrong data presented as success. `error_path` makes it an error instead:

```sql
create foreign table zoho.records (
  id text,
  attrs jsonb
)
server zoho_api
options (
  endpoint '/crm/v2/Deals',
  response_path '/data',
  error_path '/status',
  error_value 'error',
  error_message_path '/message'
);
```

A response of `{"code": "RATE_LIMIT_EXCEEDED", "message": "API rate limit
exceeded.", "status": "error"}` then raises with the API's own message rather
than returning nothing.

!!! note "Empty responses"
    A successful response with an empty body is treated as zero rows, not a
    parse error. Several APIs answer `204` (or a body-less `200`) instead of an
    empty collection when a lookup matches nothing.

## Limitations

- **Writes are opt-in and non-transactional**: INSERT, UPDATE, and DELETE require `writable 'true'` plus per-operation options, issue one immediate HTTP request per row, and cannot be rolled back. `RETURNING` is not supported. See [Data Modify](#data-modify-insert-update-delete).
- **No transactions**: Each SQL statement results in immediate HTTP requests; there is no transactional grouping.
- **Authentication**: Supports API Key and Bearer Token authentication, either static (server option or Vault) or resolved per request from a session variable (see [Per-request credentials](#per-request-credentials-session-variables)). The FDW does not run OAuth flows itself, but a session variable lets you supply a token your application already obtained.
- **OpenAPI version**: Only OpenAPI 3.0+ specifications are supported (not Swagger 2.0).

## Automatic Retries

The FDW automatically retries transient HTTP errors on read requests up to 3 times:

- **HTTP 429** (Rate Limit), **502** (Bad Gateway), **503** (Service Unavailable)
- **Retry-After header**: Respects server-specified delay when provided
- **Exponential backoff**: Falls back to 1s, 2s, 4s delays when no Retry-After header is present

Write requests are never retried by the FDW itself, though the Wrappers host retries transient failures up to 3 times for all requests — see the [double-apply warning](#data-modify-insert-update-delete) under Data Modify.

For APIs with very strict rate limits, consider using materialized views to cache results.

## Examples

For additional real-world examples with multiple tables, pagination, and advanced features, see the **[examples directory on GitHub](https://github.com/supabase/wrappers/tree/main/wasm-wrappers/fdw/openapi_fdw/examples)**. There are step-by-step walkthroughs for querying the NWS Weather API, PokéAPI, CarAPI, GitHub, and Threads.

### Basic Query

```sql
-- Create a foreign server connecting to the Weather.gov API
create server openapi_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
    base_url 'https://api.weather.gov',
    spec_url 'https://api.weather.gov/openapi.json'
  );

-- Create a schema to hold the imported foreign tables
create schema if not exists openapi;

-- Auto-import all API endpoints as foreign tables based on the OpenAPI spec
import foreign schema openapi from server openapi_server into openapi;

-- Query the stations endpoint to get weather station data
select * from openapi.stations limit 5;
```

### Nested Resources

```sql
-- Create a foreign table for a parameterized endpoint with {zone_id} path parameter
create foreign table openapi.zone_stations (
  zone_id text,
  id text,
  type text,
  attrs jsonb
) server openapi_server
options (
  endpoint '/zones/forecast/{zone_id}/stations',
  rowid_column 'id'
);

-- Query stations for Alaska zone AKZ317 - generates GET /zones/forecast/AKZ317/stations
select id, type from openapi.zone_stations where zone_id = 'AKZ317';
```

### POST-for-Read

```sql
-- Query a search API that uses POST for read operations
create foreign table openapi.search_results (
  id text,
  title text,
  score real,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/search',
  method 'POST',
  request_body '{"query": "postgresql", "limit": 25}'
);

select id, title, score from openapi.search_results;
```

### Custom Headers

```sql
create server custom_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
    base_url 'https://openapi.example.com',
    api_key 'your-key',
    user_agent 'MyApp/1.0',
    accept 'application/json',
    headers '{"X-Request-ID": "postgres-fdw", "X-Feature-Flag": "beta"}'
  );
```

### API Key Location

By default, the API key is sent as a header. Use `api_key_location` to send it as a query parameter or cookie instead:

```sql
create server query_auth_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    fdw_package_checksum 'f0d4d6e50f7c519a66363bd8bdbe1ea8086ca810ca14b43fb0ed18b64acdf6aa',
    base_url 'https://api.example.com',
    api_key 'sk-your-api-key',
    api_key_location 'query'  -- sends as ?api_key=sk-... (uses api_key_header as param name)
  );
```

### Per-request credentials (session variables)

A server's credential is normally fixed when the server is created. To vary it per request (for example, per-user OAuth tokens in a multi-tenant app) set `auth_token_setting` to the name of a Postgres session variable, then resolve that variable per query with a `SECURITY DEFINER` function:

```sql
create server per_user_api
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.1/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.1',
    fdw_package_checksum '12c902f3089e18142a1d8d35c66b9ceb85c193224229687bd929aff6b44cddde',
    base_url 'https://api.example.com',
    auth_token_setting 'app.api_token'  -- read the token from this session variable each request
  );

-- Resolve the calling user's token (e.g. from an RLS-protected table keyed to
-- auth.uid()) and pin it for the life of the transaction:
create function set_api_token() returns void
  language sql security definer set search_path = '' as $$
  select set_config('app.api_token',
    (select access_token from public.user_tokens where user_id = auth.uid()),
    true);
$$;

select set_api_token();
select * from some_foreign_table;
```

On each request the FDW reads `app.api_token` and sends it as `Authorization: Bearer <token>`. If the variable is unset or empty no token is injected, and any static credential on the server still applies. Use `auth_token_prefix` to change the `Bearer` prefix, or set it to an empty string to send the raw token.

### Response Path Extraction

For APIs that wrap data in a container object:

```sql
-- API returns: {"data": [...], "meta": {...}}
create foreign table openapi.items (
  id text,
  name text,
  attrs jsonb
)
server my_api_server
options (
  endpoint '/items',
  response_path '/data'
);
```

### Combining with Materialized Views

For frequently accessed data, use materialized views to reduce API calls:

```sql
create materialized view api_users_cache as
select * from openapi.users;

-- Query the cache
select * from api_users_cache;

-- Refresh when needed
refresh materialized view api_users_cache;
```
