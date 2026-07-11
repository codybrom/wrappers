-- OpenAPI FDW example: httpbin.org
-- httpbin echoes every request back in the response body (method, url, args,
-- parsed JSON body), which makes writes self-verifying: pointing success_path
-- at the echoed request proves the FDW built exactly the request shape the
-- table options describe. No authentication required, nothing is persisted.
-- See: https://httpbin.org/
-- Note: fdw_package_url uses file:// for local Docker testing. In production, use the
-- GitHub release URL: https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.3.0/openapi_fdw.wasm

-- Create supabase_admin role if it doesn't exist (required by wrappers extension)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'supabase_admin') THEN
    CREATE ROLE supabase_admin WITH SUPERUSER CREATEDB CREATEROLE LOGIN PASSWORD 'postgres';
  END IF;
END
$$;

create schema if not exists extensions;
create extension if not exists wrappers with schema extensions;

set search_path to public, extensions;

create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;

-- ============================================================
-- Server 1: httpbin — Echo API server
-- ============================================================
create server httpbin
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.3.0',
    base_url 'https://httpbin.org',
    user_agent 'openapi-fdw-example/0.3.0'
  );

-- ============================================================
-- Server 2: httpbin_debug — Same API with debug output
-- ============================================================
create server httpbin_debug
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.3.0',
    base_url 'https://httpbin.org',
    user_agent 'openapi-fdw-example/0.3.0',
    debug 'true'
  );

-- ============================================================
-- Table 1: echo_update
-- Envelope-style UPDATE, the "enterprise CRM" shape:
--   PUT to the collection URL, rowid inside the record,
--   body wrapped as {"data": [ {...} ]}, success verified in the body.
-- httpbin echoes the parsed request body under /json, so
-- success_path '/json/data/0/stage' proves the envelope, the array
-- wrap, and the body-placed rowid all arrived exactly as configured
-- (the example UPDATE sets stage = 'Qualification').
-- ============================================================
create foreign table echo_update (
  id text,
  stage text,
  amount numeric,
  attrs jsonb
)
  server httpbin
  options (
    endpoint '/anything/records',
    rowid_column 'id',
    writable 'true',
    update_method 'PUT',
    rowid_location 'body',
    body_root_path '/data',
    body_wrap 'array',
    success_path '/json/data/0/stage',
    success_value 'Qualification'
  );

-- ============================================================
-- Table 2: echo_insert
-- write_endpoint routing: reads hit /anything/read-side, writes hit
-- /anything/write-side. httpbin echoes the request URL under /url,
-- so success_path '/url' proves INSERT was routed to the write
-- endpoint and not the read endpoint.
-- ============================================================
create foreign table echo_insert (
  id text,
  name text,
  attrs jsonb
)
  server httpbin
  options (
    endpoint '/anything/read-side',
    rowid_column 'id',
    writable 'true',
    insert_method 'POST',
    write_endpoint '/anything/write-side',
    success_path '/url',
    success_value 'https://httpbin.org/anything/write-side'
  );

-- ============================================================
-- Table 3: echo_delete
-- Query-parameter rowid DELETE: DELETE /anything/records?ids=<rowid>.
-- On the debug server so the emitted URL (containing ids=...) is
-- visible as an INFO message. success_path '/method' verifies the
-- echoed verb.
-- ============================================================
create foreign table echo_delete (
  id text,
  name text,
  attrs jsonb
)
  server httpbin_debug
  options (
    endpoint '/anything/records',
    rowid_column 'id',
    writable 'true',
    delete_method 'DELETE',
    delete_rowid_location 'query',
    rowid_param 'ids',
    success_path '/method',
    success_value 'DELETE'
  );

-- ============================================================
-- Table 4: echo_missing_check
-- Deliberate misconfiguration used by the test harness: an envelope
-- body (body_root_path) without success_path. The FDW rejects this in
-- begin_modify, before any HTTP request — an envelope API may signal
-- per-record failure inside a 2xx body, which a status check alone
-- cannot detect.
-- ============================================================
create foreign table echo_missing_check (
  id text,
  name text
)
  server httpbin
  options (
    endpoint '/anything/records',
    rowid_column 'id',
    writable 'true',
    update_method 'PUT',
    body_root_path '/data'
  );
