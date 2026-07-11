-- OpenAPI FDW example: JSONPlaceholder
-- A free fake REST API for testing — no authentication required.
-- Writes are accepted and echoed back but never persisted, which makes this a
-- safe playground for the FDW's INSERT / UPDATE / DELETE support.
-- See: https://jsonplaceholder.typicode.com/
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
-- Server 1: jsonplaceholder — Main API server
-- No authentication required
-- ============================================================
create server jsonplaceholder
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.3.0',
    base_url 'https://jsonplaceholder.typicode.com',
    user_agent 'openapi-fdw-example/0.3.0'
  );

-- ============================================================
-- Server 2: jsonplaceholder_debug — Same API with debug output
-- Emits HTTP request details (including write verbs) as INFO messages
-- ============================================================
create server jsonplaceholder_debug
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.3.0',
    base_url 'https://jsonplaceholder.typicode.com',
    user_agent 'openapi-fdw-example/0.3.0',
    debug 'true'
  );

-- ============================================================
-- Table 1: posts
-- Full CRUD — GET/POST /posts, GET/PATCH/DELETE /posts/{id}
-- Features: write support (INSERT/UPDATE/DELETE), rowid in URL path,
--           single-resource reads, camelCase matching (userId -> user_id)
-- ============================================================
create foreign table posts (
  id bigint,
  user_id bigint,
  title text,
  body text,
  attrs jsonb
)
  server jsonplaceholder
  options (
    endpoint '/posts',
    rowid_column 'id',
    writable 'true',
    insert_method 'POST',
    update_method 'PATCH',
    delete_method 'DELETE'
  );

-- ============================================================
-- Table 2: users
-- Read-only user directory — GET /users
-- Features: plain list response, no write options (writes rejected)
-- ============================================================
create foreign table users (
  id bigint,
  name text,
  username text,
  email text,
  phone text,
  website text,
  address jsonb,
  company jsonb,
  attrs jsonb
)
  server jsonplaceholder
  options (
    endpoint '/users',
    rowid_column 'id'
  );

-- ============================================================
-- Table 3: posts_checked
-- INSERT with body-level success checking — POST /posts
-- JSONPlaceholder always answers POST /posts with {"id": 101, ...},
-- so success_path '/id' + success_value '101' verifies the FDW parses
-- the response body, not just the HTTP status line.
-- ============================================================
create foreign table posts_checked (
  id bigint,
  title text,
  body text
)
  server jsonplaceholder
  options (
    endpoint '/posts',
    rowid_column 'id',
    writable 'true',
    insert_method 'POST',
    success_path '/id',
    success_value '101'
  );

-- ============================================================
-- Table 4: posts_wrong_check
-- Deliberate misconfiguration used by the test harness: the expected
-- success_value never matches, so every INSERT must abort with a
-- statement error even though the API answers HTTP 201.
-- ============================================================
create foreign table posts_wrong_check (
  id bigint,
  title text,
  body text
)
  server jsonplaceholder
  options (
    endpoint '/posts',
    rowid_column 'id',
    writable 'true',
    insert_method 'POST',
    success_path '/id',
    success_value '999'
  );

-- ============================================================
-- Table 5: posts_readonly
-- Same endpoint without writable 'true' — DML errors in begin_modify,
-- before any HTTP request is made.
-- ============================================================
create foreign table posts_readonly (
  id bigint,
  title text,
  body text
)
  server jsonplaceholder
  options (
    endpoint '/posts',
    rowid_column 'id',
    insert_method 'POST'
  );

-- ============================================================
-- Table 6: posts_debug
-- Writable table on the debug server — INSERT/UPDATE emit
-- "HTTP POST ..." / "HTTP PATCH ..." INFO messages
-- ============================================================
create foreign table posts_debug (
  id bigint,
  user_id bigint,
  title text,
  body text,
  attrs jsonb
)
  server jsonplaceholder_debug
  options (
    endpoint '/posts',
    rowid_column 'id',
    writable 'true',
    insert_method 'POST',
    update_method 'PATCH',
    delete_method 'DELETE'
  );
