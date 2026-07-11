-- OpenAPI FDW example: GitHub API
-- Requires a GitHub personal access token. Replace 'placeholder' in the server definitions
-- below with your token.
-- See: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens
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
-- Server 1: github — Main GitHub API server
-- Bearer token auth via Authorization header (default behavior)
-- Custom headers for GitHub API versioning
-- ============================================================
create server github
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.3.0',
    base_url 'https://api.github.com',
    api_key 'placeholder',
    user_agent 'openapi-fdw-example/0.3.0',
    accept 'application/vnd.github+json',
    headers '{"X-GitHub-Api-Version": "2022-11-28"}',
    page_size '30',
    page_size_param 'per_page'
  );

-- ============================================================
-- Server 2: github_debug — Same API with debug output
-- Emits HTTP request details as INFO messages
-- ============================================================
create server github_debug
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.3.0',
    base_url 'https://api.github.com',
    api_key 'placeholder',
    user_agent 'openapi-fdw-example/0.3.0',
    accept 'application/vnd.github+json',
    headers '{"X-GitHub-Api-Version": "2022-11-28"}',
    page_size '30',
    page_size_param 'per_page',
    debug 'true'
  );

-- ============================================================
-- Server 3: github_import — With spec_url for IMPORT FOREIGN SCHEMA
-- Note: The GitHub spec is large (~15 MB). The FDW's default
-- max_response_bytes (50 MiB) can handle it, but the initial
-- IMPORT may take a few seconds.
-- ============================================================
create server github_import
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.3.0',
    base_url 'https://api.github.com',
    api_key 'placeholder',
    user_agent 'openapi-fdw-example/0.3.0',
    accept 'application/vnd.github+json',
    headers '{"X-GitHub-Api-Version": "2022-11-28"}',
    page_size '30',
    page_size_param 'per_page',
    spec_url 'https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.json'
  );

-- ============================================================
-- Server 4: github_session — Per-request credentials (0.2.1+)
-- No static api_key: the bearer token is read from the Postgres
-- session variable 'app.github_token' on every request, so each
-- session (or user, via a SECURITY DEFINER helper) supplies its own.
-- ============================================================
create server github_session
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.3.0',
    base_url 'https://api.github.com',
    user_agent 'openapi-fdw-example/0.3.0',
    accept 'application/vnd.github+json',
    headers '{"X-GitHub-Api-Version": "2022-11-28"}',
    page_size '30',
    page_size_param 'per_page',
    auth_token_setting 'app.github_token'
  );

-- ============================================================
-- Table 1: my_profile
-- Authenticated user's profile — GET /user
-- Features: single object response, bearer token auth, custom headers
-- ============================================================
create foreign table my_profile (
  login text,
  id bigint,
  name text,
  email text,
  bio text,
  public_repos integer,
  public_gists integer,
  followers integer,
  following integer,
  created_at timestamptz,
  avatar_url text,
  company text,
  location text,
  blog text,
  attrs jsonb
)
  server github
  options (
    endpoint '/user'
  );

-- ============================================================
-- Table 2: my_repos
-- Authenticated user's repositories — GET /user/repos
-- Features: page-based pagination (auto-detected), query pushdown (type, sort)
-- ============================================================
create foreign table my_repos (
  id bigint,
  name text,
  full_name text,
  description text,
  private boolean,
  fork boolean,
  language text,
  stargazers_count integer,
  forks_count integer,
  open_issues_count integer,
  created_at timestamptz,
  updated_at timestamptz,
  pushed_at timestamptz,
  html_url text,
  default_branch text,
  archived boolean,
  type text,
  sort text,
  attrs jsonb
)
  server github
  options (
    endpoint '/user/repos',
    rowid_column 'id'
  );

-- ============================================================
-- Table 3: repo_detail
-- Full repository metadata — GET /repos/{owner}/{repo}
-- Features: two path parameters, single object response
-- ============================================================
create foreign table repo_detail (
  id bigint,
  name text,
  full_name text,
  description text,
  private boolean,
  stargazers_count integer,
  forks_count integer,
  open_issues_count integer,
  watchers_count integer,
  language text,
  default_branch text,
  created_at timestamptz,
  updated_at timestamptz,
  license jsonb,
  topics jsonb,
  owner text,
  repo text,
  attrs jsonb
)
  server github
  options (
    endpoint '/repos/{owner}/{repo}'
  );

-- ============================================================
-- Table 4: repo_issues
-- Repository issues — GET /repos/{owner}/{repo}/issues
-- Features: two path parameters, page-based pagination,
--           query pushdown (state), timestamptz coercion
-- ============================================================
create foreign table repo_issues (
  id bigint,
  number integer,
  title text,
  state text,
  body text,
  created_at timestamptz,
  updated_at timestamptz,
  closed_at timestamptz,
  comments integer,
  user_col jsonb,
  labels jsonb,
  html_url text,
  owner text,
  repo text,
  attrs jsonb
)
  server github
  options (
    endpoint '/repos/{owner}/{repo}/issues',
    rowid_column 'id'
  );

-- ============================================================
-- Table 5: repo_pulls
-- Repository pull requests — GET /repos/{owner}/{repo}/pulls
-- Features: two path parameters, page-based pagination,
--           query pushdown (state), boolean + timestamptz coercion
-- ============================================================
create foreign table repo_pulls (
  id bigint,
  number integer,
  title text,
  state text,
  draft boolean,
  created_at timestamptz,
  updated_at timestamptz,
  merged_at timestamptz,
  user_col jsonb,
  head jsonb,
  base jsonb,
  html_url text,
  owner text,
  repo text,
  attrs jsonb
)
  server github
  options (
    endpoint '/repos/{owner}/{repo}/pulls',
    rowid_column 'id'
  );

-- ============================================================
-- Table 6: repo_releases
-- Repository releases — GET /repos/{owner}/{repo}/releases
-- Features: two path parameters, page-based pagination
-- ============================================================
create foreign table repo_releases (
  id bigint,
  tag_name text,
  name text,
  body text,
  draft boolean,
  prerelease boolean,
  created_at timestamptz,
  published_at timestamptz,
  author jsonb,
  html_url text,
  owner text,
  repo text,
  attrs jsonb
)
  server github
  options (
    endpoint '/repos/{owner}/{repo}/releases',
    rowid_column 'id'
  );

-- ============================================================
-- Table 7: search_repos
-- Search repositories — GET /search/repositories
-- Features: query pushdown (q), auto-detected "items" wrapper key
-- ============================================================
create foreign table search_repos (
  id bigint,
  name text,
  full_name text,
  description text,
  stargazers_count integer,
  forks_count integer,
  language text,
  open_issues_count integer,
  created_at timestamptz,
  html_url text,
  topics jsonb,
  license jsonb,
  q text,
  attrs jsonb
)
  server github
  options (
    endpoint '/search/repositories',
    rowid_column 'id'
  );

-- ============================================================
-- Table 8: search_repos_debug
-- Same as search_repos but on the debug server
-- Features: debug output in INFO messages
-- ============================================================
create foreign table search_repos_debug (
  id bigint,
  name text,
  full_name text,
  stargazers_count integer,
  q text,
  attrs jsonb
)
  server github_debug
  options (
    endpoint '/search/repositories',
    rowid_column 'id'
  );

-- ============================================================
-- Table 9: session_profile
-- Same as my_profile but on the session-variable auth server (0.2.1+).
-- Queries fail with HTTP 401 until the session supplies a token:
--   SELECT set_config('app.github_token', '<token>', false);
-- ============================================================
create foreign table session_profile (
  login text,
  id bigint,
  name text,
  attrs jsonb
)
  server github_session
  options (
    endpoint '/user'
  );

-- ============================================================
-- Table 10: issue_editor
-- Live write support (0.3.0+): UPDATE an issue via PATCH.
-- UPDATE rows carry only the SET columns plus the rowid, so the only
-- path parameter a write endpoint can substitute from the row is the
-- rowid itself ({issue_number} here). Owner and repo must be static in
-- the endpoint — replace 'youruser/sandbox' with a repo you own (the
-- test harness rewrites it from GITHUB_WRITE_OWNER/GITHUB_WRITE_REPO).
-- Only the SET columns (title, body, state) travel in the JSON body.
-- ============================================================
create foreign table issue_editor (
  issue_number bigint,
  title text,
  body text,
  state text,
  attrs jsonb
)
  server github
  options (
    endpoint '/repos/youruser/sandbox/issues/{issue_number}',
    rowid_column 'issue_number',
    writable 'true',
    update_method 'PATCH'
  );

-- ============================================================
-- Table 11: issue_comments
-- Live write support (0.3.0+): INSERT a comment via POST.
-- owner/repo/issue_number are consumed as path parameters and excluded
-- from the body, which carries only {"body": "..."}.
-- ============================================================
create foreign table issue_comments (
  id bigint,
  owner text,
  repo text,
  issue_number bigint,
  body text,
  created_at timestamptz,
  attrs jsonb
)
  server github
  options (
    endpoint '/repos/{owner}/{repo}/issues/{issue_number}/comments',
    rowid_column 'id',
    writable 'true',
    insert_method 'POST'
  );

-- ============================================================
-- Table 12: new_issues
-- Live write support (0.3.0+): CREATE an issue via POST.
-- The collection endpoint (no {issue_number}) is the create URL.
-- RETURNING is unsupported, so the created issue's number is not
-- visible to SQL — re-select repo_issues by a unique title to find it
-- (the create-then-reference pattern from the Data Modify docs).
-- ============================================================
create foreign table new_issues (
  id bigint,
  owner text,
  repo text,
  title text,
  body text,
  attrs jsonb
)
  server github
  options (
    endpoint '/repos/{owner}/{repo}/issues',
    rowid_column 'id',
    writable 'true',
    insert_method 'POST'
  );
