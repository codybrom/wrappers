# httpbin Example

Advanced write shapes against [httpbin.org](https://httpbin.org/), a request-echo service — no authentication required, nothing persisted. httpbin answers every request with a JSON description of that request (`method`, `url`, `args`, and the parsed body under `json`), which makes writes **self-verifying**: by pointing `success_path` at the echoed request, the statement only succeeds if the FDW built exactly the request shape the table options describe.

Where the [jsonplaceholder example](../jsonplaceholder/) covers the common REST write path, this one exercises the exotic shapes: body envelopes, body/query rowid placement, per-operation endpoints, and the misconfiguration gates.

## Server Configuration

```sql
create server httpbin
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.3.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.3.0',
    base_url 'https://httpbin.org',
    user_agent 'openapi-fdw-example/0.3.0'
  );
```

---

## 1. Envelope UPDATE (the "enterprise CRM" shape)

Many enterprise APIs want `PUT` to the collection URL with the record id *inside* an array envelope: `{"data": [ {...} ]}`. That's `rowid_location 'body'` + `body_root_path '/data'` + `body_wrap 'array'`:

```sql
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

UPDATE echo_update SET stage = 'Qualification', amount = 8000 WHERE id = 'rec-1';
-- PUT /anything/records
-- body: {"data":[{"amount":8000.0,"id":"rec-1","stage":"Qualification"}]}
```

httpbin echoes the parsed body under `/json`, and `success_path '/json/data/0/stage'` reads back into that echo — so the UPDATE only reports success if the envelope, the array wrap, and the body-placed rowid all arrived intact. This is the same `success_path` mechanism you'd point at a real API's per-record outcome code (e.g. `/data/0/code` with `success_value 'SUCCESS'`).

> The scan phase (`WHERE id = 'rec-1'`) issues `GET /anything/records/rec-1` first; httpbin happily echoes that too, and the FDW injects the id back into the row so the filter matches.

## 2. Separate Read and Write Endpoints

`write_endpoint` routes writes to a different path than reads. The echoed `/url` proves where the INSERT actually landed:

```sql
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

INSERT INTO echo_insert (name) VALUES ('routed');
-- POST /anything/write-side (NOT /anything/read-side)
-- success only if httpbin echoes url = https://httpbin.org/anything/write-side
```

Per-operation variants (`insert_endpoint` / `update_endpoint` / `delete_endpoint`) follow the same pattern, falling back to `write_endpoint`, then `endpoint`.

## 3. Query-Parameter Rowid DELETE

Some APIs delete via query string (`DELETE /records?ids=<id>`) rather than a path segment. That's `delete_rowid_location 'query'` + `rowid_param`:

```sql
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

DELETE FROM echo_delete WHERE id = 'del-1';
-- DELETE /anything/records?ids=del-1
```

This table lives on the debug server, so the INFO output shows the exact URL:

```log
INFO:  [openapi_fdw] HTTP GET https://httpbin.org/anything/records/del-1 -> 200 (…)
INFO:  [openapi_fdw] HTTP DELETE https://httpbin.org/anything/records?ids=del-1 -> 200 (…)
```

`update_rowid_location` and `delete_rowid_location` can differ on one table — that's how you express an API that PUTs to `/records/{id}` but deletes via `?ids=`.

## 4. Misconfiguration Fails Loud and Early

All write validation runs in `begin_modify`, before any HTTP request. Two examples:

An operation whose `*_method` is unset errors instead of silently doing nothing:

```sql
INSERT INTO echo_update (id, stage) VALUES ('x', 'y');
-- ERROR: INSERT is not enabled for this foreign table.
--        Set the insert_method table option to enable it.
```

An envelope table without `success_path` is rejected outright (`echo_missing_check` in `init.sql` is set up this way for the test harness) — an envelope API may signal per-record failure inside a 2xx body, which a status check alone cannot detect:

```sql
UPDATE echo_missing_check SET name = 'x' WHERE id = 'm-1';
-- ERROR: success_path is required for this table: the API may signal
--        per-record failure inside a 2xx response body. …
```
