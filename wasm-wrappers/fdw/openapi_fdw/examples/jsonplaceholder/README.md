# JSONPlaceholder Example

Full CRUD against [JSONPlaceholder](https://jsonplaceholder.typicode.com/), a free fake REST API — no authentication required. This example demonstrates the FDW's write support (`INSERT` / `UPDATE` / `DELETE`), body-level success checking, and the write-path debug output, alongside ordinary reads.

JSONPlaceholder accepts writes and echoes them back but never persists them, which makes it a safe playground: you can run every statement below repeatedly without mutating anything real.

## Server Configuration

```sql
create server jsonplaceholder
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.3.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.3.0',
    base_url 'https://jsonplaceholder.typicode.com',
    user_agent 'openapi-fdw-example/0.3.0'
  );
```

---

## 1. Reads

The `posts` table maps `GET /posts` (list) and `GET /posts/{id}` (single lookup via `rowid_column`):

```sql
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
```

```sql
SELECT id, user_id, title FROM posts LIMIT 3;
```

| id | user_id | title |
| --- | --- | --- |
| 1 | 1 | sunt aut facere repellat provident occaecati excepturi optio reprehenderit |
| 2 | 1 | qui est esse |
| 3 | 1 | ea molestias quasi exercitationem repellat qui ipsa sit aut |

The API returns `userId` in camelCase; the FDW matches it to the `user_id` column automatically. A `WHERE id = ...` filter becomes a single-resource fetch:

```sql
-- GET /posts/1
SELECT title FROM posts WHERE id = 1;
```

## 2. INSERT

`insert_method 'POST'` maps SQL INSERT to `POST /posts`. Columns become a typed JSON body — numbers stay JSON numbers, and null columns are omitted:

```sql
INSERT INTO posts (user_id, title, body)
VALUES (1, 'hello from SQL', 'written through the FDW');
-- POST /posts   body: {"user_id":1,"title":"hello from SQL","body":"written through the FDW"}
```

> **Field naming on writes:** the FDW sends column names as-is in write bodies. JSONPlaceholder doesn't care, but if your API expects camelCase on write (e.g. `userId`), declare the column quoted — `"userId" bigint` — so the body carries the exact key.

## 3. UPDATE and DELETE

With `rowid_column 'id'` and the default `rowid_location 'url'`, the row id is appended to the URL path and excluded from the body:

```sql
UPDATE posts SET title = 'updated from SQL' WHERE id = 1;
-- PATCH /posts/1   body: {"title":"updated from SQL"}

DELETE FROM posts WHERE id = 1;
-- DELETE /posts/1
```

Each affected row issues one immediate HTTP request. Writes are not transactional — see the [Data Modify docs](https://fdw.dev/catalog/openapi/#data-modify-insert-update-delete) for the full caveats.

## 4. Body-Level Success Checking

Some APIs answer HTTP 2xx and signal failure inside the response body. `success_path` makes the FDW verify the body of every write response. JSONPlaceholder always answers `POST /posts` with `{"id": 101, ...}`, so we can prove the check runs against the parsed body:

```sql
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

INSERT INTO posts_checked (title, body) VALUES ('t', 'b');
-- HTTP 201 with {"id":101} -> /id matches '101' -> success
```

The sibling table `posts_wrong_check` expects `success_value '999'`, so its INSERTs always abort with a statement error even though the API answers 201 — demonstrating that a 2xx status alone never passes when the body check fails:

```sql
INSERT INTO posts_wrong_check (title, body) VALUES ('t', 'b');
-- ERROR: write to API endpoint (/posts) returned HTTP 201 but success_path '/id'
--        did not match expected value '999' (got 101)
```

## 5. Writes Are Opt-In

`posts_readonly` points at the same endpoint but omits `writable 'true'`. DML fails in `begin_modify`, before any HTTP request:

```sql
INSERT INTO posts_readonly (title) VALUES ('t');
-- ERROR: foreign table is read-only. Set the writable 'true' table option to enable data modify.
```

## 6. Debug Mode

`posts_debug` uses the debug server. Write statements log their verb, URL, and response status:

```sql
INSERT INTO posts_debug (title, body) VALUES ('t', 'b');
```

```log
INFO:  [openapi_fdw] HTTP POST https://jsonplaceholder.typicode.com/posts -> 201 (68 bytes)
```

```sql
UPDATE posts_debug SET title = 'renamed' WHERE id = 2;
```

```log
INFO:  [openapi_fdw] HTTP GET https://jsonplaceholder.typicode.com/posts/2 -> 200 (292 bytes)
INFO:  [openapi_fdw] HTTP PATCH https://jsonplaceholder.typicode.com/posts/2 -> 200 (166 bytes)
```

The GET is the scan phase (PostgreSQL fetches matching rows before modifying them); the PATCH is the write itself.
