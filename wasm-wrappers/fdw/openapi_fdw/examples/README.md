# Examples

Each example shows how to configure the FDW against a real API, with complete server options, table definitions, and sample queries.

## No Auth Required

| Example | API | Features |
| --- | --- | --- |
| [nws](nws/) | [Weather.gov](https://www.weather.gov/documentation/services-web-api) | GeoJSON responses, nested path extraction, custom User-Agent |
| [carapi](carapi/) | [CarAPI](https://carapi.app/) | Page-based pagination, query pushdown, auto-detected `data` wrapper |
| [pokeapi](pokeapi/) | [PokéAPI](https://pokeapi.co/) | Offset-based pagination, path params, auto-detected `results` wrapper |
| [jsonplaceholder](jsonplaceholder/) | [JSONPlaceholder](https://jsonplaceholder.typicode.com/) | Write support (INSERT/UPDATE/DELETE), body-level success checking, write debug output |
| [httpbin](httpbin/) | [httpbin.org](https://httpbin.org/) | Advanced write shapes: body envelopes, body/query rowid placement, per-op endpoints, misconfiguration gates |

## Auth Required

| Example | API | Auth | Features |
| --- | --- | --- | --- |
| [github](github/) | [GitHub REST API](https://docs.github.com/en/rest) | Bearer token | Path params, custom headers, `items` wrapper, search pushdown, Link-header pagination, session-variable auth, live writes |
| [threads](threads/) | [Meta Threads API](https://developers.facebook.com/docs/threads) | OAuth token (query param) | Cursor-based pagination, path params, query pushdown |

## Feature Coverage

| Feature | Weather.gov | CarAPI | PokéAPI | JSONPlaceholder | httpbin | GitHub | Threads |
| --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| IMPORT FOREIGN SCHEMA (`spec_url`) | ✓ | ✓ | ✓ | | | ✓ | |
| IMPORT FOREIGN SCHEMA (`spec_json`) | | | | | | | ✓ |
| YAML spec support | | | ✓ | | | | |
| Page/offset-based pagination | | ✓ | ✓ | | | ✓ | |
| Cursor-based pagination | ✓ | | | | | | ✓ |
| URL-based pagination (auto-detected) | | | ✓ | | | ✓ | |
| RFC 8288 `Link` header pagination | | | | | | ✓ | |
| Path parameter substitution | ✓ | | ✓ | | | ✓ | ✓ |
| Query parameter pushdown | ✓ | ✓ | | | | ✓ | ✓ |
| LIMIT pushdown | ✓ | ✓ | ✓ | ✓ | | ✓ | ✓ |
| GeoJSON extraction (`object_path`) | ✓ | | | | | | |
| Nested response path (`response_path`) | ✓ | | | | | | |
| Bearer token / API key auth | | | | | | ✓ | |
| Query param auth (`api_key_location`) | | | | | | | ✓ |
| Session-variable auth (`auth_token_setting`) | | | | | | ✓ | |
| Custom headers | ✓ | | | | | ✓ | |
| Type coercion (int, bool, timestamptz) | ✓ | ✓ | ✓ | ✓ | | ✓ | ✓ |
| camelCase to snake_case matching | ✓ | | | ✓ | | | |
| Single object response | ✓ | | ✓ | | | ✓ | |
| Auto-detected wrapper key | | ✓ | ✓ | | | ✓ | |
| INSERT / UPDATE / DELETE | | | | ✓ | ✓ | ✓ | |
| Body-level success checking (`success_path`) | | | | ✓ | ✓ | | |
| Body envelope (`body_root_path` / `body_wrap`) | | | | | ✓ | | |
| Rowid placement (url / body / query) | | | | ✓ | ✓ | ✓ | |
| Per-op write endpoints (`write_endpoint`) | | | | | ✓ | | |
| Debug mode | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | |
| `attrs` full JSON column | ✓ | ✓ | ✓ | ✓ | | ✓ | ✓ |
