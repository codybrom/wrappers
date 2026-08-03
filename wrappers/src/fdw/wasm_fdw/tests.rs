#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn wasm_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER wasm_wrapper
                     HANDLER wasm_fdw_handler VALIDATOR wasm_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();

            // Snowflake FDW test
            c.update(
                r#"CREATE SERVER snowflake_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/snowflake_fdw.wasm',
                       fdw_package_name 'supabase:snowflake-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/snowflake/{}',
                       account_identifier 'TEST-ABC1234',
                       user 'TESTUSER',
                       timeout_secs '30',
                       public_key_fingerprint 'WVggEofeFX0jwCdImbOfGFyOggF2o8DT7S1uTLZhCJQ=',
                       private_key E'-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCOQv0mMe1yElR8\nhiQgduWu7OrMR3iW8xbu+i04LkMDKB/JtdMl1Mq3Gs/XWUB07BOIytcK4L7k1Z/3\nnkTGvib85S5+VLsngBzWltltvCOfnM2uCLWHmVpfMR1WVFrCU1r7NP92U2APpwvN\ncJps39VNyQWTm+TvQN25enqAC2uR6xdvItb86dn4Tab3KcAXj0f0qHompa8SSwBO\nGZFpgAjt7QFPWTniNQyBU7wXTntwV+a2WC/bf1i9MRAf8bqSr+yqijLHVGrmBPSb\njmvo/8bj69DzeY2//gZXAMe1m6cEjXPm2MY/POd66Xg1YZDzYsv95GZ54kJ+EgEF\n9rjsi/J5AgMBAAECggEAAsG9kh3pkgpU5Mzcqlxjew5QRoEkDxjK2vqyIaKT3d3L\nL+d8HgGPpBi66ltqalmgz0fO/wD38gtJvEyu3IMW0lPGoOAXeF59MJNfx0acEh3B\nxpuYmPYZ0DptbRzZXWasHq4aPTrEY8lC60pBU9bKlWVN3FxrBU/mfA+pjA2smflC\n54brJTsSb1/1xAExxsvB2Leb6VcNWKRCaN6Z4gdWd1Qofi080LVWxE3MXhxHpTMj\nVf3KHhKI5DHJXlZPU/w4KXOlp99UH0vnx3EJzD07kI+nR2k7tfh8PxwFzn8g6hEC\nK9Q+HmzUUTxFh1M8eXi7IMRjLRJThVSl/Kbqr6cpeQKBgQC+410aRuHUteQIgJBQ\nbceAOjEh5MetByIEFdXLEgYspl1rSjN/JoIMUguyJ8KZGj5G3NaRaJklNOofYekI\nhIL3SBWZ9U58MJVMnSUVeBdazCu0k9HnOfOFrFJIDoRPBfjnP0UJmC+9ggoVayOX\nVW5psrxGiQXWiG7mho1bshSlNwKBgQC+yX1wFSF8gGsAb41iw60K9W+O5PLVWxAu\nYst8CQTY64RVctvAypWtNTb4nmIBe9aX9k5loe+uv8Zse/t1hgCVGR7n70EyT9Y+\nGNrGqYtVjtZQ+L+dAivrlUKsTDGqzWldUTg7gpOqkFaQbV0O11ytyKJKYXCpTrL2\nwib6V4X9zwKBgQCllTJAxfXFfxZUbblBm0iwKUpPXVX7+LEAHDS9F2B1wMZOeCod\nhLjQmSb+HlFGX6Zf79bMgZA+3xyrplHviorUmBns2AaB4d7Qe4wciHSx1WOgG427\n5uAgNy+Uw8rvhX24koB/Zx0aZT/7/lj8QCYr19hL0zZWNzkEDPl37gzMlwKBgQC3\noOsww8XVNSzH4JZupvOYhp53JHltTRaH7uL3YR7fQd++9qv4JYRmj793D8o4r17e\nKF1QiMpOoZpzs+lVNkK9Ps52YduYdys33WhEqc7H7JDuolya3Ao11xWzDCsJwGdX\nP+MltAo4sm/+1qQosrQrN96sRJjQ/ERYKIqnjTIUFQKBgQC6xaC5SB1UMepFbTVa\n2tuRuYwcrU/NxjeW85SAyeyv2dMg7S+Ot8mnA1Js2O0NHlczUZgRZalYkCuSUE78\nb6rIbezIW2azrw3tqAAPLsB+rhXvaUpICoybu+j6aCiqtZYsDx7zIj/FTD27Tpwx\nYfLx1Erqd3vM/LzOIaIOqlfETw==\n-----END PRIVATE KEY-----'
                       )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE snowflake_mytable (
                      id bigint,
                      name text,
                      num numeric,
                      dt date,
                      ts timestamp
                  )
                  SERVER snowflake_server
                  OPTIONS (
                    table 'mydatabase.public.mytable',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM snowflake_mytable WHERE id = 42", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["foo"]);

            // Paddle FDW test
            c.update(
                r#"CREATE SERVER paddle_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/paddle_fdw.wasm',
                       fdw_package_name 'supabase:paddle-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/paddle',
                       api_key '1234567890'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS paddle"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA paddle FROM SERVER paddle_server INTO paddle"#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM paddle.customers WHERE id = 'ctm_01hymwgpkx639a6mkvg99563sp'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("email").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["test@test.com"]);

            // Notion FDW test
            c.update(
                r#"CREATE SERVER notion_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/notion_fdw.wasm',
                       fdw_package_name 'supabase:notion-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/notion',
                       api_key '1234567890'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE notion_pages (
                    id text,
                    url text,
                    created_time timestamp,
                    last_edited_time timestamp,
                    archived boolean,
                    attrs jsonb
                  )
                  SERVER notion_server
                  OPTIONS (
                    object 'page'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM notion_pages WHERE id = '5a67c86f-d0da-4d0a-9dd7-f4cf164e6247'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("url").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec!["https://www.notion.so/test-page3-5a67c86fd0da4d0a9dd7f4cf164e6247"]
            );

            // Calendly FDW test
            c.update(
                r#"CREATE SERVER calendly_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/calendly_fdw.wasm',
                       fdw_package_name 'supabase:calendly-fdw',
                       fdw_package_version '>=0.1.0',
                       organization 'https://api.calendly.com/organizations/xxx',
                       api_url 'http://localhost:8096/calendly',
                       api_key '1234567890'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE calendly_event_types (
                    uri text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                  )
                  SERVER calendly_server
                  OPTIONS (
                    object 'event_types'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM calendly_event_types", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("uri").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec!["https://api.calendly.com/event_types/158ecbf6-79bb-4205-a5fc-a7fefa5883a2"]
            );

            // Cal.com FDW test
            c.update(
                r#"CREATE SERVER cal_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/cal_fdw.wasm',
                       fdw_package_name 'supabase:cal-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/cal',
                       api_key '1234567890'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE cal_my_profile (
                    id bigint,
                    username text,
                    email text,
                    attrs jsonb
                  )
                  SERVER cal_server
                  OPTIONS (
                    object 'my_profile'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM cal_my_profile", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![1234567]);

            // Cloudflare D1 FDW test
            c.update(
                r#"CREATE SERVER cfd1_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/cfd1_fdw.wasm',
                       fdw_package_name 'supabase:cfd1-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/cfd1',
                       account_id 'aaa',
                       database_id 'bbb',
                       api_token 'ccc'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE cfd1_table (
                    id bigint,
                    name text,
                    _attrs jsonb
                  )
                  SERVER cfd1_server
                  OPTIONS (
                    table 'test_table'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM cfd1_table order by id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec![42, 123]);

            // Clerk FDW test
            c.update(
                r#"CREATE SERVER clerk_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/clerk_fdw.wasm',
                       fdw_package_name 'supabase:clerk-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/clerk',
                       api_key 'ccc'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE clerk_table (
                    id text,
                    external_id text,
                    username text,
                    first_name text,
                    last_name text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                  )
                  SERVER clerk_server
                  OPTIONS (
                    object 'users'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM clerk_table", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["user_2rvWkk90azWI2o3PH4LDuCMDPPh"]);

            // Orb FDW test
            c.update(
                r#"CREATE SERVER orb_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/orb_fdw.wasm',
                       fdw_package_name 'supabase:orb-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/orb',
                       api_key 'ccc'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE orb_table (
                    id text,
                    name text,
                    email text,
                    created_at timestamp,
                    auto_collection boolean,
                    attrs jsonb
                  )
                  SERVER orb_server
                  OPTIONS (
                    object 'customers'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT * FROM orb_table", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["XimGiw3pnsgusvc3"]);

            // HubSpot FDW test
            c.update(
                r#"CREATE SERVER hubspot_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/hubspot_fdw.wasm',
                       fdw_package_name 'supabase:hubspot-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/hubspot',
                       api_key 'ccc'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE hubspot_table (
                    id text,
                    email text,
                    firstname text,
                    lastname text,
                    user_id text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                  )
                  SERVER hubspot_server
                  OPTIONS (
                    object 'objects/contacts'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT id, user_id FROM hubspot_table", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("user_id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["8527", "8528"]);

            // Gravatar FDW test
            c.update(
                r#"CREATE SERVER gravatar_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'https://github.com/Automattic/gravatar-wasm-fdw/releases/download/v0.2.0/gravatar_fdw.wasm',
                       fdw_package_name 'automattic:gravatar-fdw',
                       fdw_package_version '0.2.0',
                       fdw_package_checksum '5273ae07e66bc2f1bb5a23d7b9e0342463971691e587bbd6f9466814a8bac11c',
                       api_url 'http://localhost:8096/gravatar',
                       api_key 'test'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS gravatar"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA gravatar FROM SERVER gravatar_server INTO gravatar"#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT * FROM gravatar.profiles where email = 'email@example.com'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("display_name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["Test"]);

            // Shopify FDW test
            c.update(
                r#"CREATE SERVER shopify_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/shopify_fdw.wasm',
                       fdw_package_name 'supabase:shopify-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/shopify',
                       shop 'test',
                       access_token 'ccc'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(r#"CREATE SCHEMA IF NOT EXISTS shopify"#, None, &[])
                .unwrap();
            c.update(
                r#"IMPORT FOREIGN SCHEMA shopify FROM SERVER shopify_server INTO shopify"#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT id FROM shopify.products order by id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("id").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    "gid://shopify/Product/9975063609658",
                    "gid://shopify/Product/9975063904570"
                ]
            );

            // Infura FDW test
            c.update(
                r#"CREATE SERVER infura_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/infura_fdw.wasm',
                       fdw_package_name 'supabase:infura-fdw',
                       fdw_package_version '>=0.1.0',
                       api_url 'http://localhost:8096/infura',
                       api_key 'test_api_key',
                       network 'mainnet'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE infura_blocks (
                    number numeric,
                    hash text,
                    parent_hash text,
                    timestamp timestamp,
                    miner text,
                    gas_used numeric,
                    gas_limit numeric,
                    transaction_count bigint,
                    base_fee_per_gas numeric,
                    attrs jsonb
                  )
                  SERVER infura_server
                  OPTIONS (
                    resource 'blocks'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT hash FROM infura_blocks", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("hash").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec!["0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"]
            );

            // OpenAPI FDW test
            c.update(
                r#"CREATE SERVER openapi_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/openapi_fdw.wasm',
                       fdw_package_name 'supabase:openapi-fdw',
                       fdw_package_version '>=0.1.0',
                       base_url 'http://localhost:8096/openapi',
                       api_key 'test_key'
                     )"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_users (
                    id text,
                    name text,
                    email text,
                    created_at timestamptz,
                    active boolean,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/users',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            // Test list query
            let results = c
                .select("SELECT id, name, email FROM openapi_users", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("email").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["john@example.com", "jane@example.com"]);

            // Test ID pushdown query
            let results = c
                .select(
                    "SELECT name FROM openapi_users WHERE id = 'user-123'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["John Doe"]);

            // Test path parameter substitution: /users/{user_id}/posts
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_user_posts (
                    id text,
                    user_id text,
                    title text,
                    content text,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/users/{user_id}/posts',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT user_id, title FROM openapi_user_posts WHERE user_id = 'user-123'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("title").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["First Post", "Second Post"]);

            // Test multiple path parameters: /projects/{org}/{repo}/issues
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_issues (
                    id bigint,
                    org text,
                    repo text,
                    title text,
                    state text,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/projects/{org}/{repo}/issues',
                    response_path '/items',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT org, repo, title FROM openapi_issues WHERE org = 'supabase' AND repo = 'wrappers'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("org").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["supabase", "supabase"]);

            // Test GeoJSON FeatureCollection with object_path
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_locations (
                    id text,
                    name text,
                    category text,
                    population bigint,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/locations',
                    response_path '/features',
                    object_path '/properties',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT name, population FROM openapi_locations", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["Austin", "Dallas"]);

            // Test direct array response
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_tags (
                    id bigint,
                    name text,
                    count bigint,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/tags',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT name FROM openapi_tags", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["rust", "postgres", "wasm"]);

            // Test resource type/id pattern: /resources/{type}/{id}
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_resources (
                    id text,
                    type text,
                    name text,
                    description text,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/resources/{type}/{id}',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select(
                    "SELECT type, id, name FROM openapi_resources WHERE type = 'document' AND id = 'doc-001'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("type").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(results, vec!["document"]);

            // Test URL-based pagination with relative next_url (e.g., "?page=2")
            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_paginated (
                    id bigint,
                    name text,
                    attrs jsonb
                  )
                  SERVER openapi_server
                  OPTIONS (
                    endpoint '/paginated',
                    rowid_column 'id'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT name FROM openapi_paginated", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec!["Item One", "Item Two", "Item Three", "Item Four"]
            );
        });
    }

    // Exercises the OpenAPI FDW `auth_token_setting` option end-to-end: the FDW
    // resolves a Postgres session GUC via the `query-setting` host function and
    // injects it as the `Authorization` header on each request. The mock server
    // reflects the received header back so we can assert on it.
    #[pg_test]
    fn openapi_session_token_injection() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER wasm_wrapper
                     HANDLER wasm_fdw_handler VALIDATOR wasm_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"CREATE SERVER openapi_auth_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/openapi_fdw.wasm',
                       fdw_package_name 'supabase:openapi-fdw',
                       fdw_package_version '>=0.1.0',
                       base_url 'http://localhost:8096/openapi',
                       auth_token_setting 'app.test_token',
                       auth_token_prefix 'Bearer'
                     )"#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                r#"
                  CREATE FOREIGN TABLE openapi_whoami (
                    received_auth text
                  )
                  SERVER openapi_auth_server
                  OPTIONS (
                    endpoint '/whoami',
                    response_path '/data'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            // Negative: with the GUC unset, query-setting resolves to NULL and no
            // Authorization header is injected, so the mock reflects an empty string.
            let unset = c
                .select("SELECT received_auth FROM openapi_whoami", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("received_auth").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(unset, vec![""]);

            // Inject a per-session token, then confirm it reaches the API prefixed.
            c.update(
                "SELECT set_config('app.test_token', 'sess-secret-123', false)",
                None,
                &[],
            )
            .unwrap();

            let injected = c
                .select("SELECT received_auth FROM openapi_whoami", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("received_auth").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(injected, vec!["Bearer sess-secret-123"]);
        });
    }

    // Create the wasm FDW handler and an OpenAPI server pointing at the mock
    // server's write-support endpoints. Used by the openapi_write_* tests.
    fn create_openapi_write_server(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN DATA WRAPPER wasm_wrapper
                 HANDLER wasm_fdw_handler VALIDATOR wasm_fdw_validator"#,
            None,
            &[],
        )
        .unwrap();

        c.update(
            r#"CREATE SERVER openapi_write_server
                 FOREIGN DATA WRAPPER wasm_wrapper
                 OPTIONS (
                   fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/openapi_fdw.wasm',
                   fdw_package_name 'supabase:openapi-fdw',
                   fdw_package_version '>=0.1.0',
                   base_url 'http://localhost:8096/openapi'
                 )"#,
            None,
            &[],
        )
        .unwrap();
    }

    // Exercises the OpenAPI FDW write support end-to-end against the mock
    // server. The mock validates the request shapes a type-faithful FDW must
    // produce (rowid placement, no attrs/rowid in body, numbers as JSON
    // numbers) and answers with 4xx or in-band failure codes otherwise, so a
    // malformed request fails the statement.
    #[pg_test]
    fn openapi_write_support() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);

            // Plain JSON API: rowid appended to the URL path.
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_items (
                    id text,
                    name text,
                    count bigint,
                    active boolean
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_items',
                    rowid_column 'id',
                    writable 'true',
                    insert_method 'POST',
                    update_method 'PATCH',
                    delete_method 'DELETE'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            // UPDATE -> PATCH /wr_items/i-1 with a sparse typed JSON body.
            // The scan phase first fetches the row via GET (rowid pushdown).
            c.update(
                "UPDATE wr_items SET name = 'renamed', count = 7, active = true WHERE id = 'i-1'",
                None,
                &[],
            )
            .unwrap();

            // INSERT -> POST /wr_items; null id is omitted from the body.
            c.update(
                "INSERT INTO wr_items (name, count, active) VALUES ('new item', 3, false)",
                None,
                &[],
            )
            .unwrap();

            // DELETE -> DELETE /wr_items/i-1 (rowid in URL path).
            c.update("DELETE FROM wr_items WHERE id = 'i-1'", None, &[])
                .unwrap();

            // Envelope API (the hard case): PUT to the collection URL with
            // {"data":[{...}]}, rowid inside the record, and per-record
            // success signalled by a code in the HTTP 200 response body.
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_enveloped (
                    id text,
                    stage text,
                    amount numeric
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_enveloped',
                    rowid_column 'id',
                    writable 'true',
                    update_method 'PUT',
                    rowid_location 'body',
                    body_root_path '/data',
                    body_wrap 'array',
                    success_path '/data/0/code',
                    success_value 'SUCCESS',
                    success_status '200,201,202'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            // The mock answers {"data":[{"code":"SUCCESS"}]} only when the
            // envelope, rowid placement, and value types are all correct.
            c.update(
                "UPDATE wr_enveloped SET stage = 'Qualification', amount = 8000 WHERE id = 'e-1'",
                None,
                &[],
            )
            .unwrap();

            // Mixed per-verb rowid placement: DELETE via query parameter
            // (?ids=<id>) while other verbs would use the default.
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_delete (
                    id text,
                    name text
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_delete',
                    rowid_column 'id',
                    writable 'true',
                    delete_method 'DELETE',
                    delete_rowid_location 'query',
                    rowid_param 'ids',
                    success_path '/code',
                    success_value 'SUCCESS'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            // The mock returns code SUCCESS only when it sees ?ids=d-1.
            c.update("DELETE FROM wr_delete WHERE id = 'd-1'", None, &[])
                .unwrap();

            // Writes are strictly per-row with exactly one request per row:
            // the mock counts POSTs to /wr_count and reports the counter via
            // GET, so a 3-row INSERT must move it by exactly 3.
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_count (
                    id text,
                    name text,
                    posts bigint
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_count',
                    rowid_column 'id',
                    writable 'true',
                    insert_method 'POST'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let count_before = c
                .select("SELECT posts FROM wr_count", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("posts").unwrap())
                .next()
                .unwrap();

            c.update(
                "INSERT INTO wr_count (name) VALUES ('a'), ('b'), ('c')",
                None,
                &[],
            )
            .unwrap();

            let count_after = c
                .select("SELECT posts FROM wr_count", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("posts").unwrap())
                .next()
                .unwrap();
            assert_eq!(count_after - count_before, 3);
        });
    }

    // An HTTP 200 response carrying an in-band failure code must fail the
    // statement when success_path is configured.
    #[pg_test]
    fn openapi_write_failure_code_rejected() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_failcode (
                    id text,
                    name text
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_failcode',
                    rowid_column 'id',
                    writable 'true',
                    insert_method 'POST',
                    success_path '/code',
                    success_value 'SUCCESS'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
        });

        // The mock returns HTTP 200 with {"code":"FAILED"}.
        let result = std::panic::catch_unwind(|| {
            Spi::connect_mut(|c| {
                c.update("INSERT INTO wr_failcode (name) VALUES ('x')", None, &[])
                    .is_err()
            })
        });
        assert!(result.is_err());
    }

    // HTTP 207 Multi-Status is rejected: per-record outcomes inside it cannot
    // be verified.
    #[pg_test]
    fn openapi_write_multistatus_rejected() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_multistatus (
                    id text,
                    name text
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_multistatus',
                    rowid_column 'id',
                    writable 'true',
                    insert_method 'POST'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
        });

        let result = std::panic::catch_unwind(|| {
            Spi::connect_mut(|c| {
                c.update("INSERT INTO wr_multistatus (name) VALUES ('x')", None, &[])
                    .is_err()
            })
        });
        assert!(result.is_err());
    }

    // DML on a table without writable 'true' is rejected in begin_modify,
    // before any HTTP request.
    #[pg_test]
    fn openapi_write_requires_writable_option() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_readonly (
                    id text,
                    name text
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_items',
                    rowid_column 'id',
                    insert_method 'POST'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
        });

        let result = std::panic::catch_unwind(|| {
            Spi::connect_mut(|c| {
                c.update("INSERT INTO wr_readonly (name) VALUES ('x')", None, &[])
                    .is_err()
            })
        });
        assert!(result.is_err());
    }

    // An op whose *_method is unset errors instead of silently no-op'ing.
    #[pg_test]
    fn openapi_write_disabled_op_rejected() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_insert_only (
                    id text,
                    name text
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_items',
                    rowid_column 'id',
                    writable 'true',
                    insert_method 'POST'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
        });

        // update_method is unset, so UPDATE must error.
        let result = std::panic::catch_unwind(|| {
            Spi::connect_mut(|c| {
                c.update(
                    "UPDATE wr_insert_only SET name = 'y' WHERE id = 'i-1'",
                    None,
                    &[],
                )
                .is_err()
            })
        });
        assert!(result.is_err());
    }

    // A writable envelope table without success_path is a misconfiguration
    // that errors in begin_modify (the API may signal per-record failure
    // inside a 2xx body, which a status check alone cannot detect).
    #[pg_test]
    fn openapi_write_envelope_requires_success_path() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_misconfigured (
                    id text,
                    name text
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_enveloped',
                    rowid_column 'id',
                    writable 'true',
                    insert_method 'POST',
                    body_root_path '/data',
                    body_wrap 'array'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
        });

        let result = std::panic::catch_unwind(|| {
            Spi::connect_mut(|c| {
                c.update(
                    "INSERT INTO wr_misconfigured (name) VALUES ('x')",
                    None,
                    &[],
                )
                .is_err()
            })
        });
        assert!(result.is_err());
    }

    // RETURNING is rejected at plan time by the host framework.
    #[pg_test]
    fn openapi_write_returning_rejected() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_returning (
                    id text,
                    name text
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_items',
                    rowid_column 'id',
                    writable 'true',
                    insert_method 'POST'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
        });

        let result = std::panic::catch_unwind(|| {
            Spi::connect_mut(|c| {
                c.update(
                    "INSERT INTO wr_returning (name) VALUES ('x') RETURNING id",
                    None,
                    &[],
                )
                .is_err()
            })
        });
        assert!(result.is_err());
    }

    // Create the wasm FDW handler, a spec-backed OpenAPI server, and the target
    // schema, then run IMPORT FOREIGN SCHEMA. Used by the openapi_import_* tests.
    // The spec is served by the mock at /openapi/spec -- see IMPORT_SPEC in
    // server.py for the shape and why each path is there.
    fn import_openapi_schema(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN DATA WRAPPER wasm_wrapper
                 HANDLER wasm_fdw_handler VALIDATOR wasm_fdw_validator"#,
            None,
            &[],
        )
        .unwrap();

        c.update(
            r#"CREATE SERVER openapi_import_server
                 FOREIGN DATA WRAPPER wasm_wrapper
                 OPTIONS (
                   fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/openapi_fdw.wasm',
                   fdw_package_name 'supabase:openapi-fdw',
                   fdw_package_version '>=0.1.0',
                   base_url 'http://localhost:8096/openapi',
                   spec_url 'http://localhost:8096/openapi/spec'
                 )"#,
            None,
            &[],
        )
        .unwrap();

        c.update("CREATE SCHEMA api", None, &[]).unwrap();
        c.update(
            "IMPORT FOREIGN SCHEMA openapi FROM SERVER openapi_import_server INTO api",
            None,
            &[],
        )
        .unwrap();
    }

    // Read one foreign table's options as a text[] predicate result.
    fn ft_option_matches(c: &mut pgrx::spi::SpiClient<'_>, relname: &str, pred: &str) -> bool {
        let sql = format!(
            "SELECT {pred} AS matched FROM pg_foreign_table ft \
             JOIN pg_class cl ON cl.oid = ft.ftrelid \
             JOIN pg_namespace n ON n.oid = cl.relnamespace \
             WHERE n.nspname = 'api' AND cl.relname = '{relname}'"
        );
        c.select(&sql, None, &[])
            .unwrap()
            .filter_map(|r| r.get_by_name::<bool, _>("matched").unwrap())
            .next()
            .unwrap_or_else(|| panic!("foreign table api.{relname} not found"))
    }

    // An envelope response ({data: [Record]}) must generate columns from the
    // RECORD, plus a response_path pinning runtime extraction to the very
    // envelope those columns came from. Previously the envelope object itself
    // was modeled as the row, producing all-NULL rows for the most common REST
    // list shape.
    #[pg_test]
    fn openapi_import_generates_record_columns_and_response_path() {
        Spi::connect_mut(|c| {
            import_openapi_schema(c);

            assert!(
                ft_option_matches(c, "things", "'response_path=/data' = ANY(ftoptions)"),
                "things should pin response_path to the envelope it was derived from"
            );
            assert!(
                ft_option_matches(c, "things", "'rowid_column=id' = ANY(ftoptions)"),
                "things has an 'id' field so rowid_column should be emitted"
            );

            // The generated columns must actually select real record data --
            // this is what all-NULL rows looked like before.
            let names = c
                .select(r#"SELECT name FROM api.things"#, None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(names, vec!["Thing One"]);

            let counts = c
                .select(r#"SELECT "count" FROM api.things"#, None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("count").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(counts, vec![5]);
        });
    }

    // A record with no 'id' field must yield a table with NO rowid_column. The
    // old alphabetical fallback could pick a non-unique column (here 'label' or
    // 'sku'), which the read path would then treat as a single-resource lookup
    // and UPDATE/DELETE would address the wrong remote resource.
    #[pg_test]
    fn openapi_import_omits_rowid_without_id_field() {
        Spi::connect_mut(|c| {
            import_openapi_schema(c);

            // The table itself is still generated and readable...
            let skus = c
                .select("SELECT sku FROM api.widgets", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("sku").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(skus, vec!["w-1"]);

            // ...but carries no rowid_column at all.
            assert!(
                !ft_option_matches(
                    c,
                    "widgets",
                    "EXISTS (SELECT 1 FROM unnest(ftoptions) o WHERE o LIKE 'rowid_column=%')"
                ),
                "widgets has no 'id' field so no rowid_column may be guessed"
            );
        });
    }

    // A POST-only path must not produce a table: a plain SELECT on it would
    // trigger a remote create, and the Operation model cannot send a request
    // body so a POST-as-search table could not work anyway.
    #[pg_test]
    fn openapi_import_skips_post_only_endpoints() {
        Spi::connect_mut(|c| {
            import_openapi_schema(c);

            let orders_tables = c
                .select(
                    "SELECT count(*) AS n FROM pg_class cl \
                     JOIN pg_namespace n ON n.oid = cl.relnamespace \
                     WHERE n.nspname = 'api' AND cl.relname LIKE 'orders%'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("n").unwrap())
                .next()
                .unwrap();
            assert_eq!(orders_tables, 0, "POST-only /orders must not be imported");

            // Sanity: the GET-backed tables from the same spec were imported,
            // so a zero above means "POST skipped", not "import did nothing".
            let imported = c
                .select(
                    "SELECT count(*) AS n FROM pg_foreign_table ft \
                     JOIN pg_class cl ON cl.oid = ft.ftrelid \
                     JOIN pg_namespace n ON n.oid = cl.relnamespace \
                     WHERE n.nspname = 'api'",
                    None,
                    &[],
                )
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("n").unwrap())
                .next()
                .unwrap();
            assert_eq!(imported, 2, "expected exactly things + widgets");
        });
    }

    // KNOWN GAP -- ignored, not passing. The rowid identifies the remote
    // resource and lives in the URL, so it is excluded from the request body;
    // a changed rowid is therefore silently dropped and SHOULD be rejected.
    //
    // It is not, and cannot be from a wasm guest: exec_foreign_update in
    // supabase-wrappers/src/modify.rs strips the rowid column out of new_row
    // ("remove junk attributes, including rowid attribute") before calling
    // update(), so openapi_fdw's reject_rowid_reassignment never sees a new
    // rowid to compare. Observed behaviour today: this UPDATE issues
    // PATCH /wr_reassign/r-1 -- the OLD id -- and reports success.
    //
    // The mock deliberately answers 200 here so this test can distinguish
    // "rejected by the FDW" from "the API happened to error". Closing the gap
    // needs a supabase-wrappers core change; un-ignore this test then.
    #[pg_test]
    #[ignore = "blocked on supabase-wrappers core: modify.rs strips the rowid from new_row"]
    fn openapi_write_rejects_rowid_reassignment() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_reassign (
                    id text,
                    name text
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_reassign',
                    rowid_column 'id',
                    writable 'true',
                    update_method 'PATCH'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
        });

        let result = std::panic::catch_unwind(|| {
            Spi::connect_mut(|c| {
                c.update(
                    "UPDATE wr_reassign SET id = 'r-2', name = 'x' WHERE id = 'r-1'",
                    None,
                    &[],
                )
                .is_err()
            })
        });
        assert!(result.is_err());
    }

    // An UPDATE that leaves the rowid alone still works on the same table --
    // proves the guard above rejects reassignment specifically, not all UPDATEs.
    #[pg_test]
    fn openapi_write_allows_update_without_rowid_change() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_items_ok (
                    id text,
                    name text,
                    count bigint,
                    active boolean
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_items',
                    rowid_column 'id',
                    writable 'true',
                    update_method 'PATCH'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            c.update(
                "UPDATE wr_items_ok SET name = 'renamed', count = 7, active = true \
                 WHERE id = 'i-1'",
                None,
                &[],
            )
            .unwrap();
        });
    }

    // success_status is an allowlist of statuses that mean "written", not a
    // trust override. A non-2xx entry (e.g. a body-less 302, which would also
    // bypass the success_path body check) is a misconfiguration.
    #[pg_test]
    fn openapi_write_rejects_non_2xx_success_status() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_badstatus (
                    id text,
                    name text
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_items',
                    rowid_column 'id',
                    writable 'true',
                    insert_method 'POST',
                    success_status '200,302'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
        });

        let result = std::panic::catch_unwind(|| {
            Spi::connect_mut(|c| {
                c.update("INSERT INTO wr_badstatus (name) VALUES ('x')", None, &[])
                    .is_err()
            })
        });
        assert!(result.is_err());
    }

    // When the table shape makes success_path mandatory, the API is expected to
    // put the per-record outcome in the body. A 2xx that is not 204/205 and
    // carries no body cannot be verified, so it must fail closed rather than be
    // reported as a successful write.
    #[pg_test]
    fn openapi_write_empty_body_fails_closed() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);
            c.update(
                r#"
                  CREATE FOREIGN TABLE wr_emptybody (
                    id text,
                    name text
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/wr_emptybody',
                    rowid_column 'id',
                    writable 'true',
                    insert_method 'POST',
                    body_root_path '/data',
                    body_wrap 'array',
                    success_path '/data/0/code',
                    success_value 'SUCCESS'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
        });

        let result = std::panic::catch_unwind(|| {
            Spi::connect_mut(|c| {
                c.update("INSERT INTO wr_emptybody (name) VALUES ('x')", None, &[])
                    .is_err()
            })
        });
        assert!(result.is_err());
    }

    // A single business object that merely contains a field named like a
    // wrapper key must not be unwrapped. 'total' is business-plausible and so
    // is NOT envelope metadata -- unwrapping to 'items' would discard it.
    #[pg_test]
    fn openapi_read_preserves_business_siblings() {
        Spi::connect_mut(|c| {
            create_openapi_write_server(c);
            c.update(
                r#"
                  CREATE FOREIGN TABLE biz_siblings (
                    items jsonb,
                    total numeric
                  )
                  SERVER openapi_write_server
                  OPTIONS (
                    endpoint '/biz_siblings'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let totals = c
                .select("SELECT total FROM biz_siblings", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<pgrx::AnyNumeric, _>("total").unwrap())
                .map(|n| n.to_string())
                .collect::<Vec<_>>();
            assert_eq!(
                totals,
                vec!["99.99"],
                "sibling 'total' must survive: the response is a business object, not an envelope"
            );
        });
    }

    // Create the wasm FDW handler and a server configured for page-number
    // pagination against the mock's /paged endpoint.
    fn create_openapi_paged_server(c: &mut pgrx::spi::SpiClient<'_>, page_param: &str) {
        c.update(
            r#"CREATE FOREIGN DATA WRAPPER wasm_wrapper
                 HANDLER wasm_fdw_handler VALIDATOR wasm_fdw_validator"#,
            None,
            &[],
        )
        .unwrap();

        c.update(
            &format!(
                r#"CREATE SERVER openapi_paged_server
                     FOREIGN DATA WRAPPER wasm_wrapper
                     OPTIONS (
                       fdw_package_url 'file://../../../wasm-wrappers/fdw/target/wasm32-unknown-unknown/release/openapi_fdw.wasm',
                       fdw_package_name 'supabase:openapi-fdw',
                       fdw_package_version '>=0.1.0',
                       base_url 'http://localhost:8096/openapi',
                       page_param '{page_param}',
                       page_size_param 'per_page',
                       page_size '2'
                     )"#
            ),
            None,
            &[],
        )
        .unwrap();
    }

    // The mock's /paged endpoint returns 5 records over 3 pages of 2 and
    // signals continuation only with info.more_records -- no next URL, no
    // cursor. Before page-number support a scan returned 2 of 5 and reported
    // success, so the row count is the whole point of this test.
    #[pg_test]
    fn openapi_page_number_pagination_fetches_every_page() {
        Spi::connect_mut(|c| {
            create_openapi_paged_server(c, "page");
            c.update(
                r#"
                  CREATE FOREIGN TABLE paged_items (
                    id bigint,
                    name text
                  )
                  SERVER openapi_paged_server
                  OPTIONS (
                    endpoint '/paged',
                    response_path '/data',
                    has_more_path '/info/more_records'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let names = c
                .select("SELECT name FROM paged_items ORDER BY id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                names,
                vec!["item-1", "item-2", "item-3", "item-4", "item-5"],
                "expected all 5 records across 3 pages"
            );
        });
    }

    // LIMIT still stops the scan early rather than walking every page.
    #[pg_test]
    fn openapi_page_number_pagination_respects_limit() {
        Spi::connect_mut(|c| {
            create_openapi_paged_server(c, "page");
            c.update(
                r#"
                  CREATE FOREIGN TABLE paged_limited (
                    id bigint,
                    name text
                  )
                  SERVER openapi_paged_server
                  OPTIONS (
                    endpoint '/paged',
                    response_path '/data',
                    has_more_path '/info/more_records'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            let n = c
                .select("SELECT count(*) AS n FROM (SELECT * FROM paged_limited LIMIT 3) t", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<i64, _>("n").unwrap())
                .next()
                .unwrap();
            assert_eq!(n, 3);
        });
    }

    // page_param without has_more_path is a half configuration: there would be
    // no signal to stop on, so it must error at scan start rather than degrade
    // into the single-page read this feature exists to prevent.
    #[pg_test]
    fn openapi_page_param_without_has_more_path_rejected() {
        Spi::connect_mut(|c| {
            create_openapi_paged_server(c, "page");
            c.update(
                r#"
                  CREATE FOREIGN TABLE paged_misconfigured (
                    id bigint,
                    name text
                  )
                  SERVER openapi_paged_server
                  OPTIONS (
                    endpoint '/paged',
                    response_path '/data'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();
        });

        let result = std::panic::catch_unwind(|| {
            Spi::connect_mut(|c| {
                c.select("SELECT * FROM paged_misconfigured", None, &[])
                    .is_err()
            })
        });
        assert!(result.is_err());
    }

    // Existing cursor/URL/Link-header tables must be untouched by the new
    // options: with no page_param configured, nothing changes.
    #[pg_test]
    fn openapi_page_pagination_is_opt_in_only() {
        Spi::connect_mut(|c| {
            create_openapi_paged_server(c, "");
            c.update(
                r#"
                  CREATE FOREIGN TABLE paged_optout (
                    id bigint,
                    name text
                  )
                  SERVER openapi_paged_server
                  OPTIONS (
                    endpoint '/paged',
                    response_path '/data'
                  )
             "#,
                None,
                &[],
            )
            .unwrap();

            // No page_param, so no page mode and no error: the scan reads the
            // first page only, exactly as it did before this feature.
            let names = c
                .select("SELECT name FROM paged_optout ORDER BY id", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(names, vec!["item-1", "item-2"]);
        });
    }
}
