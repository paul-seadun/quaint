mod config;
mod conversion;
mod error;

use crate::{
    ast::{Query, Value},
    connector::{bind::Bind, metrics, queryable::*, timeout::timeout, ResultSet, Transaction},
    error::Error,
    visitor::{self, Visitor},
};
use async_trait::async_trait;
pub use config::*;
use either::Either;
use futures::{lock::Mutex, TryStreamExt};
use sqlx::{Column as _, Connect, Executor, PgConnection, Row as _};
use std::time::Duration;

/// A connector interface for the PostgreSQL database.
#[derive(Debug)]
pub struct PostgreSql {
    connection: Mutex<PgConnection>,
    pg_bouncer: bool,
    socket_timeout: Option<Duration>,
}

impl PostgreSql {
    /// Create a new connection to the database.
    pub async fn new(url: PostgresUrl) -> crate::Result<Self> {
        let config = url.to_config();
        let mut conn = PgConnection::connect_with(&config).await?;

        let schema = url.schema();

        // SET NAMES sets the client text encoding. It needs to be explicitly set for automatic
        // conversion to and from UTF-8 to happen server-side.
        //
        // Relevant docs: https://www.postgresql.org/docs/current/multibyte.html
        let session_variables = format!(
            r##"
            SET search_path = "{schema}";
            SET NAMES 'UTF8';
            "##,
            schema = schema
        );

        conn.execute(session_variables.as_str()).await?;

        Ok(Self {
            connection: Mutex::new(conn),
            socket_timeout: url.socket_timeout(),
            pg_bouncer: url.pg_bouncer(),
        })
    }
}

impl TransactionCapable for PostgreSql {}

#[async_trait]
impl Queryable for PostgreSql {
    async fn query(&self, q: Query<'_>) -> crate::Result<ResultSet> {
        let (sql, params) = visitor::Postgres::build(q)?;
        self.query_raw(sql.as_str(), params).await
    }

    async fn execute(&self, q: Query<'_>) -> crate::Result<u64> {
        let (sql, params) = visitor::Postgres::build(q)?;
        self.execute_raw(sql.as_str(), params).await
    }

    async fn query_raw(&self, sql: &str, params: Vec<Value<'_>>) -> crate::Result<ResultSet> {
        metrics::query_new("postgres.query_raw", sql, params, |params| async move {
            let mut conn = self.connection.lock().await;
            let describe = timeout(self.socket_timeout, conn.describe(sql)).await?;

            let mut query = sqlx::query(sql);

            match describe.parameters() {
                Some(Either::Left(type_infos)) => {
                    let values = params.into_iter();
                    let infos = type_infos.into_iter().map(Some);

                    for (param, type_info) in values.zip(infos) {
                        query = query.bind_value(param, type_info)?;
                    }
                }
                _ => {
                    for param in params.into_iter() {
                        query = query.bind_value(param, None)?;
                    }
                }
            };

            let mut columns = Vec::new();
            let mut rows = Vec::new();

            timeout(self.socket_timeout, async {
                let mut stream = query.fetch(&mut *conn);

                while let Some(row) = stream.try_next().await? {
                    if columns.is_empty() {
                        columns = row.columns().iter().map(|c| c.name().to_string()).collect();
                    }

                    rows.push(conversion::map_row(row)?);
                }

                Ok::<(), Error>(())
            })
            .await?;

            Ok(ResultSet::new(columns, rows))
        })
        .await
    }

    async fn execute_raw(&self, sql: &str, params: Vec<Value<'_>>) -> crate::Result<u64> {
        metrics::query_new("postgres.execute_raw", sql, params, |params| async move {
            let mut query = sqlx::query(sql);

            for param in params.into_iter() {
                query = query.bind_value(param, None)?;
            }

            let mut conn = self.connection.lock().await;
            let changes = query.execute(&mut *conn).await?;

            Ok(changes)
        })
        .await
    }

    async fn raw_cmd(&self, cmd: &str) -> crate::Result<()> {
        metrics::query("postgres.raw_cmd", cmd, &[], move || async move {
            let mut conn = self.connection.lock().await;
            timeout(self.socket_timeout, sqlx::query(cmd).execute(&mut *conn)).await?;
            Ok(())
        })
        .await
    }

    async fn version(&self) -> crate::Result<Option<String>> {
        let query = r#"SELECT version()"#;
        let rows = self.query_raw(query, vec![]).await?;

        let version_string = rows
            .get(0)
            .and_then(|row| row.get("version").and_then(|version| version.to_string()));

        Ok(version_string)
    }

    async fn server_reset_query(&self, tx: &Transaction<'_>) -> crate::Result<()> {
        if self.pg_bouncer {
            tx.raw_cmd("DEALLOCATE ALL").await
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ast::{self, *},
        col,
        connector::Queryable,
        error::*,
        single::Quaint,
        val, values,
    };
    use once_cell::sync::Lazy;
    use std::env;
    use url::Url;

    static CONN_STR: Lazy<String> = Lazy::new(|| env::var("TEST_PSQL").expect("TEST_PSQL env var"));

    #[test]
    fn should_parse_socket_url() {
        let url = PostgresUrl::new(Url::parse("postgresql:///dbname?host=/var/run/psql.sock").unwrap()).unwrap();
        assert_eq!("dbname", url.dbname());
        assert_eq!("/var/run/psql.sock", url.host());
    }

    #[test]
    fn should_parse_escaped_url() {
        let url = PostgresUrl::new(Url::parse("postgresql:///dbname?host=%2Fvar%2Frun%2Fpostgresql").unwrap()).unwrap();
        assert_eq!("dbname", url.dbname());
        assert_eq!("/var/run/postgresql", url.host());
    }

    #[test]
    fn should_allow_changing_of_cache_size() {
        let url =
            PostgresUrl::new(Url::parse("postgresql:///localhost:5432/foo?statement_cache_size=420").unwrap()).unwrap();
        assert_eq!(420, url.statement_cache_size());
    }

    #[test]
    fn should_have_default_cache_size() {
        let url = PostgresUrl::new(Url::parse("postgresql:///localhost:5432/foo").unwrap()).unwrap();
        assert_eq!(500, url.statement_cache_size());
    }

    #[test]
    fn should_not_enable_caching_with_pgbouncer() {
        let url = PostgresUrl::new(Url::parse("postgresql:///localhost:5432/foo?pgbouncer=true").unwrap()).unwrap();
        assert_eq!(0, url.statement_cache_size());
    }

    #[test]
    fn should_parse_default_host() {
        let url = PostgresUrl::new(Url::parse("postgresql:///dbname").unwrap()).unwrap();
        assert_eq!("dbname", url.dbname());
        assert_eq!("localhost", url.host());
    }

    #[tokio::test]
    async fn should_provide_a_database_connection() {
        let connection = Quaint::new(&CONN_STR).await.unwrap();

        let res = connection
            .query_raw("select * from \"pg_catalog\".\"pg_am\" where amtype = 'x'", vec![])
            .await
            .unwrap();

        // No results expected.
        assert!(res.is_empty());
    }

    #[allow(unused)]
    const TABLE_DEF: &str = r#"
    CREATE TABLE "user"(
        id       int4    PRIMARY KEY     NOT NULL,
        name     text    NOT NULL,
        age      int4    NOT NULL,
        salary   float4
    );
    "#;

    #[allow(unused)]
    const CREATE_USER: &str = r#"
    INSERT INTO "user" (id, name, age, salary)
    VALUES (1, 'Joe', 27, 20000.00 );
    "#;

    #[allow(unused)]
    const DROP_TABLE: &str = "DROP TABLE IF EXISTS \"user\";";

    #[tokio::test]
    async fn should_map_columns_correctly() {
        let connection = Quaint::new(&CONN_STR).await.unwrap();

        connection.query_raw(DROP_TABLE, vec![]).await.unwrap();
        connection.query_raw(TABLE_DEF, vec![]).await.unwrap();

        let changes = connection.execute_raw(CREATE_USER, vec![]).await.unwrap();
        assert_eq!(1, changes);

        let rows = connection.query_raw("SELECT * FROM \"user\"", vec![]).await.unwrap();
        assert_eq!(rows.len(), 1);

        let row = rows.get(0).unwrap();
        assert_eq!(row["id"].as_i64(), Some(1));
        assert_eq!(row["name"].as_str(), Some("Joe"));
        assert_eq!(row["age"].as_i64(), Some(27));

        assert_eq!(row["salary"].as_f64(), Some(20000.0));
    }

    #[tokio::test]
    async fn tuples_in_selection() {
        let table = r#"
            CREATE TABLE tuples (id SERIAL PRIMARY KEY, age INTEGER NOT NULL, length REAL NOT NULL);
        "#;

        let connection = Quaint::new(&CONN_STR).await.unwrap();

        connection
            .query_raw("DROP TABLE IF EXISTS tuples", vec![])
            .await
            .unwrap();
        connection.query_raw(table, vec![]).await.unwrap();

        let insert = Insert::multi_into("tuples", vec!["age", "length"])
            .values(vec![val!(35), val!(20.0)])
            .values(vec![val!(40), val!(18.0)]);

        connection.insert(insert.into()).await.unwrap();

        // 1-tuple
        {
            let mut cols = Row::new();
            cols.push(Column::from("age"));

            let mut vals = Row::new();
            vals.push(35);

            let select = Select::from_table("tuples").so_that(cols.in_selection(vals));
            let rows = connection.select(select).await.unwrap();

            let row = rows.get(0).unwrap();
            assert_eq!(row["age"].as_i64(), Some(35));
            assert_eq!(row["length"].as_f64(), Some(20.0));
        }

        // 2-tuple
        {
            let cols = Row::from((col!("age"), col!("length")));
            let vals = values!((35, 20.0));

            let select = Select::from_table("tuples").so_that(cols.in_selection(vals));
            let rows = connection.select(select).await.unwrap();

            let row = rows.get(0).unwrap();
            assert_eq!(row["age"].as_i64(), Some(35));
            assert_eq!(row["length"].as_f64(), Some(20.0));
        }
    }

    #[tokio::test]
    async fn type_roundtrips() {
        let table = r#"
            CREATE TABLE types (
                id SERIAL PRIMARY KEY,

                binary_bits bit(12),
                binary_bits_arr bit(12)[],
                bytes_uuid uuid,
                bytes_uuid_arr uuid[],
                network_inet inet,
                network_inet_arr inet[],
                numeric_float4 float4,
                numeric_float4_arr float4[],
                numeric_float8 float8,
                numeric_decimal decimal(12,8),
                numeric_money money,
                time_timetz timetz,
                time_time time,
                time_date date,
                text_jsonb jsonb,
                time_timestamptz timestamptz
            );
        "#;

        let connection = Quaint::new(&CONN_STR).await.unwrap();

        connection.raw_cmd("DROP TABLE IF EXISTS types").await.unwrap();
        connection.raw_cmd(table).await.unwrap();

        let insert = ast::Insert::single_into("types")
            .value("binary_bits", "111011100011")
            .value("binary_bits_arr", Value::array(vec!["111011100011"]))
            .value(
                "bytes_uuid",
                Value::uuid("111142ec-880b-4062-913d-8eac479ab957".parse().unwrap()),
            )
            .value(
                "bytes_uuid_arr",
                Value::array(vec![
                    Value::uuid("111142ec-880b-4062-913d-8eac479ab957".parse().unwrap()),
                    Value::uuid("111142ec-880b-4062-913d-8eac479ab958".parse().unwrap()),
                ]),
            )
            .value("network_inet", "127.0.0.1")
            .value("network_inet_arr", Value::array(vec!["127.0.0.1"]))
            .value("numeric_float4", 3.14)
            .value("numeric_float4_arr", Value::array(vec![3.14]))
            .value("numeric_float8", 3.14912932)
            .value("numeric_decimal", Value::real("0.00006927".parse().unwrap()))
            .value("numeric_money", 3.551)
            .value("time_date", Value::date("2020-03-02".parse().unwrap()))
            .value("time_timetz", Value::datetime("2020-03-02T08:00:00Z".parse().unwrap()))
            .value("time_time", Value::time("08:00:00".parse().unwrap()))
            .value("text_jsonb", serde_json::json!({ "isJSONB": true }))
            .value(
                "time_timestamptz",
                Value::datetime("2020-03-02T08:00:00Z".parse().unwrap()),
            );
        let select = ast::Select::from_table("types").value(ast::asterisk());

        connection.query(insert.into()).await.unwrap();
        let result = connection
            .query(select.into())
            .await
            .unwrap()
            .into_single()
            .unwrap()
            .values;

        let expected = &[
            Value::integer(1),
            Value::text("111011100011"),
            Value::array(vec!["111011100011"]),
            Value::uuid("111142ec-880b-4062-913d-8eac479ab957".parse().unwrap()),
            Value::array(vec![
                Value::uuid("111142ec-880b-4062-913d-8eac479ab957".parse().unwrap()),
                Value::uuid("111142ec-880b-4062-913d-8eac479ab958".parse().unwrap()),
            ]),
            Value::text("127.0.0.1/32"),
            Value::array(vec!["127.0.0.1/32"]),
            Value::real("3.14".parse().unwrap()),
            Value::array(vec![3.14]),
            Value::real("3.14912932".parse().unwrap()),
            Value::real("0.00006927".parse().unwrap()),
            Value::real("3.55".parse().unwrap()),
            Value::datetime("1970-01-01T08:00:00Z".parse().unwrap()),
            Value::time("08:00:00".parse().unwrap()),
            Value::date("2020-03-02".parse().unwrap()),
            Value::json(serde_json::json!({ "isJSONB": true })),
            Value::datetime("2020-03-02T08:00:00Z".parse().unwrap()),
        ];

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_money_value_conversions_match_with_manual_inserts() {
        let conn = Quaint::new(&CONN_STR).await.unwrap();

        conn.query_raw("DROP TABLE IF EXISTS money_conversion_test", vec![])
            .await
            .unwrap();
        conn.query_raw(
            "CREATE TABLE money_conversion_test (id SERIAL PRIMARY KEY, cash money)",
            vec![],
        )
        .await
        .unwrap();

        conn.query_raw(
            "INSERT INTO money_conversion_test (cash) VALUES (0), (12), (855.32)",
            vec![],
        )
        .await
        .unwrap();

        let select = ast::Select::from_table("money_conversion_test").value(ast::asterisk());
        let result = conn.query(select.into()).await.unwrap();

        let expected_first_row = vec![Value::integer(1), Value::real("0".parse().unwrap())];

        assert_eq!(result.get(0).unwrap().values, &expected_first_row);

        let expected_second_row = vec![Value::integer(2), Value::real("12".parse().unwrap())];

        assert_eq!(result.get(1).unwrap().values, &expected_second_row);

        let expected_third_row = vec![Value::integer(3), Value::real("855.32".parse().unwrap())];

        assert_eq!(result.get(2).unwrap().values, &expected_third_row);
    }

    #[tokio::test]
    async fn test_bits_value_conversions_match_with_manual_inserts() {
        let conn = Quaint::new(&CONN_STR).await.unwrap();

        conn.query_raw("DROP TABLE IF EXISTS bits_conversion_test", vec![])
            .await
            .unwrap();
        conn.query_raw(
            "CREATE TABLE bits_conversion_test (id SERIAL PRIMARY KEY, onesandzeroes bit(12), vars varbit(12))",
            vec![],
        )
        .await
        .unwrap();

        conn.query_raw(
            "INSERT INTO bits_conversion_test (onesandzeroes, vars) VALUES \
            ('000000000000', '0000000000'), \
            ('110011000100', '110011000100')",
            vec![],
        )
        .await
        .unwrap();

        let select = ast::Select::from_table("bits_conversion_test").value(ast::asterisk());
        let result = conn.query(select.into()).await.unwrap();

        let expected_first_row = vec![
            Value::integer(1),
            Value::text("000000000000"),
            Value::text("0000000000"),
        ];

        assert_eq!(result.get(0).unwrap().values, &expected_first_row);

        let expected_second_row = vec![
            Value::integer(2),
            Value::text("110011000100"),
            Value::text("110011000100"),
        ];

        assert_eq!(result.get(1).unwrap().values, &expected_second_row);
    }

    #[tokio::test]
    async fn test_uniq_constraint_violation() {
        let conn = Quaint::new(&CONN_STR).await.unwrap();

        let _ = conn.raw_cmd("DROP TABLE test_uniq_constraint_violation").await;
        let _ = conn.raw_cmd("DROP INDEX idx_uniq_constraint_violation").await;

        conn.raw_cmd("CREATE TABLE test_uniq_constraint_violation (id1 int, id2 int)")
            .await
            .unwrap();
        conn.raw_cmd("CREATE UNIQUE INDEX idx_uniq_constraint_violation ON test_uniq_constraint_violation (id1, id2)")
            .await
            .unwrap();

        conn.query_raw(
            "INSERT INTO test_uniq_constraint_violation (id1, id2) VALUES (1, 2)",
            vec![],
        )
        .await
        .unwrap();

        let res = conn
            .query_raw(
                "INSERT INTO test_uniq_constraint_violation (id1, id2) VALUES (1, 2)",
                vec![],
            )
            .await;

        let err = res.unwrap_err();

        match err.kind() {
            ErrorKind::UniqueConstraintViolation { constraint } => {
                assert_eq!(Some("23505"), err.original_code());
                assert_eq!(Some("Key (id1, id2)=(1, 2) already exists."), err.original_message());

                assert_eq!(
                    &DatabaseConstraint::Fields(vec![String::from("id1"), String::from("id2")]),
                    constraint,
                )
            }
            _ => panic!(err),
        }
    }

    #[tokio::test]
    async fn test_null_constraint_violation() {
        let conn = Quaint::new(&CONN_STR).await.unwrap();

        let _ = conn.raw_cmd("DROP TABLE test_null_constraint_violation").await;

        conn.raw_cmd("CREATE TABLE test_null_constraint_violation (id1 int not null, id2 int not null)")
            .await
            .unwrap();

        let res = conn
            .query_raw("INSERT INTO test_null_constraint_violation DEFAULT VALUES", vec![])
            .await;

        let err = res.unwrap_err();

        match err.kind() {
            ErrorKind::NullConstraintViolation { constraint } => {
                assert_eq!(Some("23502"), err.original_code());
                assert_eq!(
                    Some("null value in column \"id1\" violates not-null constraint"),
                    err.original_message()
                );
                assert_eq!(&DatabaseConstraint::Fields(vec![String::from("id1")]), constraint)
            }
            _ => panic!(err),
        }
    }

    #[tokio::test]
    async fn test_custom_search_path() {
        let mut url = Url::parse(&CONN_STR).unwrap();
        url.query_pairs_mut().append_pair("schema", "musti-test");

        let client = Quaint::new(url.as_str()).await.unwrap();

        let result_set = client.query_raw("SHOW search_path", vec![]).await.unwrap();
        let row = result_set.first().unwrap();

        assert_eq!(Some("\"musti-test\""), row[0].as_str());
    }

    #[tokio::test]
    async fn should_map_nonexisting_database_error() {
        let mut url = Url::parse(&CONN_STR).unwrap();
        url.set_path("/this_does_not_exist");

        let res = Quaint::new(url.as_str()).await;

        assert!(res.is_err());

        match res {
            Ok(_) => unreachable!(),
            Err(e) => match e.kind() {
                ErrorKind::DatabaseDoesNotExist { db_name } => {
                    assert_eq!(Some("3D000"), e.original_code());
                    assert_eq!(
                        Some("database \"this_does_not_exist\" does not exist"),
                        e.original_message()
                    );
                    assert_eq!("this_does_not_exist", db_name.as_str())
                }
                kind => panic!("Expected `DatabaseDoesNotExist`, got {:?}", kind),
            },
        }
    }

    #[tokio::test]
    async fn should_map_tls_errors() {
        let mut url = Url::parse(&CONN_STR).expect("parsing url");
        url.set_query(Some("sslmode=require&sslaccept=strict"));

        let res = Quaint::new(url.as_str()).await;

        assert!(res.is_err());

        match res {
            Ok(_) => unreachable!(),
            Err(e) => match e.kind() {
                ErrorKind::TlsError { .. } => (),
                other => panic!("{:#?}", other),
            },
        }
    }

    #[tokio::test]
    async fn should_map_null_constraint_errors() {
        use crate::ast::*;

        let conn = Quaint::new(&CONN_STR).await.unwrap();

        conn.query_raw("DROP TABLE IF EXISTS should_map_null_constraint_errors_test", vec![])
            .await
            .unwrap();

        conn.query_raw(
            "CREATE TABLE should_map_null_constraint_errors_test (id TEXT PRIMARY KEY, optional TEXT)",
            vec![],
        )
        .await
        .unwrap();

        let with_null_id =
            Insert::single_into("should_map_null_constraint_errors_test").value("id", Option::<String>::None);

        let err = conn.insert(with_null_id.into()).await.unwrap_err();

        match err.kind() {
            ErrorKind::NullConstraintViolation { constraint } => {
                assert_eq!(Some("23502"), err.original_code());
                assert_eq!(
                    Some("null value in column \"id\" violates not-null constraint"),
                    err.original_message()
                );
                assert_eq!(constraint, &DatabaseConstraint::Fields(vec!["id".into()]))
            }
            other => panic!("{:?}", other),
        }

        // Schema change null constraint violations now

        let insert_with_id = Insert::single_into("should_map_null_constraint_errors_test").value("id", "theid");
        conn.insert(insert_with_id.into()).await.unwrap();

        let err = conn
            .query_raw(
                "ALTER TABLE should_map_null_constraint_errors_test ALTER COLUMN optional SET NOT NULL",
                vec![],
            )
            .await
            .unwrap_err();

        match err.kind() {
            ErrorKind::NullConstraintViolation { constraint } => {
                assert_eq!(Some("23502"), err.original_code());
                assert_eq!(Some("column \"optional\" contains null values"), err.original_message());
                assert_eq!(constraint, &DatabaseConstraint::Fields(vec!["optional".into()]))
            }
            other => panic!("{:?}", other),
        }
    }

    #[tokio::test]
    async fn json_filtering_works() {
        let conn = Quaint::new(&CONN_STR).await.unwrap();

        let create_table = r#"
            CREATE TABLE "table_with_json" (
                id SERIAL PRIMARY KEY,
                obj json
            )
        "#;

        let drop_table = r#"DROP TABLE IF EXISTS "table_with_json""#;

        let insert = Insert::single_into("table_with_json").value("obj", serde_json::json!({ "a": "a" }));
        let second_insert = Insert::single_into("table_with_json").value("obj", serde_json::json!({ "a": "b" }));

        conn.query_raw(drop_table, vec![]).await.unwrap();
        conn.query_raw(create_table, vec![]).await.unwrap();
        conn.insert(insert.into()).await.unwrap();
        conn.query(second_insert.into()).await.unwrap();

        // Equals
        {
            let select = Select::from_table("table_with_json")
                .value(asterisk())
                .so_that(Column::from("obj").equals(Value::json(serde_json::json!({ "a": "b" }))));

            let result = conn.query(select.into()).await.unwrap();

            assert_eq!(result.len(), 1);
            assert_eq!(result.get(0).unwrap().get("id").unwrap(), &Value::integer(2))
        }

        // Not equals
        {
            let select = Select::from_table("table_with_json")
                .value(asterisk())
                .so_that(Column::from("obj").not_equals(Value::json(serde_json::json!({ "a": "a" }))));

            let result = conn.query(select.into()).await.unwrap();

            assert_eq!(result.len(), 1);
            assert_eq!(result.get(0).unwrap().get("id").unwrap(), &Value::integer(2))
        }
    }

    #[tokio::test]
    async fn upper_fun() {
        let conn = Quaint::new(&CONN_STR).await.unwrap();
        let select = Select::default().value(upper("foo").alias("val"));

        let res = conn.query(select.into()).await.unwrap();
        let row = res.get(0).unwrap();
        let val = row.get("val").unwrap().as_str();

        assert_eq!(Some("FOO"), val);
    }

    #[tokio::test]
    async fn lower_fun() {
        let conn = Quaint::new(&CONN_STR).await.unwrap();
        let select = Select::default().value(lower("BAR").alias("val"));

        let res = conn.query(select.into()).await.unwrap();
        let row = res.get(0).unwrap();
        let val = row.get("val").unwrap().as_str();

        assert_eq!(Some("bar"), val);
    }
}
