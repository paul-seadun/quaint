mod conversion;
mod error;

use crate::{
    ast::{Id, ParameterizedValue, Query},
    connector::{
        queryable::{Database, Queryable, ToColumnNames, ToRow, Transactional},
        ResultSet,
    },
    error::Error,
    visitor::{self, Visitor},
};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::NO_PARAMS;
use std::{collections::HashSet, convert::TryFrom, path::PathBuf};

type PooledConnection = r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>;
type Pool = r2d2::Pool<SqliteConnectionManager>;

/// A connector interface for the SQLite database.
pub struct Sqlite {
    file_path: String,
    pool: Pool,
    test_mode: bool,
}

impl Transactional for Sqlite {
    type Error = Error;

    fn with_transaction<F, T>(&self, db: &str, f: F) -> crate::Result<T>
    where
        F: FnOnce(&mut Queryable) -> crate::Result<T>,
    {
        self.with_connection_internal(db, |conn| {
            let tx = conn.transaction()?;
            tx.set_prepared_statement_cache_capacity(65536);

            let mut connection_like = ConnectionLike::from(tx);
            let result = f(&mut connection_like);

            if result.is_ok() {
                let tx = rusqlite::Transaction::try_from(connection_like).unwrap();
                tx.commit()?;
            }

            result
        })
    }
}

impl Database for Sqlite {
    fn with_connection<'a, F, T>(&self, db: &str, f: F) -> crate::Result<T>
    where
        F: FnOnce(&mut Queryable) -> crate::Result<T>,
        Self: Sized,
    {
        self.with_connection_internal(db, |c| f(c))
    }

    fn execute_on_connection<'a>(&self, db: &str, query: Query<'a>) -> crate::Result<Option<Id>> {
        self.with_connection(&db, |conn| conn.execute(query))
    }

    fn query_on_connection<'a>(&self, db: &str, query: Query<'a>) -> crate::Result<ResultSet> {
        self.with_connection(&db, |conn| conn.query(query))
    }

    fn query_on_raw_connection<'a>(
        &self,
        db: &str,
        sql: &str,
        params: &[ParameterizedValue<'a>],
    ) -> crate::Result<ResultSet> {
        self.with_connection(&db, |conn| conn.query_raw(&sql, &params))
    }
}

pub enum ConnectionLike<'a> {
    Pooled(PooledConnection),
    Connection(rusqlite::Connection),
    Transaction(rusqlite::Transaction<'a>),
}

impl<'a> From<PooledConnection> for ConnectionLike<'a> {
    fn from(conn: PooledConnection) -> Self {
        ConnectionLike::Pooled(conn)
    }
}

impl<'a> From<rusqlite::Connection> for ConnectionLike<'a> {
    fn from(conn: rusqlite::Connection) -> Self {
        ConnectionLike::Connection(conn)
    }
}

impl<'a> From<rusqlite::Transaction<'a>> for ConnectionLike<'a> {
    fn from(conn: rusqlite::Transaction<'a>) -> Self {
        ConnectionLike::Transaction(conn)
    }
}

impl<'a> TryFrom<ConnectionLike<'a>> for rusqlite::Transaction<'a> {
    type Error = Error;

    fn try_from(cl: ConnectionLike<'a>) -> crate::Result<Self> {
        match cl {
            ConnectionLike::Transaction(tx) => Ok(tx),
            _ => Err(Error::ConversionError(
                "ConnectionLike was not a transaction...",
            )),
        }
    }
}

impl<'a> TryFrom<ConnectionLike<'a>> for PooledConnection {
    type Error = Error;

    fn try_from(cl: ConnectionLike<'a>) -> crate::Result<Self> {
        match cl {
            ConnectionLike::Pooled(pooled) => Ok(pooled),
            _ => Err(Error::ConversionError(
                "ConnectionLike was not a pooled connection...",
            )),
        }
    }
}

impl<'a> TryFrom<ConnectionLike<'a>> for rusqlite::Connection {
    type Error = Error;

    fn try_from(cl: ConnectionLike<'a>) -> crate::Result<Self> {
        match cl {
            ConnectionLike::Connection(conn) => Ok(conn),
            _ => Err(Error::ConversionError(
                "ConnectionLike was not a connection...",
            )),
        }
    }
}

impl<'a> ConnectionLike<'a> {
    fn prepare_cached(&self, sql: &str) -> rusqlite::Result<rusqlite::CachedStatement> {
        match self {
            ConnectionLike::Pooled(c) => c.prepare_cached(sql),
            ConnectionLike::Connection(c) => c.prepare_cached(sql),
            ConnectionLike::Transaction(c) => c.prepare_cached(sql),
        }
    }

    fn last_insert_rowid(&self) -> i64 {
        match self {
            ConnectionLike::Pooled(c) => c.last_insert_rowid(),
            ConnectionLike::Connection(c) => c.last_insert_rowid(),
            ConnectionLike::Transaction(c) => c.last_insert_rowid(),
        }
    }

    pub fn transaction(&mut self) -> rusqlite::Result<rusqlite::Transaction> {
        match self {
            ConnectionLike::Pooled(ref mut c) => c.transaction(),
            ConnectionLike::Connection(ref mut c) => c.transaction(),
            ConnectionLike::Transaction(_) => {
                panic!("Could not start a transaction from transaction")
            }
        }
    }
}

impl<'t> Queryable for ConnectionLike<'t> {
    fn execute<'a>(&mut self, q: Query<'a>) -> crate::Result<Option<Id>> {
        let (sql, params) = dbg!(visitor::Sqlite::build(q));

        let mut stmt = self.prepare_cached(&sql)?;
        stmt.execute(params)?;

        Ok(Some(Id::Int(self.last_insert_rowid() as usize)))
    }

    fn query<'a>(&mut self, q: Query<'a>) -> crate::Result<ResultSet> {
        let (sql, params) = dbg!(visitor::Sqlite::build(q));
        self.query_raw(&sql, &params)
    }

    fn query_raw<'a>(
        &mut self,
        sql: &str,
        params: &[ParameterizedValue<'a>],
    ) -> crate::Result<ResultSet> {
        let mut stmt = self.prepare_cached(sql)?;
        let mut rows = stmt.query(params)?;

        let mut result = ResultSet::new(rows.to_column_names(), Vec::new());

        while let Some(row) = rows.next()? {
            result.rows.push(row.to_result_row()?);
        }

        Ok(result)
    }

    fn turn_off_fk_constraints(&mut self) -> crate::Result<()> {
        self.query_raw("PRAGMA foreign_keys = OFF", &[])?;
        Ok(())
    }

    fn turn_on_fk_constraints(&mut self) -> crate::Result<()> {
        self.query_raw("PRAGMA foreign_keys = ON", &[])?;
        Ok(())
    }
}

impl TryFrom<&str> for Sqlite {
    type Error = Error;

    /// Todo connection limit configuration
    fn try_from(url: &str) -> crate::Result<Sqlite> {
        // We must handle file URLs ourselves.
        let normalized = url.trim_start_matches("file:");
        let path = PathBuf::from(&normalized);

        if path.is_dir() {
            Err(Error::DatabaseUrlIsInvalid(url.to_string()))
        } else {
            Sqlite::new(normalized.to_string(), 10, false)
        }
    }
}

impl Sqlite {
    pub fn new(file_path: String, connection_limit: u32, test_mode: bool) -> crate::Result<Sqlite> {
        let pool = r2d2::Pool::builder()
            .max_size(connection_limit)
            .build(SqliteConnectionManager::memory())?;

        Ok(Sqlite {
            file_path,
            pool,
            test_mode,
        })
    }

    pub fn does_file_exist(&self) -> bool {
        let path = PathBuf::from(&self.file_path);
        path.exists()
    }

    fn attach_database(&self, conn: &mut rusqlite::Connection, db_name: &str) -> crate::Result<()> {
        let mut stmt = conn.prepare("PRAGMA database_list")?;

        let databases: HashSet<String> = stmt
            .query_map(NO_PARAMS, |row| {
                let name: String = row.get(1)?;

                Ok(name)
            })?
            .map(|res| res.unwrap())
            .collect();

        if !databases.contains(db_name) {
            rusqlite::Connection::execute(
                conn,
                "ATTACH DATABASE ? AS ?",
                &[self.file_path.as_ref(), db_name],
            )?;
        }

        rusqlite::Connection::execute(conn, "PRAGMA foreign_keys = ON", NO_PARAMS)?;
        Ok(())
    }

    fn with_connection_internal<F, T>(&self, db: &str, f: F) -> crate::Result<T>
    where
        F: FnOnce(&mut ConnectionLike) -> crate::Result<T>,
    {
        let mut conn = self.pool.get()?;
        self.attach_database(&mut conn, db)?;

        let mut connection_like = ConnectionLike::from(conn);
        let result = f(&mut connection_like);

        if self.test_mode {
            let conn = PooledConnection::try_from(connection_like).unwrap();
            conn.execute("DETACH DATABASE ?", &[db])?;
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_provide_a_database_connection() {
        let connector = Sqlite::new(String::from("db/test.db"), 1, true).unwrap();

        connector
            .with_connection("test", |connection| {
                let res = connection.query_raw("SELECT * FROM sqlite_master", &[])?;

                // No results expected.
                assert!(res.is_empty());

                Ok(())
            })
            .unwrap()
    }

    #[test]
    fn should_provide_a_database_transaction() {
        let connector = Sqlite::new(String::from("db/test.db"), 1, true).unwrap();

        connector
            .with_transaction("test", |transaction| {
                let res = transaction.query_raw("SELECT * FROM sqlite_master", &[])?;

                // No results expected.
                assert!(res.is_empty());

                Ok(())
            })
            .unwrap()
    }

    #[allow(unused)]
    const TABLE_DEF: &str = r#"
    CREATE TABLE USER (
        ID INT PRIMARY KEY     NOT NULL,
        NAME           TEXT    NOT NULL,
        AGE            INT     NOT NULL,
        SALARY         REAL
    );
    "#;

    #[allow(unused)]
    const CREATE_USER: &str = r#"
    INSERT INTO USER (ID,NAME,AGE,SALARY)
    VALUES (1, 'Joe', 27, 20000.00 );
    "#;

    #[test]
    fn should_map_columns_correctly() {
        let connector = Sqlite::new(String::from("db/test.db"), 1, true).unwrap();

        connector
            .with_connection("test", |connection| {
                connection.query_raw(TABLE_DEF, &[])?;
                connection.query_raw(CREATE_USER, &[])?;

                let rows = connection.query_raw("SELECT * FROM USER", &[])?;
                assert_eq!(rows.len(), 1);

                let row = rows.get(0).unwrap();
                assert_eq!(row["ID"].as_i64(), Some(1));
                assert_eq!(row["NAME"].as_str(), Some("Joe"));
                assert_eq!(row["AGE"].as_i64(), Some(27));
                assert_eq!(row["SALARY"].as_f64(), Some(20000.0));

                Ok(())
            })
            .unwrap()
    }
}