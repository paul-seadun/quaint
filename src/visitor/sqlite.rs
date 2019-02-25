use crate::{ast::*, visitor::Visitor};

use rusqlite::{
    types::{Null, ToSql, ToSqlOutput},
    Error as RusqlError,
};

pub struct Sqlite {
    parameters: Vec<ParameterizedValue>,
}

impl Visitor for Sqlite {
    const C_PARAM: &'static str = "?";
    const C_QUOTE: &'static str = "`";

    fn add_parameter(&mut self, value: ParameterizedValue) {
        self.parameters.push(value);
    }

    fn build<Q>(query: Q) -> (String, Vec<ParameterizedValue>)
    where
        Q: Into<Query>,
    {
        let mut sqlite = Sqlite {
            parameters: Vec::new(),
        };

        (
            Sqlite::visit_query(&mut sqlite, query.into()),
            sqlite.parameters,
        )
    }
}

impl ToSql for ParameterizedValue {
    fn to_sql(&self) -> Result<ToSqlOutput, RusqlError> {
        let value = match self {
            ParameterizedValue::Null => ToSqlOutput::from(Null),
            ParameterizedValue::Integer(integer) => ToSqlOutput::from(*integer),
            ParameterizedValue::Real(float) => ToSqlOutput::from(*float),
            ParameterizedValue::Text(string) => ToSqlOutput::from(string.clone()),
            ParameterizedValue::Boolean(boo) => ToSqlOutput::from(*boo),
        };

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use crate::visitor::*;

    fn expected_values<T>(sql: &'static str, params: Vec<T>) -> (String, Vec<ParameterizedValue>)
    where
        T: Into<ParameterizedValue>,
    {
        (
            String::from(sql),
            params.into_iter().map(|p| p.into()).collect(),
        )
    }

    #[test]
    fn test_select_1() {
        let expected = expected_values("SELECT ?", vec![1]);

        let query = Select::default().value(1);
        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected.0, sql);
        assert_eq!(expected.1, params);
    }

    #[test]
    fn test_select_star_from() {
        let expected_sql = "SELECT * FROM `musti` LIMIT -1";
        let query = Select::from("musti");
        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected_sql, sql);
        assert_eq!(Vec::<ParameterizedValue>::new(), params);
    }

    #[test]
    fn test_select_fields_from() {
        let expected_sql = "SELECT `paw`, `nose` FROM `cat`.`musti` LIMIT -1";
        let query = Select::from(("cat", "musti")).column("paw").column("nose");
        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected_sql, sql);
        assert_eq!(Vec::<ParameterizedValue>::new(), params);
    }

    #[test]
    fn test_select_where_equals() {
        let expected = expected_values(
            "SELECT * FROM `naukio` WHERE `word` = ? LIMIT -1",
            vec!["meow"],
        );

        let query = Select::from("naukio").so_that("word".equals("meow"));
        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected.0, sql);
        assert_eq!(expected.1, params);
    }

    #[test]
    fn test_select_where_like() {
        let expected = expected_values(
            "SELECT * FROM `naukio` WHERE `word` LIKE ? LIMIT -1",
            vec!["%meow%"],
        );

        let query = Select::from("naukio").so_that("word".like("meow"));
        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected.0, sql);
        assert_eq!(expected.1, params);
    }

    #[test]
    fn test_select_where_not_like() {
        let expected = expected_values(
            "SELECT * FROM `naukio` WHERE `word` NOT LIKE ? LIMIT -1",
            vec!["%meow%"],
        );

        let query = Select::from("naukio").so_that("word".not_like("meow"));
        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected.0, sql);
        assert_eq!(expected.1, params);
    }

    #[test]
    fn test_select_where_begins_with() {
        let expected = expected_values(
            "SELECT * FROM `naukio` WHERE `word` LIKE ? LIMIT -1",
            vec!["meow%"],
        );

        let query = Select::from("naukio").so_that("word".begins_with("meow"));
        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected.0, sql);
        assert_eq!(expected.1, params);
    }

    #[test]
    fn test_select_where_not_begins_with() {
        let expected = expected_values(
            "SELECT * FROM `naukio` WHERE `word` NOT LIKE ? LIMIT -1",
            vec!["meow%"],
        );

        let query = Select::from("naukio").so_that("word".not_begins_with("meow"));
        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected.0, sql);
        assert_eq!(expected.1, params);
    }

    #[test]
    fn test_select_where_ends_into() {
        let expected = expected_values(
            "SELECT * FROM `naukio` WHERE `word` LIKE ? LIMIT -1",
            vec!["%meow"],
        );

        let query = Select::from("naukio").so_that("word".ends_into("meow"));
        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected.0, sql);
        assert_eq!(expected.1, params);
    }

    #[test]
    fn test_select_where_not_ends_into() {
        let expected = expected_values(
            "SELECT * FROM `naukio` WHERE `word` NOT LIKE ? LIMIT -1",
            vec!["%meow"],
        );

        let query = Select::from("naukio").so_that("word".not_ends_into("meow"));
        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected.0, sql);
        assert_eq!(expected.1, params);
    }

    #[test]
    fn test_select_and() {
        let expected_sql =
            "SELECT * FROM `naukio` WHERE ((`word` = ? AND `age` < ?) AND `paw` = ?) LIMIT -1";

        let expected_params = vec![
            ParameterizedValue::Text(String::from("meow")),
            ParameterizedValue::Integer(10),
            ParameterizedValue::Text(String::from("warm")),
        ];

        let conditions = "word"
            .equals("meow")
            .and("age".less_than(10))
            .and("paw".equals("warm"));

        let query = Select::from("naukio").so_that(conditions);

        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected_sql, sql);
        assert_eq!(expected_params, params);
    }

    #[test]
    fn test_select_and_different_execution_order() {
        let expected_sql =
            "SELECT * FROM `naukio` WHERE (`word` = ? AND (`age` < ? AND `paw` = ?)) LIMIT -1";

        let expected_params = vec![
            ParameterizedValue::Text(String::from("meow")),
            ParameterizedValue::Integer(10),
            ParameterizedValue::Text(String::from("warm")),
        ];

        let conditions = "word"
            .equals("meow")
            .and("age".less_than(10).and("paw".equals("warm")));

        let query = Select::from("naukio").so_that(conditions);

        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected_sql, sql);
        assert_eq!(expected_params, params);
    }

    #[test]
    fn test_select_or() {
        let expected_sql =
            "SELECT * FROM `naukio` WHERE ((`word` = ? OR `age` < ?) AND `paw` = ?) LIMIT -1";

        let expected_params = vec![
            ParameterizedValue::Text(String::from("meow")),
            ParameterizedValue::Integer(10),
            ParameterizedValue::Text(String::from("warm")),
        ];

        let conditions = "word"
            .equals("meow")
            .or("age".less_than(10))
            .and("paw".equals("warm"));

        let query = Select::from("naukio").so_that(conditions);

        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected_sql, sql);
        assert_eq!(expected_params, params);
    }

    #[test]
    fn test_select_negation() {
        let expected_sql =
            "SELECT * FROM `naukio` WHERE (NOT ((`word` = ? OR `age` < ?) AND `paw` = ?)) LIMIT -1";

        let expected_params = vec![
            ParameterizedValue::Text(String::from("meow")),
            ParameterizedValue::Integer(10),
            ParameterizedValue::Text(String::from("warm")),
        ];

        let conditions = "word"
            .equals("meow")
            .or("age".less_than(10))
            .and("paw".equals("warm"))
            .not();

        let query = Select::from("naukio").so_that(conditions);

        let (sql, params) = Sqlite::build(query);

        assert_eq!(expected_sql, sql);
        assert_eq!(expected_params, params);
    }
}
