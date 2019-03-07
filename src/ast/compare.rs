use crate::ast::{Column, ConditionTree, Conjuctive, DatabaseValue, Expression};

/// For modeling comparison expression
#[derive(Debug, Clone, PartialEq)]
pub enum Compare {
    /// `left = right`
    Equals(Box<DatabaseValue>, Box<DatabaseValue>),
    /// `left <> right`
    NotEquals(Box<DatabaseValue>, Box<DatabaseValue>),
    /// `left < right`
    LessThan(Box<DatabaseValue>, Box<DatabaseValue>),
    /// `left <= right`
    LessThanOrEquals(Box<DatabaseValue>, Box<DatabaseValue>),
    /// `left > right`
    GreaterThan(Box<DatabaseValue>, Box<DatabaseValue>),
    /// `left >= right`
    GreaterThanOrEquals(Box<DatabaseValue>, Box<DatabaseValue>),
    /// `left IN (..)`
    In(Box<DatabaseValue>, Box<DatabaseValue>),
    /// `left NOT IN (..)`
    NotIn(Box<DatabaseValue>, Box<DatabaseValue>),
    /// `left LIKE %..%`
    Like(Box<DatabaseValue>, String),
    /// `left NOT LIKE %..%`
    NotLike(Box<DatabaseValue>, String),
    /// `left LIKE ..%`
    BeginsWith(Box<DatabaseValue>, String),
    /// `left NOT LIKE ..%`
    NotBeginsWith(Box<DatabaseValue>, String),
    /// `left LIKE %..`
    EndsInto(Box<DatabaseValue>, String),
    /// `left NOT LIKE %..`
    NotEndsInto(Box<DatabaseValue>, String),
    /// `value IS NULL`
    Null(Box<DatabaseValue>),
    /// `value IS NOT NULL`
    NotNull(Box<DatabaseValue>),
    /// `value` BETWEEN `left` AND `right`
    Between(Box<DatabaseValue>, Box<DatabaseValue>, Box<DatabaseValue>),
    /// `value` NOT BETWEEN `left` AND `right`
    NotBetween(Box<DatabaseValue>, Box<DatabaseValue>, Box<DatabaseValue>),
}

impl Into<ConditionTree> for Compare {
    fn into(self) -> ConditionTree {
        let expression: Expression = self.into();
        ConditionTree::single(expression)
    }
}

impl Into<Expression> for Compare {
    fn into(self) -> Expression {
        Expression::Compare(self)
    }
}

impl Conjuctive for Compare {
    fn and<E>(self, other: E) -> ConditionTree
    where
        E: Into<Expression>,
    {
        let left: Expression = self.into();
        let right: Expression = other.into();

        ConditionTree::and(left, right)
    }

    fn or<E>(self, other: E) -> ConditionTree
    where
        E: Into<Expression>,
    {
        let left: Expression = self.into();
        let right: Expression = other.into();

        ConditionTree::or(left, right)
    }

    fn not(self) -> ConditionTree {
        ConditionTree::not(self)
    }
}

/// An item that can be compared against other values in the database.
pub trait Comparable {
    /// Tests if both sides are the same value.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".equals("bar"));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` = ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Text("bar".to_string())], params);
    /// ```
    fn equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    /// Tests if both sides are not the same value.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".not_equals("bar"));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` <> ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Text("bar".to_string())], params);
    /// ```
    fn not_equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    /// Tests if the left side is smaller than the right side.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".less_than(10));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` < ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Integer(10)], params);
    /// ```
    fn less_than<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    /// Tests if the left side is smaller than the right side or the same.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".less_than_or_equals(10));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` <= ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Integer(10)], params);
    /// ```
    fn less_than_or_equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    /// Tests if the left side is bigger than the right side.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".greater_than(10));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` > ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Integer(10)], params);
    /// ```
    fn greater_than<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    /// Tests if the left side is bigger than the right side or the same.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".greater_than_or_equals(10));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` >= ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Integer(10)], params);
    /// ```
    fn greater_than_or_equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    /// Tests if the left side is included in the right side collection.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".in_selection(vec![1, 2]));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` IN (?, ?) LIMIT -1", sql);
    /// assert_eq!(vec![
    ///     ParameterizedValue::Integer(1),
    ///     ParameterizedValue::Integer(2),
    /// ], params);
    /// ```
    fn in_selection<T>(self, selection: T) -> Compare
    where
        T: Into<DatabaseValue>;

    /// Tests if the left side is not included in the right side collection.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".not_in_selection(vec![1, 2]));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` NOT IN (?, ?) LIMIT -1", sql);
    /// assert_eq!(vec![
    ///     ParameterizedValue::Integer(1),
    ///     ParameterizedValue::Integer(2),
    /// ], params);
    /// ```
    fn not_in_selection<T>(self, selection: T) -> Compare
    where
        T: Into<DatabaseValue>;

    /// Tests if the left side includes the right side string.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".like("bar"));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` LIKE ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Text("%bar%".to_string())], params);
    /// ```
    fn like<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    /// Tests if the left side does not include the right side string.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".not_like("bar"));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` NOT LIKE ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Text("%bar%".to_string())], params);
    /// ```
    fn not_like<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    /// Tests if the left side starts with the right side string.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".begins_with("bar"));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` LIKE ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Text("bar%".to_string())], params);
    /// ```
    fn begins_with<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    /// Tests if the left side doesn't start with the right side string.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".not_begins_with("bar"));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` NOT LIKE ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Text("bar%".to_string())], params);
    /// ```
    fn not_begins_with<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    /// Tests if the left side ends into the right side string.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".ends_into("bar"));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` LIKE ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Text("%bar".to_string())], params);
    /// ```
    fn ends_into<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    /// Tests if the left side does not end into the right side string.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".not_ends_into("bar"));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` NOT LIKE ? LIMIT -1", sql);
    /// assert_eq!(vec![ParameterizedValue::Text("%bar".to_string())], params);
    /// ```
    fn not_ends_into<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    /// Tests if the left side is `NULL`.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".is_null());
    /// let (sql, _) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` IS NULL LIMIT -1", sql);
    /// ```
    fn is_null(self) -> Compare;

    /// Tests if the left side is not `NULL`.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".is_not_null());
    /// let (sql, _) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` IS NOT NULL LIMIT -1", sql);
    /// ```
    fn is_not_null(self) -> Compare;

    /// Tests if the value is between two given values.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".between(420, 666));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` BETWEEN ? AND ? LIMIT -1", sql);
    ///
    /// assert_eq!(vec![
    ///     ParameterizedValue::Integer(420),
    ///     ParameterizedValue::Integer(666)
    /// ], params);
    /// ```
    fn between<T, V>(self, left: T, right: V) -> Compare
    where
        T: Into<DatabaseValue>,
        V: Into<DatabaseValue>;

    /// Tests if the value is not between two given values.
    ///
    /// ```rust
    /// # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}};
    /// let query = Select::from("users").so_that("foo".not_between(420, 666));
    /// let (sql, params) = Sqlite::build(query);
    ///
    /// assert_eq!("SELECT `users`.* FROM `users` WHERE `foo` NOT BETWEEN ? AND ? LIMIT -1", sql);
    ///
    /// assert_eq!(vec![
    ///     ParameterizedValue::Integer(420),
    ///     ParameterizedValue::Integer(666)
    /// ], params);
    /// ```
    fn not_between<T, V>(self, left: T, right: V) -> Compare
    where
        T: Into<DatabaseValue>,
        V: Into<DatabaseValue>;
}

#[macro_export]
macro_rules! comparable {
    ($($kind:ty),*) => (
        $(
            impl Comparable for $kind {
                fn equals<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.equals(comparison)
                }

                fn not_equals<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.not_equals(comparison)
                }

                fn less_than<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.less_than(comparison)
                }

                fn less_than_or_equals<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.less_than_or_equals(comparison)
                }

                fn greater_than<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.greater_than(comparison)
                }

                fn greater_than_or_equals<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.greater_than_or_equals(comparison)
                }

                fn in_selection<T>(self, selection: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.in_selection(selection)
                }

                fn not_in_selection<T>(self, selection: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.not_in_selection(selection)
                }

                fn like<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.like(pattern)
                }

                fn not_like<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.not_like(pattern)
                }

                fn begins_with<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.begins_with(pattern)
                }

                fn not_begins_with<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.not_begins_with(pattern)
                }

                fn ends_into<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.ends_into(pattern)
                }

                fn not_ends_into<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.not_ends_into(pattern)
                }

                fn is_null(self) -> Compare {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.is_null()
                }

                fn is_not_null(self) -> Compare {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.is_not_null()
                }

                fn between<T, V>(self, left: T, right: V) -> Compare
                where
                    T: Into<DatabaseValue>,
                    V: Into<DatabaseValue>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.between(left, right)
                }

                fn not_between<T, V>(self, left: T, right: V) -> Compare
                where
                    T: Into<DatabaseValue>,
                    V: Into<DatabaseValue>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.not_between(left, right)
                }
            }
        )*
    );
}

comparable!(&str, (&str, &str, &str), (&str, &str));
comparable!(String, (String, String, String), (String, String));