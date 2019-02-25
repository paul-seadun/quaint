use crate::ast::{Column, ConditionTree, Conjuctive, DatabaseValue, Expression, Row};

#[derive(Debug, Clone, PartialEq)]
pub enum Compare {
    Equals(Box<DatabaseValue>, Box<DatabaseValue>),
    NotEquals(Box<DatabaseValue>, Box<DatabaseValue>),
    LessThan(Box<DatabaseValue>, Box<DatabaseValue>),
    LessThanOrEquals(Box<DatabaseValue>, Box<DatabaseValue>),
    GreaterThan(Box<DatabaseValue>, Box<DatabaseValue>),
    GreaterThanOrEquals(Box<DatabaseValue>, Box<DatabaseValue>),

    In(Box<DatabaseValue>, Box<Row>),

    NotIn(Box<DatabaseValue>, Box<Row>),
    Like(Box<DatabaseValue>, String),
    NotLike(Box<DatabaseValue>, String),
    BeginsWith(Box<DatabaseValue>, String),
    NotBeginsWith(Box<DatabaseValue>, String),
    EndsInto(Box<DatabaseValue>, String),
    NotEndsInto(Box<DatabaseValue>, String),

    Null(Box<DatabaseValue>),
    NotNull(Box<DatabaseValue>),
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

pub trait Comparable {
    fn equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    fn not_equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    fn less_than<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    fn less_than_or_equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    fn greater_than<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    fn greater_than_or_equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>;

    fn in_selection<T>(self, selection: Vec<T>) -> Compare
    where
        T: Into<DatabaseValue>;

    fn not_in_selection<T>(self, selection: Vec<T>) -> Compare
    where
        T: Into<DatabaseValue>;

    fn like<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    fn not_like<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    fn begins_with<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    fn not_begins_with<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    fn ends_into<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    fn not_ends_into<T>(self, pattern: T) -> Compare
    where
        T: Into<String>;

    fn is_null(self) -> Compare;
    fn is_not_null(self) -> Compare;
}

impl Comparable for DatabaseValue {
    #[inline]
    fn equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>,
    {
        Compare::Equals(Box::new(self), Box::new(comparison.into()))
    }

    #[inline]
    fn not_equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>,
    {
        Compare::NotEquals(Box::new(self), Box::new(comparison.into()))
    }

    #[inline]
    fn less_than<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>,
    {
        Compare::LessThan(Box::new(self), Box::new(comparison.into()))
    }

    #[inline]
    fn less_than_or_equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>,
    {
        Compare::LessThanOrEquals(Box::new(self), Box::new(comparison.into()))
    }

    #[inline]
    fn greater_than<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>,
    {
        Compare::GreaterThan(Box::new(self), Box::new(comparison.into()))
    }

    #[inline]
    fn greater_than_or_equals<T>(self, comparison: T) -> Compare
    where
        T: Into<DatabaseValue>,
    {
        Compare::GreaterThanOrEquals(Box::new(self), Box::new(comparison.into()))
    }

    #[inline]
    fn in_selection<T>(self, selection: Vec<T>) -> Compare
    where
        T: Into<DatabaseValue>,
    {
        Compare::In(Box::new(self), Box::new(Row::from(selection).into()))
    }

    #[inline]
    fn not_in_selection<T>(self, selection: Vec<T>) -> Compare
    where
        T: Into<DatabaseValue>,
    {
        Compare::NotIn(Box::new(self), Box::new(Row::from(selection).into()))
    }

    #[inline]
    fn like<T>(self, pattern: T) -> Compare
    where
        T: Into<String>,
    {
        Compare::Like(Box::new(self), pattern.into())
    }

    #[inline]
    fn not_like<T>(self, pattern: T) -> Compare
    where
        T: Into<String>,
    {
        Compare::NotLike(Box::new(self), pattern.into())
    }

    #[inline]
    fn begins_with<T>(self, pattern: T) -> Compare
    where
        T: Into<String>,
    {
        Compare::BeginsWith(Box::new(self), pattern.into())
    }

    #[inline]
    fn not_begins_with<T>(self, pattern: T) -> Compare
    where
        T: Into<String>,
    {
        Compare::NotBeginsWith(Box::new(self), pattern.into())
    }

    #[inline]
    fn ends_into<T>(self, pattern: T) -> Compare
    where
        T: Into<String>,
    {
        Compare::EndsInto(Box::new(self), pattern.into())
    }

    #[inline]
    fn not_ends_into<T>(self, pattern: T) -> Compare
    where
        T: Into<String>,
    {
        Compare::NotEndsInto(Box::new(self), pattern.into())
    }

    #[inline]
    fn is_null(self) -> Compare {
        Compare::Null(Box::new(self))
    }

    #[inline]
    fn is_not_null(self) -> Compare {
        Compare::NotNull(Box::new(self))
    }
}

#[macro_export]
macro_rules! comparable {
    ($($kind:ty),*) => (
        $(
            impl Comparable for $kind {
                #[inline]
                fn equals<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.equals(comparison)
                }

                #[inline]
                fn not_equals<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.not_equals(comparison)
                }

                #[inline]
                fn less_than<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.less_than(comparison)
                }

                #[inline]
                fn less_than_or_equals<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.less_than_or_equals(comparison)
                }

                #[inline]
                fn greater_than<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.greater_than(comparison)
                }

                #[inline]
                fn greater_than_or_equals<T>(self, comparison: T) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.greater_than_or_equals(comparison)
                }

                #[inline]
                fn in_selection<T>(self, selection: Vec<T>) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.in_selection(selection)
                }

                #[inline]
                fn not_in_selection<T>(self, selection: Vec<T>) -> Compare
                where
                    T: Into<DatabaseValue>,
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.not_in_selection(selection)
                }

                #[inline]
                fn like<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.like(pattern)
                }

                #[inline]
                fn not_like<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.not_like(pattern)
                }

                #[inline]
                fn begins_with<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.begins_with(pattern)
                }

                #[inline]
                fn not_begins_with<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.not_begins_with(pattern)
                }

                #[inline]
                fn ends_into<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.ends_into(pattern)
                }

                #[inline]
                fn not_ends_into<T>(self, pattern: T) -> Compare
                where
                    T: Into<String>
                {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.not_ends_into(pattern)
                }

                #[inline]
                fn is_null(self) -> Compare {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.is_null()
                }

                #[inline]
                fn is_not_null(self) -> Compare {
                    let col: Column = self.into();
                    let val: DatabaseValue = col.into();
                    val.is_not_null()
                }
            }
        )*
    );
}

comparable!(&str, (&str, &str, &str), (&str, &str));
