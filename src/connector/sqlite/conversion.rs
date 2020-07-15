use crate::{
    ast::Value,
    connector::bind::Bind,
    error::{Error, ErrorKind},
};
use rust_decimal::prelude::ToPrimitive;
use sqlx::{
    query::Query,
    sqlite::{SqliteArguments, SqliteRow, SqliteTypeInfo},
    Column, Row, Sqlite, TypeInfo,
};
use std::{borrow::Cow, convert::TryFrom};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SqliteValue<'a> {
    /// 64-bit signed integer.
    Integer(Option<i64>),
    /// A decimal value.
    Real(Option<f64>),
    /// String value.
    Text(Option<Cow<'a, str>>),
    /// Bytes value.
    Bytes(Option<Cow<'a, [u8]>>),
    /// Boolean value.
    Boolean(Option<bool>),
}

impl<'a> Bind<'a, Sqlite> for Query<'a, Sqlite, SqliteArguments<'a>> {
    fn bind_value(self, value: Value<'a>, _: Option<&SqliteTypeInfo>) -> crate::Result<Self> {
        let query = match SqliteValue::try_from(value)? {
            SqliteValue::Integer(i) => self.bind(i),
            SqliteValue::Real(r) => self.bind(r),
            SqliteValue::Text(s) => self.bind(s.map(|s| s.into_owned())),
            SqliteValue::Bytes(b) => self.bind(b.map(|s| s.into_owned())),
            SqliteValue::Boolean(b) => self.bind(b),
        };

        Ok(query)
    }
}

impl<'a> TryFrom<Value<'a>> for SqliteValue<'a> {
    type Error = Error;

    fn try_from(v: Value<'a>) -> crate::Result<Self> {
        match v {
            Value::Integer(i) => Ok(Self::Integer(i)),
            Value::Real(r) => {
                let f = r.map(|r| r.to_f64().expect("Decimal is not f64"));
                Ok(Self::Real(f))
            }
            Value::Text(s) => Ok(Self::Text(s)),
            Value::Enum(e) => Ok(Self::Text(e)),
            Value::Bytes(b) => Ok(Self::Bytes(b)),
            Value::Boolean(b) => Ok(Self::Boolean(b)),
            Value::Char(c) => Ok(Self::Text(c.map(|c| c.to_string().into()))),
            #[cfg(all(feature = "array", feature = "postgresql"))]
            Value::Array(_) => {
                let msg = "Arrays are not supported in SQLite.";
                let kind = ErrorKind::conversion(msg);

                let mut builder = Error::builder(kind);
                builder.set_original_message(msg);

                Err(builder.build())?
            }
            #[cfg(feature = "json-1")]
            Value::Json(j) => {
                let s = j.map(|j| serde_json::to_string(&j).unwrap());
                let c = s.map(Cow::from);

                Ok(Self::Text(c))
            }
            #[cfg(feature = "uuid-0_8")]
            Value::Uuid(u) => Ok(Self::Text(u.map(|u| u.to_hyphenated().to_string().into()))),
            #[cfg(feature = "chrono-0_4")]
            Value::DateTime(d) => Ok(Self::Integer(d.map(|d| d.timestamp_millis()))),
            #[cfg(feature = "chrono-0_4")]
            Value::Date(date) => {
                let ts = date.map(|d| d.and_hms(0, 0, 0)).map(|dt| dt.timestamp_millis());
                Ok(Self::Integer(ts))
            }
            #[cfg(feature = "chrono-0_4")]
            Value::Time(t) => {
                use chrono::{NaiveDate, Timelike};

                let ts = t.map(|time| {
                    let date = NaiveDate::from_ymd(1970, 1, 1);
                    let dt = date.and_hms(time.hour(), time.minute(), time.second());
                    dt.timestamp_millis()
                });

                Ok(Self::Integer(ts))
            }
        }
    }
}

pub fn map_row<'a>(row: SqliteRow) -> Result<Vec<Value<'a>>, sqlx::Error> {
    let mut result = Vec::with_capacity(row.len());

    for i in 0..row.len() {
        let value = match row.columns()[i].type_info().name() {
            "INTEGER" => Value::Integer(row.get_unchecked(i)),

            "TEXT" => {
                let string_opt: Option<String> = row.get_unchecked(i);
                Value::Text(string_opt.map(Cow::from))
            }

            "REAL" => {
                let f: Option<f64> = row.get_unchecked(i);
                match f {
                    Some(f) => Value::from(f),
                    None => Value::Real(None),
                }
            }

            "BLOB" => {
                let bytes_opt: Option<Vec<u8>> = row.get_unchecked(i);
                Value::Bytes(bytes_opt.map(Cow::from))
            }

            "BOOLEAN" => {
                let bool_opt = row.get_unchecked(i);
                Value::Boolean(bool_opt)
            }

            "NULL" => Value::Integer(None),

            ti => {
                let msg = format!("Type {} is not yet supported in the SQLite connector.", ti);
                let kind = ErrorKind::conversion(msg.clone());

                let mut builder = Error::builder(kind);
                builder.set_original_message(msg);

                let error = sqlx::Error::ColumnDecode {
                    index: format!("{}", i),
                    source: Box::new(builder.build()),
                };

                Err(error)?
            }
        };

        result.push(value);
    }

    Ok(result)
}
