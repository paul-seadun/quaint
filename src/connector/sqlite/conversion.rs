use crate::{
    ast::Value,
    connector::bind::Bind,
    error::{Error, ErrorKind},
};
use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};
use sqlx::{
    decode::Decode,
    query::Query,
    sqlite::{SqliteArguments, SqliteRow, SqliteTypeInfo},
    Row, Sqlite, Type, TypeInfo, ValueRef,
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
        let value_ref = row.try_get_raw(i)?;

        let decode_err = |source| sqlx::Error::ColumnDecode {
            index: format!("{}", i),
            source,
        };

        let value = match value_ref.type_info() {
            #[cfg(feature = "chrono-0_4")]
            ti if <i64 as Type<Sqlite>>::compatible(ti) && ti.name() == "DATETIME" => {
                let int_opt: Option<i64> = Decode::<Sqlite>::decode(value_ref).map_err(decode_err)?;

                let dt = int_opt.map(|ts| {
                    let nsecs = ((ts % 1000) * 1_000_000) as u32;
                    let secs = (ts / 1000) as i64;
                    let naive = chrono::NaiveDateTime::from_timestamp(secs, nsecs);
                    chrono::DateTime::from_utc(naive, chrono::Utc)
                });

                Value::DateTime(dt)
            }

            #[cfg(feature = "chrono-0_4")]
            ti if <String as Type<Sqlite>>::compatible(ti) && ti.name() == "DATETIME" => {
                let str_opt: Option<String> = Decode::<Sqlite>::decode(value_ref).map_err(decode_err)?;

                match str_opt {
                    Some(dt_string) => {
                        let dt = chrono::DateTime::parse_from_rfc3339(dt_string.as_str())
                            .or_else(|_| chrono::DateTime::parse_from_rfc2822(dt_string.as_str()))
                            .or_else(|_| chrono::DateTime::parse_from_str(dt_string.as_str(), "%Y-%m-%d %H:%M:%S.%f"))
                            .map_err(|_| {
                                let msg = "Could not parse a DateTime string from SQLite.";
                                let kind = ErrorKind::conversion(msg);

                                let mut builder = Error::builder(kind);
                                builder.set_original_message(msg);

                                sqlx::Error::ColumnDecode {
                                    index: format!("{}", i),
                                    source: Box::new(builder.build()),
                                }
                            })?;

                        Value::datetime(dt.with_timezone(&chrono::Utc))
                    }
                    None => Value::DateTime(None),
                }
            }

            ti if <i64 as Type<Sqlite>>::compatible(ti) => {
                let int_opt = Decode::<Sqlite>::decode(value_ref).map_err(decode_err)?;

                Value::Integer(int_opt)
            }

            ti if <f32 as Type<Sqlite>>::compatible(ti) => {
                let f_opt: Option<f32> = Decode::<Sqlite>::decode(value_ref).map_err(decode_err)?;

                Value::Real(f_opt.map(|f| Decimal::from_f32(f).unwrap()))
            }

            ti if <f64 as Type<Sqlite>>::compatible(ti) => {
                let f_opt: Option<f64> = Decode::<Sqlite>::decode(value_ref).map_err(decode_err)?;

                Value::Real(f_opt.map(|f| Decimal::from_f64(f).unwrap()))
            }

            ti if <String as Type<Sqlite>>::compatible(ti) => {
                let string_opt: Option<String> = Decode::<Sqlite>::decode(value_ref).map_err(decode_err)?;

                Value::Text(string_opt.map(Cow::from))
            }

            ti if <Vec<u8> as Type<Sqlite>>::compatible(ti) => {
                let bytes_opt: Option<Vec<u8>> = Decode::<Sqlite>::decode(value_ref).map_err(decode_err)?;

                Value::Bytes(bytes_opt.map(Cow::from))
            }

            ti if <bool as Type<Sqlite>>::compatible(ti) => {
                let bool_opt = Decode::<Sqlite>::decode(value_ref).map_err(decode_err)?;

                Value::Boolean(bool_opt)
            }

            ti if ti.name() == "NULL" => Value::Integer(None),

            ti => {
                let msg = format!("Type {} is not yet supported in the SQLite connector.", ti.name());
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
