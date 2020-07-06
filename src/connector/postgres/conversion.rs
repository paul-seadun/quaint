use crate::{
    ast::Value,
    connector::bind::Bind,
    error::{Error, ErrorKind},
};
use ipnetwork::IpNetwork;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use sqlx::{
    decode::Decode,
    encode::{Encode, IsNull},
    postgres::{types::PgMoney, PgArgumentBuffer, PgArguments, PgRow, PgTypeInfo},
    query::Query,
    types::Json,
    Postgres, Row, Type, TypeInfo, ValueRef,
};
use std::borrow::Cow;

impl<'a> Bind<'a> for Query<'a, Postgres, PgArguments> {
    #[inline]
    fn bind_value(self, value: Value<'a>) -> crate::Result<Self> {
        Ok(self.bind(value))
    }
}

impl<'a> Type<Postgres> for Value<'a> {
    fn type_info() -> PgTypeInfo {
        unreachable!()
    }
}

impl<'a> Encode<'a, Postgres> for Value<'a> {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
        match self {
            Self::Integer(i) => <Option<i64> as Encode<'_, Postgres>>::encode(*i, buf),

            Self::Real(d) => <Option<Decimal> as Encode<'_, Postgres>>::encode(*d, buf),

            Self::Text(c) => {
                <Option<&str> as Encode<'_, Postgres>>::encode_by_ref(&c.as_ref().map(|s| s.as_ref()), buf)
            }

            Self::Enum(c) => {
                <Option<&str> as Encode<'_, Postgres>>::encode_by_ref(&c.as_ref().map(|s| s.as_ref()), buf)
            }

            Self::Bytes(c) => {
                <Option<&[u8]> as Encode<'_, Postgres>>::encode_by_ref(&c.as_ref().map(|s| s.as_ref()), buf)
            }

            Self::Boolean(b) => <Option<bool> as Encode<'_, Postgres>>::encode(*b, buf),
            Self::Char(c) => <Option<i8> as Encode<'_, Postgres>>::encode(c.map(|c| c as i8), buf),

            #[cfg(feature = "array")]
            Self::Array(Some(ary)) => match ary.first() {
                Some(Self::Integer(_)) => {
                    let ints = ary.into_iter().map(|i| i.as_i64()).collect();
                    <Vec<Option<i64>> as Encode<'_, Postgres>>::encode(ints, buf)
                }

                Some(Self::Real(_)) => {
                    let decs = ary.into_iter().map(|i| i.as_decimal()).collect();
                    <Vec<Option<Decimal>> as Encode<'_, Postgres>>::encode(decs, buf)
                }

                Some(Self::Text(_)) | Some(Self::Enum(_)) => {
                    let strs = ary.into_iter().map(|s| s.as_str()).collect();
                    <Vec<Option<&str>> as Encode<'_, Postgres>>::encode(strs, buf)
                }

                Some(Self::Bytes(_)) => {
                    let slices = ary.into_iter().map(|s| s.as_bytes()).collect();
                    <Vec<Option<&[u8]>> as Encode<'_, Postgres>>::encode(slices, buf)
                }

                Some(Self::Boolean(_)) => {
                    let boos = ary.into_iter().map(|s| s.as_bool()).collect();
                    <Vec<Option<bool>> as Encode<'_, Postgres>>::encode(boos, buf)
                }

                Some(Self::Char(_)) => {
                    let chars = ary.into_iter().map(|s| s.as_char().map(|c| c as i8)).collect();
                    <Vec<Option<i8>> as Encode<'_, Postgres>>::encode(chars, buf)
                }

                #[cfg(feature = "json-1")]
                Some(Self::Json(_)) => {
                    let jsons = ary.into_iter().map(|v| v.as_json().map(Json)).collect();
                    <Vec<Option<Json<&serde_json::Value>>> as Encode<'_, Postgres>>::encode(jsons, buf)
                }
                #[cfg(feature = "uuid-0_8")]
                Some(Self::Uuid(_)) => {
                    let uuids = ary.into_iter().map(|v| v.as_uuid()).collect();
                    <Vec<Option<uuid::Uuid>> as Encode<'_, Postgres>>::encode(uuids, buf)
                }
                #[cfg(feature = "chrono-0_4")]
                Some(Self::DateTime(_)) => {
                    let dts = ary.into_iter().map(|v| v.as_datetime()).collect();
                    <Vec<Option<chrono::DateTime<chrono::Utc>>> as Encode<'_, Postgres>>::encode(dts, buf)
                }
                #[cfg(feature = "chrono-0_4")]
                Some(Self::Date(_)) => {
                    let dates = ary.into_iter().map(|v| v.as_date()).collect();
                    <Vec<Option<chrono::NaiveDate>> as Encode<'_, Postgres>>::encode(dates, buf)
                }
                #[cfg(feature = "chrono-0_4")]
                Some(Self::Time(_)) => {
                    let times = ary.into_iter().map(|v| v.as_time()).collect();
                    <Vec<Option<chrono::NaiveTime>> as Encode<'_, Postgres>>::encode(times, buf)
                }

                Some(Self::Array(_)) => unreachable!("Nested array should not be possible."),
                None => <Vec<Option<f64>> as Encode<'_, Postgres>>::encode(Vec::new(), buf),
            },

            #[cfg(feature = "array")]
            Self::Array(None) => <Option<Vec<Option<f64>>> as Encode<'_, Postgres>>::encode(None, buf),

            #[cfg(feature = "json-1")]
            Self::Json(json) => <Option<Json<&serde_json::Value>> as Encode<'_, Postgres>>::encode_by_ref(
                &json.as_ref().map(|j| Json(j)),
                buf,
            ),

            #[cfg(feature = "uuid-0_8")]
            Self::Uuid(uuid) => <Option<uuid::Uuid> as Encode<'_, Postgres>>::encode(*uuid, buf),

            #[cfg(feature = "chrono-0_4")]
            Self::DateTime(dt) => <Option<chrono::DateTime<chrono::Utc>> as Encode<'_, Postgres>>::encode(*dt, buf),

            #[cfg(feature = "chrono-0_4")]
            Self::Date(date) => <Option<chrono::NaiveDate> as Encode<'_, Postgres>>::encode(*date, buf),

            #[cfg(feature = "chrono-0_4")]
            Self::Time(time) => <Option<chrono::NaiveTime> as Encode<'_, Postgres>>::encode(*time, buf),
        }
    }

    fn produces(&self) -> Option<PgTypeInfo> {
        let ti = match self {
            Self::Integer(_) => <i64 as Type<Postgres>>::type_info(),
            Self::Real(_) => <Decimal as Type<Postgres>>::type_info(),
            Self::Text(_) => <String as Type<Postgres>>::type_info(),
            Self::Enum(_) => <String as Type<Postgres>>::type_info(),
            Self::Bytes(_) => <Vec<u8> as Type<Postgres>>::type_info(),
            Self::Boolean(_) => <bool as Type<Postgres>>::type_info(),
            Self::Char(_) => <i8 as Type<Postgres>>::type_info(),
            #[cfg(feature = "array")]
            Self::Array(Some(ary)) => match ary.first() {
                Some(Self::Integer(_)) => <[i64] as Type<Postgres>>::type_info(),
                Some(Self::Real(_)) => <[Decimal] as Type<Postgres>>::type_info(),
                Some(Self::Text(_)) => <[String] as Type<Postgres>>::type_info(),
                Some(Self::Enum(_)) => <[String] as Type<Postgres>>::type_info(),
                Some(Self::Bytes(_)) => <[Vec<u8>] as Type<Postgres>>::type_info(),
                Some(Self::Boolean(_)) => <[bool] as Type<Postgres>>::type_info(),
                Some(Self::Char(_)) => <[i8] as Type<Postgres>>::type_info(),
                Some(Self::Array(_)) => unreachable!("Nested array should not be possible."),
                #[cfg(feature = "json-1")]
                Some(Self::Json(_)) => <Vec<Json<serde_json::Value>> as Type<Postgres>>::type_info(),
                #[cfg(feature = "uuid-0_8")]
                Some(Self::Uuid(_)) => <Vec<uuid::Uuid> as Type<Postgres>>::type_info(),
                #[cfg(feature = "chrono-0_4")]
                Some(Self::DateTime(_)) => <Vec<chrono::DateTime<chrono::Utc>> as Type<Postgres>>::type_info(),
                #[cfg(feature = "chrono-0_4")]
                Some(Self::Date(_)) => <Vec<chrono::NaiveDate> as Type<Postgres>>::type_info(),
                #[cfg(feature = "chrono-0_4")]
                Some(Self::Time(_)) => <Vec<chrono::NaiveTime> as Type<Postgres>>::type_info(),
                None => <Vec<f64> as Type<Postgres>>::type_info(),
            },
            #[cfg(feature = "array")]
            Self::Array(None) => <Vec<f64> as Type<Postgres>>::type_info(),
            #[cfg(feature = "json-1")]
            Self::Json(_) => <serde_json::Value as Type<Postgres>>::type_info(),
            #[cfg(feature = "uuid-0_8")]
            Self::Uuid(_) => <uuid::Uuid as Type<Postgres>>::type_info(),
            #[cfg(feature = "chrono-0_4")]
            Self::DateTime(_) => <chrono::DateTime<chrono::Utc> as Type<Postgres>>::type_info(),
            #[cfg(feature = "chrono-0_4")]
            Self::Date(_) => <chrono::NaiveDate as Type<Postgres>>::type_info(),
            #[cfg(feature = "chrono-0_4")]
            Self::Time(_) => <chrono::NaiveTime as Type<Postgres>>::type_info(),
        };

        Some(ti)
    }
}

pub fn map_row<'a>(row: PgRow) -> Result<Vec<Value<'a>>, sqlx::Error> {
    let mut result = Vec::with_capacity(row.len());

    for i in 0..row.len() {
        let value_ref = row.try_get_raw(i)?;

        let decode_err = |source| sqlx::Error::ColumnDecode {
            index: format!("{}", i),
            source,
        };

        let value = match value_ref.type_info() {
            // Singular types from here down, arrays after these.
            ti if <i8 as Type<Postgres>>::compatible(ti) => {
                let int_opt: Option<i8> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Integer(int_opt.map(|i| i as i64))
            }

            ti if <i16 as Type<Postgres>>::compatible(ti) => {
                let int_opt: Option<i16> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Integer(int_opt.map(|i| i as i64))
            }

            ti if <i32 as Type<Postgres>>::compatible(ti) => {
                let int_opt: Option<i32> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Integer(int_opt.map(|i| i as i64))
            }

            ti if <i64 as Type<Postgres>>::compatible(ti) => {
                let int_opt = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Integer(int_opt)
            }

            ti if <u32 as Type<Postgres>>::compatible(ti) => {
                let int_opt: Option<u32> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Integer(int_opt.map(|i| i as i64))
            }

            ti if <PgMoney as Type<Postgres>>::compatible(ti) => {
                let money_opt: Option<PgMoney> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                // We assume the default setting of 2 decimals.
                let decimal_opt = money_opt.map(|money| money.to_decimal(2));

                Value::Real(decimal_opt)
            }

            ti if <Decimal as Type<Postgres>>::compatible(ti) => {
                let decimal_opt = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Real(decimal_opt)
            }

            ti if <f32 as Type<Postgres>>::compatible(ti) => {
                let f_opt: Option<f32> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Real(f_opt.map(|f| Decimal::from_f32(f).unwrap()))
            }

            ti if <f64 as Type<Postgres>>::compatible(ti) => {
                let f_opt: Option<f64> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Real(f_opt.map(|f| Decimal::from_f64(f).unwrap()))
            }

            ti if <String as Type<Postgres>>::compatible(ti) && ti.name() != "TEXT" => {
                let string_opt: Option<String> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Enum(string_opt.map(Cow::from))
            }

            ti if <String as Type<Postgres>>::compatible(ti) => {
                let string_opt: Option<String> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Text(string_opt.map(Cow::from))
            }

            ti if <Vec<u8> as Type<Postgres>>::compatible(ti) => {
                let bytes_opt: Option<Vec<u8>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Bytes(bytes_opt.map(Cow::from))
            }

            ti if <bool as Type<Postgres>>::compatible(ti) => {
                let bool_opt = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Boolean(bool_opt)
            }

            ti if <IpNetwork as Type<Postgres>>::compatible(ti) => {
                let ip_opt: Option<IpNetwork> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Text(ip_opt.map(|ip| format!("{}", ip)).map(Cow::from))
            }

            #[cfg(feature = "chrono-0_4")]
            ti if <chrono::DateTime<chrono::Utc> as Type<Postgres>>::compatible(ti) => {
                let dt_opt = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::DateTime(dt_opt)
            }

            #[cfg(feature = "chrono-0_4")]
            ti if <chrono::NaiveDate as Type<Postgres>>::compatible(ti) => {
                let date_opt = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Date(date_opt)
            }

            #[cfg(feature = "chrono-0_4")]
            ti if <chrono::NaiveTime as Type<Postgres>>::compatible(ti) => {
                let time_opt = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Time(time_opt)
            }

            #[cfg(feature = "json-1")]
            ti if <serde_json::Value as Type<Postgres>>::compatible(ti) => {
                let json_opt = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Json(json_opt)
            }

            #[cfg(feature = "bit-vec")]
            ti if <bit_vec::BitVec as Type<Postgres>>::compatible(ti) => {
                let bit_opt: Option<bit_vec::BitVec> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Text(bit_opt.map(bits_to_string).map(Cow::from))
            }

            ti => {
                let msg = format!("Type {} is not yet supported in the PostgreSQL connector.", ti.name());
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

#[cfg(feature = "bit-vec")]
fn bits_to_string(bits: bit_vec::BitVec) -> String {
    let mut s = String::with_capacity(bits.len());

    for bit in bits {
        if bit {
            s.push('1');
        } else {
            s.push('0');
        }
    }

    s
}
