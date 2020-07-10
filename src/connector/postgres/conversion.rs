use crate::{
    ast::Value,
    connector::bind::Bind,
    error::{Error, ErrorKind},
};
use ipnetwork::IpNetwork;
use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};
#[cfg(feature = "chrono-0_4")]
use sqlx::postgres::types::PgTimeTz;
use sqlx::{
    decode::Decode,
    postgres::{types::PgMoney, PgArguments, PgRow, PgTypeInfo},
    query::Query,
    types::Json,
    Postgres, Row, Type, TypeInfo, ValueRef,
};
use std::borrow::Cow;

impl<'a> Bind<'a, Postgres> for Query<'a, Postgres, PgArguments> {
    #[inline]
    fn bind_value(self, value: Value<'a>, type_info: Option<&PgTypeInfo>) -> crate::Result<Self> {
        let query = match (value, type_info.map(|ti| ti.name())) {
            // integers
            (Value::Integer(i), Some("INT2")) => self.bind(i.map(|i| i as i16)),
            (Value::Integer(i), Some("INT4")) => self.bind(i.map(|i| i as i32)),
            (Value::Integer(i), Some("OID")) => self.bind(i.map(|i| i as u32)),
            (Value::Integer(i), Some("TEXT")) => self.bind(i.map(|i| format!("{}", i))),
            (Value::Integer(i), _) => self.bind(i.map(|i| i as i32)),

            // floating and real
            (Value::Real(d), Some("FLOAT4")) => match d {
                Some(decimal) => {
                    let f = decimal.to_f32().ok_or_else(|| {
                        let kind = ErrorKind::conversion("Could not convert `Decimal` into `f32`.");
                        Error::builder(kind).build()
                    })?;

                    self.bind(f)
                }
                None => self.bind(Option::<f32>::None),
            },
            (Value::Real(d), Some("FLOAT8")) => match d {
                Some(decimal) => {
                    let f = decimal.to_f64().ok_or_else(|| {
                        let kind = ErrorKind::conversion("Could not convert `Decimal` into `f32`.");
                        Error::builder(kind).build()
                    })?;

                    self.bind(f)
                }
                None => self.bind(Option::<f64>::None),
            },
            (Value::Real(d), Some("MONEY")) => match d {
                Some(decimal) => self.bind(PgMoney::from_decimal(decimal, 2)),
                None => self.bind(Option::<PgMoney>::None),
            },
            (Value::Real(d), _) => self.bind(d),

            // strings
            #[cfg(feature = "ipnetwork")]
            (Value::Text(c), t) if t == Some("INET") || t == Some("CIDR") => match c {
                Some(s) => {
                    let ip: IpNetwork = s.parse().map_err(|_| {
                        let msg = format!("Provided IP address ({}) not in the right format.", s);
                        let kind = ErrorKind::conversion(msg);

                        Error::builder(kind).build()
                    })?;

                    self.bind(ip)
                }
                None => self.bind(Option::<IpNetwork>::None),
            },
            #[cfg(feature = "bit-vec")]
            (Value::Text(c), t) if t == Some("BIT") || t == Some("VARBIT") => match c {
                Some(s) => {
                    let bits = string_to_bits(&s)?;
                    self.bind(bits)
                }
                None => self.bind(Option::<IpNetwork>::None),
            },
            (Value::Text(c), _) => self.bind(c.map(|c| c.into_owned())),
            (Value::Enum(c), _) => self.bind(c.map(|c| c.into_owned())),

            (Value::Bytes(c), _) => self.bind(c.map(|c| c.into_owned())),
            (Value::Boolean(b), _) => self.bind(b),
            (Value::Char(c), _) => self.bind(c.map(|c| c as i8)),

            #[cfg(all(feature = "bit-vec", feature = "array"))]
            (Value::Array(ary_opt), t) if t == Some("BIT[]") || t == Some("VARBIT[]") => match ary_opt {
                Some(ary) => {
                    let mut bits = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.into_string()) {
                        match val {
                            Some(s) => {
                                let bit = string_to_bits(&s)?;
                                bits.push(bit);
                            }
                            None => {
                                let msg = "Non-string parameter when storing a BIT[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(bits)
                }
                None => self.bind(Option::<Vec<i16>>::None),
            },

            #[cfg(feature = "array")]
            (Value::Array(ary_opt), Some("INT2[]")) => match ary_opt {
                Some(ary) => {
                    let mut ints = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_i64().map(|i| i as i16)) {
                        match val {
                            Some(int) => {
                                ints.push(int);
                            }
                            None => {
                                let msg = "Non-integer parameter when storing an INT2[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(ints)
                }
                None => self.bind(Option::<Vec<i16>>::None),
            },

            #[cfg(feature = "array")]
            (Value::Array(ary_opt), Some("INT4[]")) => match ary_opt {
                Some(ary) => {
                    let mut ints = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_i64().map(|i| i as i32)) {
                        match val {
                            Some(int) => {
                                ints.push(int);
                            }
                            None => {
                                let msg = "Non-integer parameter when storing an INT4[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(ints)
                }
                None => self.bind(Option::<Vec<i32>>::None),
            },

            #[cfg(feature = "array")]
            (Value::Array(ary_opt), Some("INT8[]")) => match ary_opt {
                Some(ary) => {
                    let mut ints = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_i64()) {
                        match val {
                            Some(int) => {
                                ints.push(int);
                            }
                            None => {
                                let msg = "Non-integer parameter when storing an INT8[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(ints)
                }
                None => self.bind(Option::<Vec<i64>>::None),
            },

            #[cfg(feature = "array")]
            (Value::Array(ary_opt), Some("OID[]")) => match ary_opt {
                Some(ary) => {
                    let mut ints = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_i64().map(|i| i as u32)) {
                        match val {
                            Some(int) => {
                                ints.push(int);
                            }
                            None => {
                                let msg = "Non-integer parameter when storing an OID[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(ints)
                }
                None => self.bind(Option::<Vec<u32>>::None),
            },

            #[cfg(feature = "array")]
            (Value::Array(ary_opt), Some("FLOAT4[]")) => match ary_opt {
                Some(ary) => {
                    let mut floats = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_f64().map(|i| i as f32)) {
                        match val {
                            Some(float) => {
                                floats.push(float);
                            }
                            None => {
                                let msg = "Non-float parameter when storing a FLOAT4[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(floats)
                }
                None => self.bind(Option::<Vec<f32>>::None),
            },

            #[cfg(feature = "array")]
            (Value::Array(ary_opt), Some("FLOAT8[]")) => match ary_opt {
                Some(ary) => {
                    let mut floats = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_f64()) {
                        match val {
                            Some(float) => {
                                floats.push(float);
                            }
                            None => {
                                let msg = "Non-float parameter when storing a FLOAT8[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(floats)
                }
                None => self.bind(Option::<Vec<f64>>::None),
            },

            #[cfg(feature = "array")]
            (Value::Array(ary_opt), Some("BOOL[]")) => match ary_opt {
                Some(ary) => {
                    let mut boos = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_bool()) {
                        match val {
                            Some(boo) => {
                                boos.push(boo);
                            }
                            None => {
                                let msg = "Non-boolean parameter when storing a BOOL[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(boos)
                }
                None => self.bind(Option::<Vec<bool>>::None),
            },

            #[cfg(all(feature = "array", feature = "chrono-0_4"))]
            (Value::Array(ary_opt), Some("TIMESTAMPTZ[]")) => match ary_opt {
                Some(ary) => {
                    let mut vals = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_datetime()) {
                        match val {
                            Some(val) => {
                                vals.push(val);
                            }
                            None => {
                                let msg = "Non-datetime parameter when storing a TIMESTAMPTZ[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(vals)
                }
                None => self.bind(Option::<Vec<chrono::DateTime<chrono::Utc>>>::None),
            },

            #[cfg(all(feature = "array", feature = "chrono-0_4"))]
            (Value::Array(ary_opt), Some("TIMESTAMP[]")) => match ary_opt {
                Some(ary) => {
                    let mut vals = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_datetime()) {
                        match val {
                            Some(val) => {
                                vals.push(val.naive_utc());
                            }
                            None => {
                                let msg = "Non-datetime parameter when storing a TIMESTAMP[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(vals)
                }
                None => self.bind(Option::<Vec<chrono::NaiveDateTime>>::None),
            },

            #[cfg(all(feature = "array", feature = "chrono-0_4"))]
            (Value::Array(ary_opt), Some("DATE[]")) => match ary_opt {
                Some(ary) => {
                    let mut vals = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_date()) {
                        match val {
                            Some(val) => {
                                vals.push(val);
                            }
                            None => {
                                let msg = "Non-date parameter when storing a DATE[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(vals)
                }
                None => self.bind(Option::<Vec<chrono::NaiveDate>>::None),
            },

            #[cfg(all(feature = "array", feature = "chrono-0_4"))]
            (Value::Array(ary_opt), Some("TIME[]")) => match ary_opt {
                Some(ary) => {
                    let mut vals = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_time()) {
                        match val {
                            Some(val) => {
                                vals.push(val);
                            }
                            None => {
                                let msg = "Non-time parameter when storing a TIME[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(vals)
                }
                None => self.bind(Option::<Vec<chrono::NaiveTime>>::None),
            },

            #[cfg(all(feature = "array", feature = "chrono-0_4"))]
            (Value::Array(ary_opt), Some("TIMETZ[]")) => match ary_opt {
                Some(ary) => {
                    let mut vals = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_datetime()) {
                        match val {
                            Some(val) => {
                                let timetz = PgTimeTz::new(val.time(), chrono::FixedOffset::east(0));
                                vals.push(timetz);
                            }
                            None => {
                                let msg = "Non-time parameter when storing a TIMETZ[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(vals)
                }
                None => self.bind(Option::<Vec<PgTimeTz>>::None),
            },

            #[cfg(all(feature = "array", feature = "json-1"))]
            (Value::Array(ary_opt), Some("JSON[]")) => match ary_opt {
                Some(ary) => {
                    let mut vals = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.into_json()) {
                        match val {
                            Some(val) => {
                                vals.push(Json(val));
                            }
                            None => {
                                let msg = "Non-json parameter when storing a JSON[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(vals)
                }
                None => self.bind(Option::<Vec<Json<serde_json::Value>>>::None),
            },

            #[cfg(feature = "array")]
            (Value::Array(ary_opt), Some("CHAR[]")) => match ary_opt {
                Some(ary) => {
                    let mut vals = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_char()) {
                        match val {
                            Some(val) => {
                                vals.push(val as i8);
                            }
                            None => {
                                let msg = "Non-char parameter when storing a CHAR[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(vals)
                }
                None => self.bind(Option::<Vec<i8>>::None),
            },

            #[cfg(any(feature = "array", feature = "uuid-0_8"))]
            (Value::Array(ary_opt), Some("UUID[]")) => match ary_opt {
                Some(ary) => {
                    let mut vals = Vec::with_capacity(ary.len());

                    for val in ary.into_iter().map(|v| v.as_uuid()) {
                        match val {
                            Some(val) => {
                                vals.push(val);
                            }
                            None => {
                                let msg = "Non-uuid parameter when storing a UUID[]";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(vals)
                }
                None => self.bind(Option::<Vec<uuid::Uuid>>::None),
            },

            #[cfg(feature = "array")]
            (Value::Array(ary_opt), t) if t == Some("TEXT[]") || t == Some("VARCHAR[]") || t == Some("NAME[]") => {
                match ary_opt {
                    Some(ary) => {
                        let mut vals = Vec::with_capacity(ary.len());

                        for val in ary.into_iter().map(|v| v.into_string()) {
                            match val {
                                Some(val) => {
                                    vals.push(val);
                                }
                                None => {
                                    let msg = "Non-string parameter when storing a string array";
                                    let kind = ErrorKind::conversion(msg);

                                    Err(Error::builder(kind).build())?
                                }
                            }
                        }

                        self.bind(vals)
                    }
                    None => self.bind(Option::<Vec<uuid::Uuid>>::None),
                }
            }

            #[cfg(all(feature = "array", feature = "ipnetwork"))]
            (Value::Array(ary_opt), t) if t == Some("INET[]") || t == Some("CIDR[]") => match ary_opt {
                Some(ary) => {
                    let mut ips = Vec::with_capacity(ary.len());

                    for val in ary.into_iter() {
                        match val.into_string() {
                            Some(s) => {
                                let ip: IpNetwork = s.parse().map_err(|_| {
                                    let msg = format!("Provided IP address ({}) not in the right format.", s);
                                    let kind = ErrorKind::conversion(msg);

                                    Error::builder(kind).build()
                                })?;

                                ips.push(ip);
                            }
                            None => {
                                let msg = "Non-string parameter when storing an IP array";
                                let kind = ErrorKind::conversion(msg);

                                Err(Error::builder(kind).build())?
                            }
                        }
                    }

                    self.bind(ips)
                }
                None => self.bind(Option::<Vec<IpNetwork>>::None),
            },

            (Value::Array(_), t) => match t {
                Some(t) => {
                    let msg = format!("Postgres type {} not supported yet", t);
                    let kind = ErrorKind::conversion(msg);

                    Err(Error::builder(kind).build())?
                }
                None => {
                    let kind = ErrorKind::conversion("Untyped Postgres arrays are not supported");
                    Err(Error::builder(kind).build())?
                }
            },

            #[cfg(feature = "json-1")]
            (Value::Json(json), _) => self.bind(json.map(Json)),

            #[cfg(feature = "uuid-0_8")]
            (Value::Uuid(uuid), _) => self.bind(uuid),

            #[cfg(feature = "chrono-0_4")]
            (Value::DateTime(dt), Some("TIMETZ")) => {
                let time_tz = dt.map(|dt| PgTimeTz::new(dt.time(), chrono::FixedOffset::east(0)));

                self.bind(time_tz)
            }

            #[cfg(feature = "chrono-0_4")]
            (Value::DateTime(dt), Some("TIME")) => self.bind(dt.map(|dt| dt.time())),

            #[cfg(feature = "chrono-0_4")]
            (Value::DateTime(dt), Some("DATE")) => self.bind(dt.map(|dt| dt.date().naive_utc())),

            #[cfg(feature = "chrono-0_4")]
            (Value::DateTime(dt), _) => self.bind(dt),

            #[cfg(feature = "chrono-0_4")]
            (Value::Date(date), _) => self.bind(date),

            #[cfg(feature = "chrono-0_4")]
            (Value::Time(time), _) => self.bind(time),
        };

        Ok(query)
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

            ti if <String as Type<Postgres>>::compatible(ti)
                && (ti.name() == "TEXT" || ti.name() == "VARCHAR" || ti.name() == "NAME") =>
            {
                let string_opt: Option<String> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Text(string_opt.map(Cow::from))
            }

            ti if <String as Type<Postgres>>::compatible(ti) => {
                let string_opt: Option<String> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Enum(string_opt.map(Cow::from))
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

            #[cfg(feature = "uuid-0_8")]
            ti if <uuid::Uuid>::compatible(ti) => {
                let dt_opt = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Uuid(dt_opt)
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

            #[cfg(all(feature = "chrono-0_4", feature = "array"))]
            ti if <chrono::NaiveDateTime as Type<Postgres>>::compatible(ti) => {
                let naive: Option<chrono::NaiveDateTime> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;
                let dt = naive.map(|d| chrono::DateTime::<chrono::Utc>::from_utc(d, chrono::Utc));

                Value::DateTime(dt)
            }

            #[cfg(feature = "chrono-0_4")]
            ti if <sqlx::postgres::types::PgTimeTz as Type<Postgres>>::compatible(ti) => {
                let timetz_opt: Option<PgTimeTz> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                let dt_opt = timetz_opt.map(|time_tz| {
                    let (time, tz) = time_tz.into_parts();

                    let dt = chrono::NaiveDate::from_ymd(1970, 1, 1).and_time(time);
                    let dt = chrono::DateTime::<chrono::Utc>::from_utc(dt, chrono::Utc);
                    let dt = dt.with_timezone(&tz);

                    chrono::DateTime::from_utc(dt.naive_utc(), chrono::Utc)
                });

                Value::DateTime(dt_opt)
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

            // arrays from here on
            #[cfg(feature = "array")]
            ti if <Vec<i8> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<i8>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| ary.into_iter().map(Value::integer).collect()))
            }

            #[cfg(feature = "array")]
            ti if <Vec<i16> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<i16>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| ary.into_iter().map(Value::integer).collect()))
            }

            #[cfg(feature = "array")]
            ti if <Vec<i32> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<i32>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| ary.into_iter().map(Value::integer).collect()))
            }

            #[cfg(feature = "array")]
            ti if <Vec<i64> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<i64>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| ary.into_iter().map(Value::integer).collect()))
            }

            #[cfg(feature = "array")]
            ti if <Vec<u32> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<u32>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| ary.into_iter().map(Value::integer).collect()))
            }

            #[cfg(feature = "array")]
            ti if <Vec<PgMoney> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<PgMoney>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                // We assume the default setting of 2 decimals.
                let decs = ary_opt.map(|ary| {
                    ary.into_iter()
                        .map(|money| money.to_decimal(2))
                        .map(Value::real)
                        .collect()
                });

                Value::Array(decs)
            }

            #[cfg(feature = "array")]
            ti if <Vec<Decimal> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<Decimal>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;
                let decs = ary_opt.map(|ary| ary.into_iter().map(Value::real).collect());

                Value::Array(decs)
            }

            #[cfg(feature = "array")]
            ti if <Vec<f32> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<f32>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                let decs = ary_opt.map(|ary| {
                    ary.into_iter()
                        .map(|f| Decimal::from_f32(f).unwrap())
                        .map(Value::real)
                        .collect()
                });

                Value::Array(decs)
            }

            #[cfg(feature = "array")]
            ti if <Vec<f64> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<f64>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                let decs = ary_opt.map(|ary| {
                    ary.into_iter()
                        .map(|f| Decimal::from_f64(f).unwrap())
                        .map(Value::real)
                        .collect()
                });

                Value::Array(decs)
            }

            #[cfg(feature = "array")]
            ti if <Vec<String> as Type<Postgres>>::compatible(ti)
                && (ti.name() == "TEXT[]" || ti.name() == "VARCHAR[]" || ti.name() == "NAME[]") =>
            {
                let ary_opt: Option<Vec<String>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| ary.into_iter().map(Value::text).collect()))
            }

            #[cfg(feature = "array")]
            ti if <Vec<String> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<String>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| ary.into_iter().map(Value::enum_variant).collect()))
            }

            #[cfg(feature = "array")]
            ti if <Vec<bool> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<bool>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| ary.into_iter().map(Value::boolean).collect()))
            }

            #[cfg(feature = "array")]
            ti if <Vec<IpNetwork> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<IpNetwork>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;
                let strs = ary_opt.map(|ary| ary.into_iter().map(|ip| Value::text(format!("{}", ip))).collect());

                Value::Array(strs)
            }

            #[cfg(all(feature = "chrono-0_4", feature = "array"))]
            ti if <Vec<chrono::DateTime<chrono::Utc>> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<chrono::DateTime<chrono::Utc>>> =
                    Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| ary.into_iter().map(Value::datetime).collect()))
            }

            #[cfg(all(feature = "chrono-0_4", feature = "array"))]
            ti if <Vec<chrono::NaiveDate> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<chrono::NaiveDate>> =
                    Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| ary.into_iter().map(Value::date).collect()))
            }

            #[cfg(all(feature = "chrono-0_4", feature = "array"))]
            ti if <Vec<chrono::NaiveDateTime> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<chrono::NaiveDateTime>> =
                    Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| {
                    ary.into_iter()
                        .map(|d| chrono::DateTime::<chrono::Utc>::from_utc(d, chrono::Utc))
                        .map(Value::datetime)
                        .collect()
                }))
            }

            #[cfg(all(feature = "chrono-0_4", feature = "array"))]
            ti if <Vec<chrono::NaiveTime> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<chrono::NaiveTime>> =
                    Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                Value::Array(ary_opt.map(|ary| ary.into_iter().map(Value::time).collect()))
            }

            #[cfg(all(feature = "chrono-0_4", feature = "array"))]
            ti if <Vec<PgTimeTz> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<PgTimeTz>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                let dts = ary_opt.map(|ary| {
                    ary.into_iter()
                        .map(|time_tz| {
                            let (time, tz) = time_tz.into_parts();

                            let dt = chrono::NaiveDate::from_ymd(1970, 1, 1).and_time(time);
                            let dt = chrono::DateTime::<chrono::Utc>::from_utc(dt, chrono::Utc);
                            let dt = dt.with_timezone(&tz);

                            chrono::DateTime::from_utc(dt.naive_utc(), chrono::Utc)
                        })
                        .map(Value::datetime)
                        .collect()
                });

                Value::Array(dts)
            }

            #[cfg(all(feature = "json-1", feature = "array"))]
            ti if <Vec<Json<serde_json::Value>> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<Json<serde_json::Value>>> =
                    Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                let jsons = ary_opt.map(|ary| ary.into_iter().map(|j| Value::json(j.0)).collect());

                Value::Array(jsons)
            }

            #[cfg(all(feature = "bit-vec", feature = "array"))]
            ti if <Vec<bit_vec::BitVec> as Type<Postgres>>::compatible(ti) => {
                let ary_opt: Option<Vec<bit_vec::BitVec>> =
                    Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;

                let strs = ary_opt.map(|ary| ary.into_iter().map(bits_to_string).map(Value::text).collect());

                Value::Array(strs)
            }

            #[cfg(all(feature = "uuid-0_8", feature = "array"))]
            ti if <Vec<uuid::Uuid>>::compatible(ti) => {
                let ary_opt: Option<Vec<uuid::Uuid>> = Decode::<Postgres>::decode(value_ref).map_err(decode_err)?;
                let uuids = ary_opt.map(|ary| ary.into_iter().map(Value::uuid).collect());

                Value::Array(uuids)
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

#[cfg(feature = "bit-vec")]
fn string_to_bits(s: &str) -> crate::Result<bit_vec::BitVec> {
    let mut bits = bit_vec::BitVec::with_capacity(s.len());

    for c in s.chars() {
        match c {
            '0' => bits.push(false),
            '1' => bits.push(true),
            _ => {
                let msg = "Unexpected character for bits input. Expected only 1 and 0.";
                let kind = ErrorKind::conversion(msg);

                Err(Error::builder(kind).build())?
            }
        }
    }

    Ok(bits)
}
