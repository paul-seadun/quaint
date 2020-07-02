use crate::ast::Value;

pub trait Bind<'a> {
    fn bind_value(self, value: Value<'a>) -> crate::Result<Self>
    where
        Self: Sized;
}
