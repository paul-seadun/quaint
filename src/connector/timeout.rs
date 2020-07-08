use crate::error::Error;
use std::{future::Future, time::Duration};

pub(crate) async fn timeout<T, F, E>(duration: Option<Duration>, f: F) -> crate::Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: Into<Error>,
{
    match duration {
        Some(duration) => match tokio::time::timeout(duration, f).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => Err(err.into()),
            Err(to) => Err(to.into()),
        },
        None => match f.await {
            Ok(result) => Ok(result),
            Err(err) => Err(err.into()),
        },
    }
}
