use rand::RngCore;

use super::super::*;
use super::*;

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

#[derive(PartialEq)]
pub(crate) enum RetryRule {
    Normal,  // For ABORTED
    Backoff, // For DEADLINE_EXCEEDED, UNAVAILABLE
    Once,    // For INTERNAL
    Never,   // For RESOURCE_EXHAUSTED and others
}

fn get_obj<'a>(
    value: &'a serde_json::Value,
    key: &str,
) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    if let serde_json::Value::Object(obj) = value {
        if let Some(serde_json::Value::Object(val)) = obj.get(key) {
            Some(val)
        } else {
            None
        }
    } else {
        None
    }
}

impl RetryRule {
    pub(crate) fn based_on_error(error: &google_datastore1::Error) -> Self {
        if let google_datastore1::Error::BadRequest(value) = error {
            if let Some(serde_json::Value::String(status)) =
                get_obj(value, "error").and_then(|obj| obj.get("status"))
            {
                match status.as_str() {
                    "ABORTED" => Self::Normal,
                    "DEADLINE_EXCEEDED" | "UNAVAILABLE" => RetryRule::Backoff,
                    "INTERNAL" => Self::Once,
                    "RESOURCE_EXHAUSTED" =>
                    // "RESOURCE_EXHAUSTED" could be retried if it's a capacity issue
                    // and not a quota issue, but I have no way of figuring that out
                    // This is also a catch-all for anything we haven't seen yet, it
                    // seems best not to retry
                    {
                        Self::Never
                    }
                    _ => Self::Never,
                }
            } else {
                Self::Never
            }
        } else {
            Self::Never
        }
    }
}

/// The configuration and body for a single Datastore transaction.
///
/// T is the return type of the transaction body.
/// F is the type of the transaction body closure.
/// Fut is the type of the Future returned by the closure.
pub struct Transaction<'a> {
    pub retry_count: u32,
    pub first_retry: Duration,
    pub ds: &'a DatastoreShell,
}

impl<'a> Transaction<'a> {
    pub fn new(ds: &'a DatastoreShell) -> Self {
        Self {
            retry_count: 16,
            first_retry: Duration::from_millis(25),
            ds,
        }
    }

    pub fn with_retry_count(mut self, retry_count: u32) -> Self {
        self.retry_count = retry_count;
        self
    }

    pub fn first_retry(mut self, first_retry: Duration) -> Self {
        self.first_retry = first_retry;
        self
    }

    pub async fn run<T, F>(self, mut body: F) -> Result<T, EntailError>
    where
        F: for<'b> FnMut(
            &'b mut TransactionShell,
        )
            -> Pin<Box<dyn Future<Output = Result<T, EntailError>> + Send + 'b>>,
        T: Send,
    {
        let mut retries_left = self.retry_count;
        let mut last_error: Option<google_datastore1::Error> = None;
        let mut last_txn: Option<Vec<u8>> = None;
        let mut current_delay = self.first_retry;
        let mut rng = rand::rng();
        loop {
            if retries_left == 0 {
                return Err(EntailError {
                    message: "Retries exhausted".into(),
                    ds_error: last_error,
                });
            }
            retries_left -= 1;
            let mut this_txn = TransactionShell::from(self.ds.begin_transaction(&last_txn).await?);
            last_txn = this_txn.ds.transaction.clone();
            let result = body(&mut this_txn).await;
            match result {
                Ok(result) => {
                    if this_txn.active {
                        this_txn.rollback().await?;
                    }
                    return Ok(result);
                }
                Err(err) => {
                    if this_txn.active {
                        if this_txn.rollback().await.is_err() {
                            return Err(EntailError {
                                message: "Autorollback error".into(),
                                ..err
                            });
                        }
                    }
                    let retry = if let Some(raw_error) = err.ds_error.as_ref() {
                        RetryRule::based_on_error(raw_error)
                    } else {
                        RetryRule::Never
                    };
                    match retry {
                        RetryRule::Backoff | RetryRule::Normal => {
                            let backoff = retry == RetryRule::Backoff;
                            let next_delay = if backoff {
                                current_delay.checked_mul(2).unwrap_or(current_delay)
                            } else {
                                current_delay
                            };
                            let min =
                                (current_delay.as_micros() >> if backoff { 0 } else { 1 }) as u64;
                            let max = next_delay.as_micros() as u64;
                            let val = if max > min {
                                rng.next_u64() % (max - min) + min
                            } else {
                                max
                            };
                            tokio::time::sleep(Duration::from_micros(val)).await;
                            current_delay = next_delay;
                        }
                        RetryRule::Once => {
                            if retries_left > 0 {
                                retries_left = 1;
                            }
                        }
                        RetryRule::Never => {
                            return Err(err);
                        }
                    };
                    last_error = err.ds_error;
                }
            }
        }
    }
}
