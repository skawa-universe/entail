use rand::RngCore;

use super::super::*;
use super::*;

use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::time::Duration;

/// A wrapper around a transactional [`DatastoreShell`] instance.
///
/// This shell is specifically created by calling [`DatastoreShell::begin_transaction`]
/// and is tied to a single, ongoing Datastore transaction. It automatically tracks
/// whether the transaction has been successfully committed or rolled back via the
/// internal `active` flag.
/// 
/// **Usage Note**: `TransactionShell` automatically **dereferences** to [`DatastoreShell`],
/// which means you can use the standard DatastoreShell methods (like `get_single` and
/// `run_query`) directly on a `TransactionShell` instance. The only exceptions are
/// the overridden `commit` and `rollback` methods. The `rollback` method in
/// particular has a simplified signature, as it always operates on its internal
/// transaction and does not require an optional transaction parameter.
pub struct TransactionShell {
    ds: DatastoreShell,
    active: bool,
}

impl<'a> Deref for TransactionShell {
    type Target = DatastoreShell;
    /// Allows all read-only methods (like `get_single`, `run_query`) from the
    /// wrapped [`DatastoreShell`] to be called directly on the `TransactionShell` instance.
    fn deref(&self) -> &Self::Target {
        &self.ds
    }
}

impl TransactionShell {
    /// Commits the pending mutations in the current transaction.
    ///
    /// If the commit is successful, the internal transaction state is marked as **inactive**
    /// (`active = false`), ensuring the transaction will not be rolled back automatically
    /// by the transaction runner.
    ///
    /// ## Parameters
    /// - `batch`: A [`ds::MutationBatch`] containing the changes to apply.
    ///
    /// ## Returns
    /// A [`Result`] containing the commit response or an error.
    pub async fn commit(
        &mut self,
        batch: ds::MutationBatch,
    ) -> Result<ds::MutationResponse, EntailError> {
        let result = self.ds.commit(batch).await;
        if result.is_ok() {
            self.active = false;
        }
        result
    }

    /// Rolls back the current transaction, discarding all uncommitted changes.
    ///
    /// If the rollback is successful, the internal transaction state is marked as **inactive**
    /// (`active = false`), ensuring the transaction will not be rolled back automatically
    /// by the transaction runner.
    ///
    /// ## Returns
    /// A [`Result`] indicating success (`()`) or an error.
    pub async fn rollback(&mut self) -> Result<(), EntailError> {
        let result = self.ds.rollback(&None).await;
        if result.is_ok() {
            self.active = false;
        }
        result
    }
}

impl From<DatastoreShell> for TransactionShell {
    /// Creates a new `TransactionShell` from a transactional `DatastoreShell`.
    ///
    /// This is an internal constructor used after a successful call to
    /// [`DatastoreShell::begin_transaction`]. The new shell is initialized to `active: true`
    /// if the shell is tied to a transaction.
    fn from(ds: DatastoreShell) -> Self {
        let has_txn = ds.transaction.is_some();
        Self { ds, active: has_txn }
    }
}

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

/// The configuration for a single Datastore transaction.
///
/// This struct acts as a runner for a series of Datastore operations that
/// must be executed atomically within a transaction. It handles the complexities
/// of transaction management, including automatic retries with exponential backoff
/// if needed.
pub struct Transaction<'a> {
    /// The maximum number of times to retry a transaction after a concurrency
    /// conflict. Defaults to `16`.
    pub retry_count: u32,
    /// The base duration for the first retry delay. This duration increases
    /// exponentially for subsequent retries, and a random jitter is added
    /// to the delay to prevent stampeding. Defaults to `25ms`.
    pub first_retry: Duration,
    ds: &'a DatastoreShell,
}

impl<'a> Transaction<'a> {
    /// Creates a new `Transaction` runner tied to a [`DatastoreShell`].
    ///
    /// The runner is initialized with the default retry configuration.
    ///
    /// ## Parameters
    /// - `ds`: A reference to the [`DatastoreShell`] to be used for Datastore access.
    pub fn new(ds: &'a DatastoreShell) -> Self {
        Self {
            retry_count: 16,
            first_retry: Duration::from_millis(25),
            ds,
        }
    }

    /// Sets the maximum number of retries for the transaction.
    ///
    /// This method consumes and returns `Self`, allowing for method chaining.
    ///
    /// ## Parameters
    /// - `retry_count`: The new maximum number of retries.
    pub fn with_retry_count(mut self, retry_count: u32) -> Self {
        self.retry_count = retry_count;
        self
    }

    /// Sets the initial delay for the first retry.
    ///
    /// This method consumes and returns `Self`, allowing for method chaining. The
    /// actual delay will be this duration with a random jitter added.
    ///
    /// ## Parameters
    /// - `first_retry`: The new base duration for the first retry.
    pub fn first_retry(mut self, first_retry: Duration) -> Self {
        self.first_retry = first_retry;
        self
    }

    /// Runs the provided asynchronous code block within a Datastore transaction.
    ///
    /// This is the primary method for executing transactional logic. It will automatically
    /// begin a transaction and execute the code in the provided closure. The closure is
    /// responsible for either committing or rolling back the transaction. If it does
    /// neither, the transaction will be automatically rolled back upon completion of the
    /// closure. If a concurrency conflict or other retryable error occurs, it will handle
    /// the full retry logic (including exponential backoff and jitter as recommended) up
    /// to the configured `retry_count`.
    ///
    /// The `body` closure is given a mutable reference to a [`TransactionShell`],
    /// which provides the transactional context for Datastore operations. The runner
    /// will automatically roll back the transaction if the closure completes
    /// without a successful commit or explicit rollback.
    ///
    /// ## Example
    /// The following example demonstrates how to use `run` to perform a transactional
    /// update. The operation will automatically retry if a concurrent change is detected.
    ///
    /// ```
    /// use entail::{
    ///     ds::{DatastoreShell, Entity, Key, MutationBatch, Value},
    ///     EntailError,
    ///     Transaction,
    /// };
    ///
    /// async fn update_user_name(
    ///     ds: &DatastoreShell,
    ///     user_id: i64,
    ///     new_name: &str,
    /// ) -> Result<(), EntailError> {
    ///     Transaction::new(ds)
    ///         .run(|ts| {
    ///             let key = Key::new("User").with_id(user_id);
    ///             let name_val = Value::unicode_string(new_name.to_string());
    ///
    ///             Box::pin(async move {
    ///                 // Read the entity within the transaction
    ///                 let mut user_entity: Entity = ts
    ///                     .get_single(key)
    ///                     .await?
    ///                     .expect("User not found");
    ///
    ///                 // Modify a property of the entity
    ///                 user_entity.set_indexed("name", name_val);
    ///
    ///                 // Commit the changes as an atomic update
    ///                 ts.commit(MutationBatch::new().update(user_entity)).await?;
    ///                 Ok(())
    ///             })
    ///         })
    ///         .await
    /// }
    /// ```
    ///
    /// ## Parameters
    /// - `body`: An async closure containing the logic to run inside the transaction.
    ///
    /// ## Returns
    /// The final result of the transaction body, or an [`EntailError`] if all
    /// retries fail.
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
