//! Contains various ubiquitously used constructs.

pub use sui_sdk::rpc_types::GatewayTxSeqNumber as SeqNum;

/// A digest is [u8; 32] but this type is more convenient to work with in the
/// context of db query params.
pub type Digest = Vec<u8>;

use anyhow::Result;
use futures::Future;
use tokio::time::{sleep, Duration};

pub async fn retry<T, F>(
    mut job: impl FnMut() -> F,
    max_retries: usize,
    mut wait_ms: u64,
    exponential_backoff_multiplier: u64,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    assert_ne!(max_retries, 0);
    assert_ne!(exponential_backoff_multiplier, 0);

    let mut retries = max_retries;
    loop {
        match job().await {
            Err(_) if retries > 0 => {
                retries -= 1;
                sleep(Duration::from_millis(wait_ms)).await;
                wait_ms *= exponential_backoff_multiplier;
            }
            res => return res,
        }
    }
}
