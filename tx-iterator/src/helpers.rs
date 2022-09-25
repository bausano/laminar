use crate::prelude::*;
use futures::Future;
use tokio::time::{sleep, Duration};

pub async fn retry_rpc<T, F>(job: impl FnMut() -> F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    // 1st retry after 10ms
    // 2nd retry after 100ms
    // 3rd retry after 1s
    retry(job, 3, 10, 10).await
}

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
