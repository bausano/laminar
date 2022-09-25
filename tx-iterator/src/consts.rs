use tokio::time::Duration;

pub const FETCH_TX_DIGESTS_BATCH: u64 = 100;
pub const SLEEP_ON_NO_NEW_TXS: Duration = Duration::from_millis(5);
