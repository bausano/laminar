use tokio::time::Duration;

/// How many digests are fetched from RPC in each call and then persisted to
/// the db.
/// OPTIMIZE:
pub const FETCH_TX_DIGESTS_BATCH: u64 = 100;

/// Unlikely to be useful once Sui is adopted, but in case the network is idle,
/// how long to wait for next poll.
pub const SLEEP_ON_NO_NEW_TXS: Duration = Duration::from_millis(5);
