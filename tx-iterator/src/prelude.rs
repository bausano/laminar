pub(crate) use crate::consts;
pub use anyhow::{bail, Context, Result};
pub use log::{error, warn};
pub use sui_sdk::rpc_types::GatewayTxSeqNumber as SeqNum;
pub use sui_sdk::SuiClient;
pub use tokio_postgres::Client as DbClient;

/// A digest is [u8; 32] but this type is more convenient to work with in the
/// context of db query params.
pub type Digest = Vec<u8>;
