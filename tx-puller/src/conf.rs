use crate::prelude::*;
use std::env;

pub mod consts {
    pub mod defaults {
        pub const BATCH_SIZE: usize = 10;
    }
}

#[derive(Clone, Debug)]
pub struct Conf {
    /// e.g. `"host=localhost user=postgres"`, see
    /// [`tokio_postgres::config::Config`] on the specific format
    pub writer_conn_conf: String,
    /// Gateway RPC, e.g. `https://gateway.devnet.sui.io:443`.
    pub sui_node_url: String,
    /// How many txs to fetch from DB at once.
    pub batch_size: usize,
}

impl Conf {
    pub fn from_env() -> Result<Self> {
        let sui_node_url = env::var("SUI_NODE_URL").context("Sui Node URL")?;
        info!("RPC url: {}", sui_node_url);

        let writer_conn_conf =
            env::var("WRITER_CONN_CONF").context("Writer DB URL")?;

        let batch_size = env::var("BATCH_SIZE")
            .ok()
            .map(|s| s.parse::<usize>())
            .transpose()?
            .unwrap_or(consts::defaults::BATCH_SIZE);
        info!("Batch size: {}", batch_size);

        Ok(Self {
            sui_node_url,
            writer_conn_conf,
            batch_size,
        })
    }

    pub async fn rpc(&self) -> Result<SuiClient> {
        SuiClient::new_rpc_client(&self.sui_node_url, None).await
    }

    pub async fn db(&self) -> Result<DbClient> {
        db::connect(&self.writer_conn_conf).await
    }
}
