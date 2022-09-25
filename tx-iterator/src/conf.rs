use crate::prelude::*;
use std::env;

#[derive(Clone, Debug)]
pub enum Role {
    /// Leader node is responsible for fetching txs from the chain and
    /// persisting them.
    Leader,
    /// Reads digests from this url.
    ///
    /// The url is associated with this variant because the writer db url is
    /// always required so that a node can self-promote.
    Support { db_conn_conf: String },
}

#[derive(Clone, Debug)]
pub struct Conf {
    /// If spawned as a leader, it will write digests into
    /// [`Env::writer_conn_conf`].
    /// If a support it will validate state from the provided (presumably
    /// read-only and different) url.
    pub spawned_as: Role,
    /// If spawned as a leader or promoted, this is where it can write new
    /// digests.
    ///
    /// e.g. `"host=localhost user=postgres"`, see
    /// [`tokio_postgres::config::Config`] on the specific format
    pub writer_conn_conf: String,
    /// Gateway RPC, e.g. `https://gateway.devnet.sui.io:443`.
    pub sui_node_url: String,
    /// Defaults to the seq# of the latest stored tx in db.
    /// This would be problematic if there was just a single tx-iterator.
    /// If leader's RPC became unavailable, we wouldn't have a way to tell
    /// whether the new RPC node ordered some txs differently.
    /// However, in a multi tx-iterator setup this is not a problem as we bet
    /// 1. at least one node is up at all times;
    /// 2. and we remember last seq# in the supervisor for that node.
    pub initial_seq_num: Option<SeqNum>,
}

impl Conf {
    pub fn from_env() -> Result<Self> {
        let sui_node_url = env::var("SUI_NODE_URL").context("Sui Node URL")?;

        let writer_conn_conf =
            env::var("WRITER_CONN_CONF").context("Writer DB URL")?;

        let role =
            if let Some(db_conn_conf) = env::var("SUPPORT_CONN_CONF").ok() {
                Role::Support { db_conn_conf }
            } else {
                Role::Leader
            };

        let initial_seq_num = env::var("INITIAL_SEQ_NUM")
            .ok()
            .map(|s| s.parse::<SeqNum>())
            .transpose()
            .context("Initial seq#")?;

        Ok(Self {
            spawned_as: role,
            writer_conn_conf,
            sui_node_url,
            initial_seq_num,
        })
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.spawned_as, Role::Leader)
    }

    pub async fn rpc(&self) -> Result<SuiClient> {
        SuiClient::new_rpc_client(&self.sui_node_url, None).await
    }

    pub async fn writer_db(&self) -> Result<DbClient> {
        let tls = tokio_postgres::NoTls;
        let (client, conn) =
            tokio_postgres::connect(&self.writer_conn_conf, tls).await?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                todo!("handle connection error: {}", e);
            }
        });

        Ok(client)
    }
}