//! When the service boots it needs to determine which db to connect to based
//! on its [`Role`] - [`Conf::db_conn_to_boot_with`].
//!
//! Then it needs to determine which seq# to start iterating from -
//! [`find_seqnum_to_start_iterating_from`].

use crate::conf::{Conf, Role};
use crate::prelude::*;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

impl Conf {
    /// The connection that was passed from env.
    ///
    /// # Note
    /// Over the lifetime of the service this may not reflect the right
    /// connection anymore: the service could have been promoted from support to
    /// lead.
    pub async fn db_conn_to_boot_with(&self) -> Result<DbClient> {
        let tls = tokio_postgres::NoTls;
        let (client, conn) = match &self.spawned_as {
            Role::Leader => {
                tokio_postgres::connect(&self.writer_conn_conf, tls).await?
            }
            Role::Support { db_conn_conf } => {
                tokio_postgres::connect(db_conn_conf, tls).await?
            }
        };

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                todo!("handle connection error: {}", e);
            }
        });

        Ok(client)
    }
}

/// Returns an atomic [`u64`] as that's the inner type of the [`SeqNum`].
/// We use atomic to share information about where the tx-iterator currently is
/// with http server which runs in this service. This is used by supervisor.
pub async fn find_seqnum_to_start_iterating_from(
    _db: &DbClient,
    sui: &SuiClient,
) -> Result<Arc<AtomicU64>> {
    // TODO: when appropriate, start fetching from last db transaction.
    // However, atm Sui SDK does not provide us with any way to map digest to
    // the seq#
    // TODO: Move to db.rs module
    // https://discord.com/channels/916379725201563759/1006322742620069898/1023675518001872940
    // let latest_db_digest: Option<String> = db
    //     .query("SELECT digest FROM txs ORDER BY id LIMIT 1", &[])
    //     .await?
    //     .first()
    //     .map(|row| {
    //         row.try_get("digest")
    //             .context("Cannot find column 'digest' on tx")
    //     })
    //     .transpose()?;
    let latest_db_digest = None::<String>;

    let start_iterating_from_seqnum =
        if let Some(_latest_db_digest) = latest_db_digest {
            unimplemented!(
                "Sui SDK does not yet support mapping from digest to seq#"
            );
        } else {
            sui.read_api().get_total_transaction_number().await?
        };

    Ok(Arc::new(AtomicU64::new(start_iterating_from_seqnum)))
}
