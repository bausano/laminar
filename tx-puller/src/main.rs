//! # Resources
//! - https://kerkour.com/rust-job-queue-with-postgresql
//! - https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
//! - https://shekhargulati.com/2022/01/27/correctly-using-postgres-as-queue
//! - https://www.crunchydata.com/blog/message-queuing-using-native-postgresql
//! - https://gist.github.com/chanks/7585810
//! - https://webapp.io/blog/postgres-is-the-answer

mod conf;
mod prelude;

use conf::Conf;
use futures::future;
use prelude::*;
use sui_sdk::rpc_types::SuiTransactionResponse;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    env_logger::init(); // set up with env RUST_LOG

    let conf = Conf::from_env().context("Cannot read env vars")?;

    let sui = conf.rpc().await?;
    let mut db = conf.db().await?;

    loop {
        let tx = db.transaction().await?;

        process_next_batch(&conf, &sui, &tx).await?;

        tx.commit().await?;
    }
}

/// 1. Locks digests in db
/// 2. Fetches details for those digests from rpc
/// 3. Checks if that tx is of interest - that is, does it touch an object that
/// some other part of the system cares about?
/// 4. Interesting txs are written to db
/// 5. All successfully fetched digest details are marked as processed
async fn process_next_batch(
    conf: &Conf,
    sui: &SuiClient,
    db: &impl GenericDbClient,
) -> Result<()> {
    // 1.
    let digests =
        db::select_and_lock_unprocessed_digests(db, conf.batch_size).await?;

    // 2.
    let responses = future::join_all(
        digests.iter().map(|(_, digest)| rpc::fetch_tx(sui, digest)),
    )
    .await;

    // 3.
    let mut ids_to_mark_processed = Vec::with_capacity(responses.len());
    let txs = digests
        .into_iter()
        .zip(responses)
        .filter_map(|(key, response)| Some((key, response.ok()?)))
        .filter(|((id, _), response)| {
            ids_to_mark_processed.push(*id);
            is_of_interest(response)
        })
        .map(|((id, digest), tx)| tx_to_json(id, digest, tx))
        .collect::<Result<Vec<_>>>()?;

    tokio::try_join!(
        // 4.
        db::insert_txs(db, &txs),
        // 5.
        db::mark_digests_as_processed(db, &ids_to_mark_processed)
    )?;

    Ok(())
}

fn is_of_interest(_tx: &SuiTransactionResponse) -> bool {
    todo!()
}

fn tx_to_json(
    _id: i64,
    _digest: Digest,
    _tx: SuiTransactionResponse,
) -> Result<db::SuiTx> {
    todo!()
}
