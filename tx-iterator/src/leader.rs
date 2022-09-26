use crate::http::StatusReport;
use crate::prelude::*;
use crate::{db, rpc};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Starts polling RPC for new digests and persists them into db.
///
/// RPC errors are retried based on implementation in [`crate::rpc`] module.
/// If the retries failed, this fn returns an error.
///
/// Db error logged, then a new connection is created. If the new connection
/// does not work, this fn returns an error.
///
/// This fn fetches from RPC and inserts into db in parallel. While prev
/// iteration is being persisted, new digests are being fetched.
pub async fn start(
    conf: Conf,
    sui: SuiClient,
    mut db: DbClient,
    status: Arc<StatusReport>,
) -> Result<()> {
    // fetches the first batch and from here on the loop writes to these two
    // variables
    //
    // we do it this way to parallelize rpc and db calls
    let (mut fetch_from_seqnum, mut digests) = rpc::fetch_digests(
        &sui,
        // since this operation happens only once on boot, it's easier not
        // having to think about ordering
        status.next_fetch_from_seqnum.load(Ordering::SeqCst),
    )
    .await?;

    loop {
        assert!(!digests.is_empty());

        // insert previous iteration's digests into db and fetch new digests
        let (db_call, rpc_call) = tokio::join!(
            db::insert_digests(&db, &digests),
            rpc::fetch_digests(&sui, fetch_from_seqnum)
        );

        // try rebuilding connection and inserting again
        if let Err(db_err) = db_call {
            warn!(
                "Failed to insert digests starting from seq# '{}' into db: {}",
                fetch_from_seqnum, db_err
            );

            db = conf
                .writer_db()
                .await
                .context("Cannot revive db connection")?;

            db::insert_digests(&db, &digests)
                .await
                .context("Retrying inserting digests failed")?;
        }

        let (next_largest_seqnum, next_digests) =
            rpc_call.with_context(|| {
                format!(
                    "Cannot fetch next batch of digests starting from '{}'",
                    fetch_from_seqnum,
                )
            })?;

        // these digests are persisted in the next loop iteration
        digests = next_digests;

        // next iteration should not be inclusive
        let next_fetch_from_seqnum = next_largest_seqnum + 1;
        fetch_from_seqnum = next_fetch_from_seqnum;

        // we communicate this way with the http server
        // we relax because we don't read it in the context of this thread, it's
        // effectively like a counter
        status
            .next_fetch_from_seqnum
            .store(next_fetch_from_seqnum, Ordering::Relaxed);
    }
}
