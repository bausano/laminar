use crate::env::Env;
use crate::prelude::*;
use crate::{db, rpc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub async fn spawn(
    env: Env,
    sui: SuiClient,
    mut db: DbClient,
    a_next_fetch_from_seqnum: Arc<AtomicU64>,
) -> Result<()> {
    // fetches the first batch and from here on the loop writes to these two
    // variables
    //
    // we do it this way to parallelize rpc and db calls
    let (mut fetch_from_seqnum, mut digests) = rpc::fetch_digests(
        &sui,
        // since this operation happens only once on boot, it's easier not
        // having to think about ordering
        a_next_fetch_from_seqnum.load(Ordering::SeqCst),
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

            db = env
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

        let next_fetch_from_seqnum = next_largest_seqnum
            // next iteration should not be inclusive
            .checked_add(1)
            .expect("seq# out of u64 bounds");
        fetch_from_seqnum = next_fetch_from_seqnum;

        // we communicate this way with the http server
        // we relax because we don't read it in the context of this thread, it's
        // effectively like a counter
        a_next_fetch_from_seqnum
            .store(next_fetch_from_seqnum, Ordering::Relaxed);
    }
}
