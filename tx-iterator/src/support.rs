use crate::db;
use crate::leader;
use crate::prelude::*;
use crate::rpc;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::Instant;

pub async fn start(
    conf: Conf,
    sui: SuiClient,
    db: DbClient,
    a_next_fetch_from_seqnum: Arc<AtomicU64>,
    a_is_leader: Arc<AtomicBool>,
) -> Result<()> {
    let fetch_from_seqnum = a_next_fetch_from_seqnum.load(Ordering::SeqCst);

    let (mut latest_db_digest, db_only_digests) =
        initial_db_digests(&sui, &db, fetch_from_seqnum).await?;

    let mut db_only_digests: HashSet<_> = db_only_digests.into_iter().collect();
    let mut rpc_only_digests =
        HashMap::with_capacity(consts::FETCH_TX_DIGESTS_BATCH as usize * 4);
    let mut rpc_only_digests_timestamps =
        VecDeque::with_capacity(rpc_only_digests.capacity());

    loop {
        let (db_call, rpc_call) = tokio::join!(
            db::select_digests_since_exclusive(&db, &latest_db_digest),
            rpc::fetch_digests(&sui, fetch_from_seqnum),
        );

        let (fetched_digests_count, chain_digests) = rpc_call?;

        let db_digests = db_call?; // TODO: handle db err
        if let Some(latest) = db_digests.last().cloned() {
            latest_db_digest = latest;
            db_only_digests.extend(db_digests.into_iter());
        }

        let latest_seqnum = fetch_from_seqnum + fetched_digests_count;
        for (seqnum, digest) in
            (fetch_from_seqnum..latest_seqnum).zip(chain_digests)
        {
            if !db_only_digests.remove(&digest) {
                rpc_only_digests_timestamps
                    .push_back((Instant::now(), digest.clone()));
                rpc_only_digests.insert(digest, seqnum);
            }
        }

        if let Promote::Yes {
            start_leader_from_seqnum,
        } = pop_observed_digests(
            &db,
            &mut rpc_only_digests,
            &mut rpc_only_digests_timestamps,
        )
        .await?
        {
            // promotion to leader happens if the observed RPC txs are not
            // written to db in a timely manner

            // this is a one-time occurrence, no need for optimization
            let o = Ordering::SeqCst;
            a_next_fetch_from_seqnum.store(start_leader_from_seqnum, o);
            a_is_leader.store(true, o);

            break;
        } else {
            let oldest_unconfirmed = rpc_only_digests_timestamps
                .front()
                .and_then(|(_, seqnum)| rpc_only_digests.get(seqnum))
                .copied()
                // TODO: panic on overflow
                .unwrap_or_else(|| latest_seqnum + 1);
            a_next_fetch_from_seqnum
                .store(oldest_unconfirmed, Ordering::Relaxed);
        }

        // TODO iterate rpc_only_digests_timestamps if nearing capacity
    }

    leader::start(conf, sui, db, a_next_fetch_from_seqnum).await
}

enum Promote {
    Yes { start_leader_from_seqnum: SeqNum },
    No,
}

/// Let's see if those digests which we are expecting have been finally
/// added to the db.
///
/// If it takes longer than
/// [`consts::INVESTIGATE_IF_TX_ONLY_OBSERVED_ON_RPC_FOR`] to add txs to the db,
/// begin procedure to become a leader.
async fn pop_observed_digests(
    db: &DbClient,
    rpc_only_digests: &mut HashMap<Digest, SeqNum>,
    rpc_only_digests_timestamps: &mut VecDeque<(Instant, Digest)>,
) -> Result<Promote> {
    while let Some((timestamp, digest)) = rpc_only_digests_timestamps.front() {
        if !rpc_only_digests.contains_key(digest) {
            // we've finally observed the digest, s'all good

            rpc_only_digests_timestamps.pop_front();
        } else if Instant::now().duration_since(*timestamp)
            > consts::INVESTIGATE_IF_TX_ONLY_OBSERVED_ON_RPC_FOR
        {
            if db::has_digest(&db, digest).await? {
                // this is an unlikely but conceivable scenario:
                //
                // we start fetching from digest0, observe digest1 but
                // leader's node observed digest1 before digest0 (implying that
                // at least one is a broadcast tx and therefore
                // eventually-ordered)
                //
                // since this can occur only when we _start_ the support, we
                // deal with this in a rather inefficient way for simplicity

                rpc_only_digests_timestamps.pop_front();
            } else {
                // leader is either dead or is missing txs, time to take over
                //
                // let supervisor optimize for only having one leader

                // safe to unwrap bcs of prev `if` branch
                let start_leader_from_seqnum =
                    *rpc_only_digests.get(digest).unwrap();

                return Ok(Promote::Yes {
                    start_leader_from_seqnum,
                });
            }
        } else {
            // the tip is not yet in db, but it's been not that long so we give
            // the leader more time to catch up

            break;
        }
    }

    Ok(Promote::No)
}

async fn initial_db_digests(
    sui: &SuiClient,
    db: &DbClient,
    fetch_from_seqnum: SeqNum,
) -> Result<(Digest, Vec<Digest>)> {
    let fetch_from_digest =
        if let Some(digest) = rpc::digest(&sui, fetch_from_seqnum).await? {
            digest
        } else {
            // if digest does not exist, fetch from the latest one
            //
            // Scenario where it might not exist: we iterate the node and store
            // `next_fetch_from_seqnum` which is `latest_seqnum + 1`.
            // This seqnum is persisted by the supervisor.
            // A node might have crashed, was spawned as a support and there
            // were no tx since the last iteration.

            rpc::latest_digest(&sui).await?
        };

    let db_only_digests =
        db::select_digests_since_inclusive(&db, &fetch_from_digest).await?;

    let latest_db_digest =
        db_only_digests.last().cloned().unwrap_or(fetch_from_digest);

    Ok((latest_db_digest, db_only_digests))
}
