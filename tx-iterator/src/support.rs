//! Support keeps fetching digests from two sources: _(i)_ RPC node and _(ii)_
//! db.
//!
//! The leader node writes to db as it iterates txs over its node.
//!
//! Support cross-validates txs with its own node's.
//! If leader goes down then support is promoted to leader.
//!
//! We keep the state in following three structures:
//! 1. hashset of db digests not yet observed on RPC
//! 2. hashmap of RPC digests to seqnums not yet observed in db
//! 3. FIFO queue of RPC digests with timestamp of when we observed them. This
//! is always a subset of the hashmap 2.

use crate::db;
use crate::http::StatusReport;
use crate::leader;
use crate::prelude::*;
use crate::rpc;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::Instant;

/// Support fetches digests from RPC and db. It verifies the work of the leader
/// by checking that expected digests are eventually present in db.
///
/// If the support observes discrepancy which is not fixed over some period of
/// time, then it assumes the leader role itself.
pub async fn start(
    conf: Conf,
    sui: SuiClient,
    mut db: DbClient,
    status: Arc<StatusReport>,
) -> Result<()> {
    let fetch_from_seqnum =
        status.next_fetch_from_seqnum.load(Ordering::SeqCst);

    // latest_db_digest will be mutated in the loop
    let (mut latest_db_digest, initial_db_only_digests) =
        initial_db_digests(&sui, &db, fetch_from_seqnum).await?;

    // 1. hashset of db digests not yet observed on RPC
    let mut db_only_digests: HashSet<_> =
        initial_db_only_digests.into_iter().collect();
    // 2. hashmap of RPC digests to seqnums not yet observed in db
    let mut rpc_only_digests =
        HashMap::with_capacity(consts::FETCH_TX_DIGESTS_BATCH as usize * 4);
    // 3. FIFO queue of RPC digests with timestamp of when we observed them
    let mut rpc_only_digests_timestamps =
        VecDeque::with_capacity(rpc_only_digests.capacity());

    loop {
        // OPTIMIZE: measure which of the two is bottleneck, if db we can skip
        // the call every nth iteration or if there hasn't been anything new
        // in the past call
        let (db_call, rpc_call) = tokio::join!(
            db::select_digests_since_exclusive(&db, &latest_db_digest),
            rpc::fetch_digests(&sui, fetch_from_seqnum),
        );

        let (latest_seqnum, new_rpc_digests) = rpc_call?;

        let new_db_digests = retry_db_conn_on_err_in_select_digests(
            &conf,
            &mut db,
            &latest_db_digest,
            db_call,
        )
        .await?;

        if let Some(latest) = new_db_digests.last().cloned() {
            // if there are some new digests...

            latest_db_digest = latest;
            db_only_digests.extend(new_db_digests.into_iter());
        }

        for (seqnum, digest) in
            (fetch_from_seqnum..latest_seqnum).zip(new_rpc_digests)
        {
            let is_in_db = db_only_digests.remove(&digest);
            if !is_in_db {
                // digest not observed in db, we are yet to see it persisted by
                // the leader

                rpc_only_digests_timestamps
                    .push_back((Instant::now(), digest.clone()));
                rpc_only_digests.insert(digest, seqnum);
            }
        }

        if let Promote::Yes {
            start_leader_from_seqnum,
        } = pop_observed_digests(
            &conf,
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
            status
                .next_fetch_from_seqnum
                .store(start_leader_from_seqnum, o);
            status.is_leader.store(true, o);

            break;
        } else {
            let oldest_unconfirmed_seqnum = rpc_only_digests_timestamps
                .front()
                .and_then(|(_, seqnum)| rpc_only_digests.get(seqnum))
                .copied()
                .unwrap_or_else(|| latest_seqnum + 1);
            status
                .next_fetch_from_seqnum
                // acts as a counter
                .store(oldest_unconfirmed_seqnum, Ordering::Relaxed);
        }

        // TODO: clean up rpc_only_digests_timestamps if nearing capacity
        // TODO: if rpc_only_digests are reaching capacity, what do we do?
        // TODO: if db_only_digests are reaching capacity, what do we do?
    }

    // explicit drop bcs next logic might allocate new memory and if we got here
    // then it's likely that our data structure grew large, so avoid OOM
    info!(
        "Promoting support of node '{}' to leader. It had stored {} db digests.",
        conf.sui_node_url,
        db_only_digests.len()
    );
    drop(db_only_digests);

    // promote db collection
    let db = conf
        .leader_db()
        .await
        .context("Cannot start writer db connection")?;

    // iterate rpc_only_digests_timestamps and insert that to db
    // in the same order those which are not there yet according to our state
    let digests_not_observed_in_db: Vec<_> = rpc_only_digests_timestamps
        .into_iter()
        .filter(|(_, digest)| rpc_only_digests.contains_key(digest))
        .map(|(_, digest)| digest)
        .collect();
    if let Some(latest_digest) = digests_not_observed_in_db.first() {
        info!(
            "There have been {} digests observed on RPC \
            but not in db starting with '{:?}'. Inserting them into db.",
            digests_not_observed_in_db.len(),
            latest_digest
        );

        let latest_seqnum =
            rpc_only_digests.get(latest_digest).copied().unwrap();
        drop(rpc_only_digests); // same reason as drop above

        // TODO: could `digests_not_observed_in_db` be too large one time
        // insert?
        db::insert_digests(&db, &digests_not_observed_in_db)
            .await
            .context("Cannot insert remaining db-unobserved digests")?;

        // we've observed all txs up until the last one, we start from the next
        // one
        status
            .next_fetch_from_seqnum
            .store(latest_seqnum + 1, Ordering::SeqCst);
    }

    leader::start(conf, sui, db, status).await
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
    conf: &Conf,
    db: &DbClient,
    rpc_only_digests: &mut HashMap<Digest, SeqNum>,
    rpc_only_digests_timestamps: &mut VecDeque<(Instant, Digest)>,
) -> Result<Promote> {
    while let Some((timestamp, digest)) = rpc_only_digests_timestamps.front() {
        if !rpc_only_digests.contains_key(digest) {
            // we've finally observed the digest, s'all good

            rpc_only_digests_timestamps.pop_front();
        } else if Instant::now().duration_since(*timestamp)
            > conf.investigate_if_tx_only_observed_on_rpc_for
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

/// Since the state we've built here is valuable, let's attempt to
/// rebuild the db conn before crashing the service.
async fn retry_db_conn_on_err_in_select_digests(
    conf: &Conf,
    db: &mut DbClient,
    latest_db_digest: &Digest,
    db_call: Result<Vec<Digest>>,
) -> Result<Vec<Digest>> {
    // since the state we've built here is valuable, let's attempt to
    // rebuild the db conn before crashing the service
    match db_call {
        ok @ Ok(_) => ok,
        Err(db_err) => {
            warn!(
                "Failed to select digests since '{:?}' from db: {}",
                latest_db_digest, db_err
            );

            *db = conf
                .support_db()
                .await
                .context("Cannot revive db connection")?;

            db::select_digests_since_exclusive(db, latest_db_digest).await
        }
    }
}
