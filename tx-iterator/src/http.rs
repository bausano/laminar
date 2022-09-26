//! HTTP server is used by supervisor to inspect tx-iterator inner state.

use crate::prelude::*;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use warp::Filter;

pub struct StatusReport {
    pub is_leader: AtomicBool,
    pub next_fetch_from_seqnum: AtomicU64,
}

/// Blocking operation which starts http server with paths:
/// 1. GET /leader => prints "true"/"false"
/// 2. GET /seqnum => prints a number in the body
///
/// # Note
/// We use [`Ordering::SeqCst`] to read the values are performance here is not
/// paramount and it's just easier to not have to think about.
pub async fn start(conf: Conf, status: Arc<StatusReport>) {
    // 1.
    let status_prime = Arc::clone(&status);
    let leader = warp::path("leader").map(move || {
        format!("{}", status_prime.is_leader.load(Ordering::SeqCst))
    });

    // 2.
    let seqnum = warp::path("seqnum").map(move || {
        format!("{}", status.next_fetch_from_seqnum.load(Ordering::SeqCst))
    });

    let routes = warp::get().and(seqnum.or(leader));

    warp::serve(routes).run(conf.http_addr).await;
}
