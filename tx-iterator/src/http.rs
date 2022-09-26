//! HTTP server is used by supervisor to inspect tx-iterator inner state.

use crate::prelude::*;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use warp::Filter;

/// Blocking operation which starts http server with paths:
/// 1. GET /seqnum => prints a number in the body
/// 2. GET /leader => prints "true"/"false"
///
/// # Note
/// We use [`Ordering::SeqCst`] to read the values are performance here is not
/// paramount and it's just easier to not have to think about.
pub async fn start(
    conf: Conf,
    next_fetch_from_seqnum: Arc<AtomicU64>,
    is_leader: Arc<AtomicBool>,
) {
    // 1.
    let seqnum = warp::path("seqnum").map(move || {
        format!("{}", next_fetch_from_seqnum.load(Ordering::SeqCst))
    });

    // 2.
    let leader = warp::path("leader")
        .map(move || format!("{}", is_leader.load(Ordering::SeqCst)));

    let routes = warp::get().and(seqnum.or(leader));

    warp::serve(routes).run(conf.http_addr).await;
}
