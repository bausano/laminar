// Methods relevant for startup
mod boot;
// Service configuration from env
mod conf;
// Wraps around db calls
mod db;
// TODO: export to another workspace member
mod helpers;
// Polling and persisting digests
mod leader;
// Ubiquitously used types
mod prelude;
// Fetching txs from gateway
mod rpc;
// Polling digests from RPC and db, validating them
mod support;
// Exports http server for service status and control
mod http;

use std::sync::{atomic::AtomicBool, Arc};

use crate::prelude::*;
use conf::Conf;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init(); // set up with env RUST_LOG

    let conf = Conf::from_env()?;

    let db = conf.db_conn_to_boot_with().await?;
    let sui = conf.rpc().await?;

    // prepares some state which is shared with the http server to allow
    // supervisor to inspect what's going on
    let start_iterating_from_seqnum =
        boot::find_seqnum_to_start_iterating_from(&db, &sui).await?;
    let is_leader = Arc::new(AtomicBool::new(conf.is_leader()));

    // run the http server in another task
    tokio::spawn(http::start(
        conf.clone(),
        Arc::clone(&start_iterating_from_seqnum),
        Arc::clone(&is_leader),
    ));

    if conf.is_leader() {
        leader::start(conf, sui, db, start_iterating_from_seqnum).await
    } else {
        support::start(conf, sui, db, start_iterating_from_seqnum, is_leader)
            .await
    }
}
