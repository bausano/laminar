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
    dotenv::dotenv().ok();

    env_logger::init(); // set up with env RUST_LOG

    let conf = Conf::from_env()?;

    let db = conf.db_conn_to_boot_with().await?;
    let sui = conf.rpc().await?;

    // prepares some state which is shared with the http server to allow
    // supervisor to inspect what's going on
    let status = Arc::new(http::StatusReport {
        is_leader: AtomicBool::new(conf.is_leader()),
        next_fetch_from_seqnum: boot::find_seqnum_to_start_iterating_from(
            &conf, &db, &sui,
        )
        .await?,
    });

    tokio::spawn(http::start(conf.clone(), Arc::clone(&status)));

    if conf.is_leader() {
        leader::start(conf, sui, db, status).await
    } else {
        support::start(conf, sui, db, status).await
    }
}
