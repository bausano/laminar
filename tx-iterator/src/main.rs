mod boot;
mod conf;
mod consts;
mod db;
mod helpers;
mod leader;
mod prelude;
mod rpc;
mod support;

use crate::prelude::*;
use conf::Conf;

#[tokio::main]
async fn main() -> Result<()> {
    let conf = Conf::from_env()?;

    let db = conf.db_conn_to_boot_with().await?;
    let sui = conf.rpc().await?;

    let start_iterating_from_seqnum =
        boot::find_seqnum_to_start_iterating_from(&db, &sui).await?;

    // TODO: expose http server for seq#, status and role

    if conf.is_leader() {
        leader::start(conf, sui, db, start_iterating_from_seqnum).await
    } else {
        support::start().await
    }
}
