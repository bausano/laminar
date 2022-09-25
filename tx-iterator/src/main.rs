mod boot;
mod consts;
mod db;
mod env;
mod helpers;
mod leader;
mod prelude;
mod rpc;
mod support;

use crate::prelude::*;
use env::Env;

#[tokio::main]
async fn main() -> Result<()> {
    let env = Env::read()?;

    let db = env.db_conn_to_boot_with().await?;
    let sui = env.rpc().await?;

    let start_iterating_from_seqnum =
        boot::find_seqnum_to_start_iterating_from(&db, &sui).await?;

    // TODO: expose http server for seq#, status and role

    if env.is_leader() {
        leader::spawn(env, sui, db, start_iterating_from_seqnum).await
    } else {
        support::spawn().await
    }
}
