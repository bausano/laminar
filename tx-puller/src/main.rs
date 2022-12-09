//! # Resources
//! - https://kerkour.com/rust-job-queue-with-postgresql
//! - https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
//! - https://shekhargulati.com/2022/01/27/correctly-using-postgres-as-queue
//! - https://www.crunchydata.com/blog/message-queuing-using-native-postgresql
//! - https://gist.github.com/chanks/7585810
//! - https://webapp.io/blog/postgres-is-the-answer

mod conf;
mod prelude;

use conf::Conf;
use fastbloom_rs::{BloomFilter, Membership};
use futures::future;
use misc::sui_sdk::{
    rpc_types::{SuiEvent, SuiExecutionStatus, SuiTransactionResponse},
    types::object::Owner,
};
use prelude::*;
use std::iter;
use std::ops::Not;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    env_logger::init(); // set up with env RUST_LOG

    let conf = Conf::from_env().context("Cannot read env vars")?;

    let sui = conf.rpc().await?;
    let mut db = conf.db().await?;

    // TODO: figure out population and updating
    let builder = fastbloom_rs::FilterBuilder::new(100_000_000, 0.01);
    let bloom = BloomFilter::new(builder);

    loop {
        let tx = db.transaction().await?;

        process_next_batch(&conf, &sui, &tx, &bloom).await?;

        tx.commit().await?;
    }
}

/// 1. Lock digests in db
/// 2. Fetch details for those digests from rpc
/// 3. Check if that tx is of interest - that is, does it touch an object that
/// some other part of the system cares about?
/// 4. Interesting txs are written to db
/// 5. All successfully fetched digest details are marked as processed
async fn process_next_batch(
    conf: &Conf,
    sui: &SuiClient,
    db: &impl GenericDbClient,
    bloom: &BloomFilter,
) -> Result<()> {
    // 1.
    let digests =
        db::select_and_lock_unprocessed_digests(db, conf.batch_size).await?;

    // 2.
    let responses = future::join_all(
        digests.iter().map(|(_, digest)| rpc::fetch_tx(sui, digest)),
    )
    .await;

    // 3.
    let mut ids_to_mark_processed = Vec::with_capacity(responses.len());
    let txs = digests
        .into_iter()
        .zip(responses)
        .filter_map(|(key, response)| Some((key, response.ok()?)))
        .filter(|((id, _), response)| {
            // at this point, if there's a failure, it's only in serialization
            //
            // there's something abnormal about the tx, report error to us but
            // we expect that serialization will never fail
            ids_to_mark_processed.push(*id);
            is_tx_of_interest(bloom, response)
        })
        .map(|((id, digest), tx)| serialize_tx(id, digest, tx))
        .collect::<Result<Vec<_>>>()?;

    tokio::try_join!(
        // 4.
        db::insert_txs(db, &txs),
        // 5.
        db::mark_digests_as_processed(db, &ids_to_mark_processed)
    )?;

    Ok(())
}

/// The tx data is serialized with bincode and versioned in the db.
fn serialize_tx(
    id: i64,
    digest: Digest,
    tx: SuiTransactionResponse,
) -> Result<db::SuiTx> {
    Ok(db::SuiTx {
        order: id,
        digest,
        version: env!("CARGO_PKG_VERSION").to_string(),
        data: bincode::serialize(&tx)?,
    })
}

// OPTIMIZE: tone of opportunity to avoid needless computation
fn is_tx_of_interest(bloom: &BloomFilter, tx: &SuiTransactionResponse) -> bool {
    let e = &tx.effects;

    if matches!(e.status, SuiExecutionStatus::Success).not() {
        return false;
    }

    let owned_objs = iter::once(&e.gas_object)
        .chain(&e.created)
        .chain(&e.mutated)
        .chain(&e.unwrapped);
    for o in owned_objs {
        match o.owner {
            Owner::AddressOwner(addr) | Owner::ObjectOwner(addr) => {
                if bloom.contains(&addr.to_inner()) {
                    return true;
                }
            }
            Owner::Shared | Owner::Immutable => (),
        };
        if bloom.contains(&o.reference.object_id.into_bytes()) {
            return true;
        }
    }

    let objs = e.shared_objects.iter().chain(&e.deleted).chain(&e.wrapped);
    for o in objs {
        if bloom.contains(&o.object_id.into_bytes()) {
            return true;
        }
    }

    for event in &e.events {
        if is_event_of_interest(bloom, event) {
            return true;
        }
    }

    let c = &tx.certificate.data;
    if bloom.contains(c.sender.as_ref()) {
        return true;
    }

    false
}

fn is_event_of_interest(bloom: &BloomFilter, event: &SuiEvent) -> bool {
    match event {
        SuiEvent::Checkpoint(_) | SuiEvent::EpochChange(_) => false,
        SuiEvent::MoveEvent {
            package_id,
            sender,
            transaction_module,
            type_,
            ..
        } => {
            bloom.contains(package_id.as_slice()) ||
            bloom.contains(&sender.to_inner())
                // to listen to a specific event, we have to add following bytes
                // to the filter: "{package_id}{transaction_module}{event_name}"
                || bloom.contains(
                    &[
                        package_id.as_slice(),
                        transaction_module.as_bytes(),
                        type_.as_bytes(),
                    ]
                    .concat(),
                )
        }
        SuiEvent::Publish { package_id, sender } => {
            bloom.contains(package_id.as_slice())
                || bloom.contains(&sender.to_inner())
        }
        SuiEvent::TransferObject {
            package_id,
            sender,
            recipient,
            object_id,
            ..
        } => {
            bloom.contains(package_id.as_slice())
                || bloom.contains(object_id.as_slice())
                || bloom.contains(&sender.to_inner())
                || recipient
                    .get_owner_address()
                    .map(|a| bloom.contains(&a.to_inner()))
                    .unwrap_or(false)
        }
        SuiEvent::DeleteObject {
            package_id,
            sender,
            object_id,
            ..
        } => {
            bloom.contains(package_id.as_slice())
                || bloom.contains(object_id.as_slice())
                || bloom.contains(&sender.to_inner())
        }
        SuiEvent::NewObject {
            package_id,
            sender,
            recipient,
            object_id,
            ..
        } => {
            bloom.contains(package_id.as_slice())
                || bloom.contains(object_id.as_slice())
                || bloom.contains(&sender.to_inner())
                || recipient
                    .get_owner_address()
                    .map(|a| bloom.contains(&a.to_inner()))
                    .unwrap_or(false)
        }
    }
}
