//! Wraps around database queries.
//!
//! TODO: prepare statements where relevant

mod models;

pub use models::SuiTx;

use anyhow::{Context, Result};
use itertools::Itertools;
use log::error;
use misc::Digest;
use models::Clusivity;
use std::ops::Not;
use tokio_postgres::{Client as DbClient, GenericClient as GenericDbClient};

/// See the documentation for [`tokio_postgres::connect`] for details.
pub async fn connect(conn_conf: &str) -> Result<DbClient> {
    let tls = tokio_postgres::NoTls;
    let (client, conn) = tokio_postgres::connect(conn_conf, tls).await?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("db connection error: {}", e);
        }
    });

    Ok(client)
}

pub async fn select_digests_since_exclusive(
    db: &DbClient,
    digest: &Digest,
    limit: usize,
) -> Result<Vec<Digest>> {
    select_digests_since(db, digest, Clusivity::Ex, limit).await
}

pub async fn select_digests_since_inclusive(
    db: &DbClient,
    digest: &Digest,
    limit: usize,
) -> Result<Vec<Digest>> {
    select_digests_since(db, digest, Clusivity::In, limit).await
}

async fn select_digests_since(
    db: &DbClient,
    digest: &Digest,
    clusivity: Clusivity,
    limit: usize,
) -> Result<Vec<Digest>> {
    let statement = format!(
        "SELECT
            digest
        FROM
            digests
        WHERE id {} (SELECT id FROM digests WHERE digest = ?)
        ORDER BY
            id
        ASC LIMIT {}",
        if matches!(clusivity, Clusivity::In) {
            ">="
        } else {
            ">"
        },
        limit
    );

    let rows = db
        .query(&statement, &[digest])
        .await
        .with_context(|| format!("Cannot select digests since {:?}", digest))?;

    Ok(rows
        .into_iter()
        .map(|row| {
            row.try_get::<_, Vec<u8>>("digest")
                .expect("No column 'digest' in 'digests' table")
        })
        .collect())
}

pub async fn has_digest(db: &DbClient, digest: &Digest) -> Result<bool> {
    Ok(db
        .query("SELECT id FROM digests WHERE digest = ?", &[digest])
        .await?
        .is_empty()
        .not())
}

/// Batch inserts digests in given order. On conflict (digests must be unique)
/// it skips given digest.
pub async fn insert_digests(db: &DbClient, digests: &[Digest]) -> Result<()> {
    let query = insert_digest_query(digests.len());

    db.execute_raw(&query, digests)
        .await
        .context("Cannot insert digests")?;

    Ok(())
}

fn insert_digest_query(digests_count: usize) -> String {
    assert_ne!(digests_count, 0, "Attempted to insert 0 digests");
    format!(
        "INSERT INTO digests (digest) VALUES ({}) ON CONFLICT DO NOTHING",
        (0..digests_count).map(|_| "?").join(","),
    )
}

/// Postgres can be used to an extend as a job queue. Unprocessed digests have
/// status 0.
///
/// See the `tx-puller` crate for more info.
pub async fn select_and_lock_unprocessed_digests(
    db: &impl GenericDbClient,
    limit: usize,
) -> Result<Vec<(i64, Digest)>> {
    let query = format!(
        "
        SELECT
            id, digest
        FROM
            digests
        WHERE
            status = 0
        LIMIT {} FOR UPDATE SKIP LOCKED;",
        limit
    );

    let rows = db.query(&query, &[]).await?;

    rows.into_iter()
        .map(|row| Ok((row.try_get("id")?, row.try_get("digest")?)))
        .collect()
}

/// Given list of ids, set their status to 1.
///
///  See also [`select_and_lock_unprocessed_digests`].
pub async fn mark_digests_as_processed(
    db: &impl GenericDbClient,
    ids_to_mark_processed: &[i64],
) -> Result<()> {
    let query = "
        UPDATE
            digests
        SET
            status = 1
        WHERE
            id IN (?)";

    db.execute_raw(query, ids_to_mark_processed).await?;

    Ok(())
}

pub async fn insert_txs(
    db: &impl GenericDbClient,
    txs: &[SuiTx],
) -> Result<()> {
    let query = format!(
        "INSERT INTO txs (order, digest, version, data) VALUES ({})",
        (0..txs.len()).map(|_| "?").join(","),
    );

    db.execute_raw(&query, txs)
        .await
        .context("Cannot insert txs")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_builds_insert_digest_query() {
        assert_eq!(
            &insert_digest_query(1),
            "INSERT INTO txs (digest) VALUES (?) ON CONFLICT DO NOTHING",
        );

        assert_eq!(
            &insert_digest_query(2),
            "INSERT INTO txs (digest) VALUES (?,?) ON CONFLICT DO NOTHING",
        );

        assert_eq!(
            &insert_digest_query(3),
            "INSERT INTO txs (digest) VALUES (?,?,?) ON CONFLICT DO NOTHING",
        );

        assert_eq!(
            &insert_digest_query(4),
            "INSERT INTO txs (digest) VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
        );
    }
}
