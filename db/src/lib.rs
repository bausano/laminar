//! Wraps around database queries.

use anyhow::{Context, Result};
use itertools::Itertools;
use misc::Digest;
use std::ops::Not;
use tokio_postgres::Client as DbClient;
enum Clusivity {
    In,
    Ex,
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
    // TODO: prepare this statement? with lazy load?
    let statement = format!(
        "SELECT
            digest
        FROM
            txs
        WHERE id {} (SELECT id FROM txs WHERE digest = ?)
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
                .expect("No column 'digest' in 'txs' table")
        })
        .collect())
}

pub async fn has_digest(db: &DbClient, digest: &Digest) -> Result<bool> {
    Ok(db
        .query("SELECT id FROM txs WHERE digest = ?", &[digest])
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
        "INSERT INTO txs (digest) VALUES ({}) ON CONFLICT DO NOTHING",
        (0..digests_count).map(|_| "?").join(","),
    )
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