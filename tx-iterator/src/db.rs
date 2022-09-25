//! Wraps around database queries performed by the tx-iterators.

use crate::prelude::*;
use itertools::Itertools;

/// Batch inserts digests in given order. On conflict (digests must be unique)
/// it skips given digest.
pub async fn insert_digests(db: &DbClient, digests: &[Digest]) -> Result<()> {
    let query = insert_digest_query(digests.len());

    db.execute_raw(&query, digests).await?;

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
