use misc::Digest;
use postgres_types::{FromSql, ToSql};

#[derive(Debug, ToSql, FromSql)]
pub struct SuiTx {
    /// Maps to `id` in `digests` table. Not a foreign key so that we can empty
    /// the `digests` table but keep the txs.
    pub order: i64,
    pub digest: Digest,
    pub version: String,
    /// We're using https://crates.io/crates/bincode to store tx info. That
    /// means this data deserializes into [`SuiTransactionResponse`].
    pub data: Vec<u8>,
}

pub(crate) enum Clusivity {
    In,
    Ex,
}
