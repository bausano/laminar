use misc::Digest;
use postgres_types::{FromSql, ToSql};

#[derive(Debug, ToSql, FromSql)]
pub struct SuiTx {
    /// Maps to `id` in `digests` table. Not a foreign key so that we can empty
    /// the `digests` table but keep the txs.
    pub order: i64,
    pub digest: Digest,
    /// We're using https://github.com/near/borsh-rs to store tx info
    pub data: Vec<u8>,
}

pub(crate) enum Clusivity {
    In,
    Ex,
}
