use anyhow::{anyhow, Result};
use futures::Future;
use misc::{Digest, SeqNum};
use sui_sdk::SuiClient;
use tokio::time::{sleep, Duration};

/// Unlikely to be useful once Sui is adopted, but in case the network is
/// idle, how long to wait for next poll.
const SLEEP_ON_NO_NEW_TXS: Duration = Duration::from_millis(5);

/// Fetches consecutive digests starting from given seq# inclusive. Also returns
/// the seqnum of the latest digest (last in the vec).
///
/// This fn never returns an empty vector, it keeps polling until new digests
/// are available.
///
/// Each RPC call is retried a few times with an exponential back-off before
/// returning an error.
pub async fn fetch_digests(
    sui: &SuiClient,
    start_from_seqnum: SeqNum,
    limit: usize,
) -> Result<(SeqNum, Vec<Digest>)> {
    let fetch_until_seqnum = start_from_seqnum + limit as u64;

    loop {
        let txs = retry_rpc(move || {
            // TODO: confirm that we can provide larger tx id than highest
            // existing and it will gracefully return
            sui.read_api().get_transactions_in_range(
                start_from_seqnum,
                fetch_until_seqnum,
            )
        })
        .await?;

        if let Some((seq_num, _)) = txs.last() {
            break Ok((
                *seq_num,
                txs.into_iter()
                    .map(|(_, digest)| digest.to_bytes())
                    .collect(),
            ));
        } else {
            sleep(SLEEP_ON_NO_NEW_TXS).await;
        };
    }
}

/// Gets the most recent tx's digest.
pub async fn latest_digest(sui: &SuiClient) -> Result<Digest> {
    let txs = retry_rpc(|| sui.read_api().get_recent_transactions(1)).await?;

    txs.into_iter()
        .next()
        .map(|(_, digest)| digest.to_bytes())
        .ok_or_else(|| anyhow!("There are no txs known to the node yet"))
}

/// Returns digest of tx with given seqnum.
pub async fn digest(sui: &SuiClient, seqnum: SeqNum) -> Result<Option<Digest>> {
    let txs =
        retry_rpc(|| sui.read_api().get_transactions_in_range(seqnum, seqnum))
            .await?;

    Ok(txs.into_iter().next().map(|(_, digest)| digest.to_bytes()))
}

async fn retry_rpc<T, F>(job: impl FnMut() -> F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    // 1st retry after 10ms
    // 2nd retry after 100ms
    // 3rd retry after 1s
    misc::retry(job, 3, 10, 10).await
}
