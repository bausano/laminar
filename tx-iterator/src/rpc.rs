use crate::helpers::retry;
use crate::prelude::*;
use futures::Future;
use sui_sdk::SuiClient;
use tokio::time::sleep;

pub async fn fetch_digests(
    sui: &SuiClient,
    start_from_seqnum: SeqNum,
) -> Result<(SeqNum, Vec<Digest>)> {
    let fetch_until_seqnum = start_from_seqnum + consts::FETCH_TX_DIGESTS_BATCH;

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
            sleep(consts::SLEEP_ON_NO_NEW_TXS).await;
        };
    }
}

async fn retry_rpc<T, F>(job: impl FnMut() -> F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    // 1st retry after 10ms
    // 2nd retry after 100ms
    // 3rd retry after 1s
    retry(job, 3, 10, 10).await
}
