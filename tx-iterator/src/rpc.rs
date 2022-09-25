use crate::helpers::retry_rpc;
use crate::prelude::*;
use sui_sdk::SuiClient;
use tokio::time::sleep;

/// Fetches digests from given seq# inclusive.
///
/// This fn never returns an empty vector, it keeps polling until new digests
/// are available.
///
/// Each RPC call is retried a few times with an exponential back-off before
/// returning an error.
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
