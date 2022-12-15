//! HTTP server receives commands to update bloom filter.
//! It receives a byte array and number of hashes settings.
//! It constructs the bloom filter and sends it over a channel.

use crate::conf::Conf;
use crossbeam_channel::Sender;
use fastbloom_rs::BloomFilter;
use warp::Filter;

/// 1. POST /bloom
pub async fn start(conf: Conf, channel: Sender<BloomFilter>) {
    // 1.
    let bloom = warp::path("bloom").map(move || {
        let hashes = 4;
        let data = vec![0; 1];
        let filter = BloomFilter::from_u8_array(&data, hashes);
        channel.send(filter).unwrap(); // todo
    });

    let routes = warp::get().and(bloom);

    warp::serve(routes).run(conf.http_addr).await;
}
