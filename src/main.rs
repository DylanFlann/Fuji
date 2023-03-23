#[macro_use]
extern crate lazy_static;
use apibara_core::starknet::v1alpha2::{Block, Filter};
use apibara_sdk::ClientBuilder;
mod apibara;
mod config;
mod listeners;
mod models;
mod processing;

#[tokio::main]
async fn main() {
    let conf = config::load();
    let apibara_conf = apibara::create_apibara_config(&conf);
    let uri = conf.apibara.stream.parse().unwrap();
    let (mut data_stream, data_client) = ClientBuilder::<Filter, Block>::default()
        .connect(uri)
        .await
        .unwrap();

    data_client.send(apibara_conf).await.unwrap();
    processing::process_data_stream(&mut data_stream, &conf)
        .await
        .unwrap();
}
