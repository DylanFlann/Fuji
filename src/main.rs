extern crate lazy_static;
use apibara_core::starknet::v1alpha2::{Block, Filter};
use apibara_sdk::ClientBuilder;
use processing::ProcessingError;
mod apibara;
mod config;
mod listeners;
mod models;
mod processing;

#[tokio::main]
async fn main() {
    let conf = config::load();
    let mut cursor_opt = None;
    loop {
        let apibara_conf = apibara::create_apibara_config(&conf, cursor_opt.clone());
        let uri = conf.apibara.stream.parse().unwrap();
        let (mut data_stream, data_client) = ClientBuilder::<Filter, Block>::default()
            .connect(uri)
            .await
            .unwrap();

        data_client.send(apibara_conf).await.unwrap();
        match processing::process_data_stream(&mut data_stream, &conf).await {
            Err(e) => {
                if let Some(ProcessingError::CursorError(cursor_opt2)) =
                    e.downcast_ref::<ProcessingError>()
                {
                    cursor_opt = cursor_opt2.clone();
                    println!("connection reset, restarting from last cursor");
                }
            }
            Ok(_) => {
                break;
            }
        }
    }
}
