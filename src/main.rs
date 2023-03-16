use anyhow::Result;
use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{Block, FieldElement, Filter, HeaderFilter},
};
use apibara_sdk::{ClientBuilder, Configuration, DataMessage};
use chrono::{DateTime, Utc};
use tokio_stream::StreamExt;
mod config;

fn build_filter(f: Filter) -> Filter {
    // eth contract address
    let eth_address = FieldElement::from_hex(
        "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
    )
    .unwrap();
    // pedersen hash of `Transfer`.
    let transfer_key =
        FieldElement::from_hex("0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9")
            .unwrap();

    // filter all transfers from the eth address, include the block header
    f.with_header(HeaderFilter::weak()).add_event(|ev| {
        ev.with_from_address(eth_address.clone())
            .with_keys(vec![transfer_key.clone()])
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let conf = config::load();

    let apibara_conf = Configuration::<Filter>::default()
        .with_finality(match conf.apibara.finality.as_str() {
            "Pending" => DataFinality::DataStatusPending,
            "Accepted" => DataFinality::DataStatusAccepted,
            "Finalized" => DataFinality::DataStatusFinalized,
            "Unknown" => DataFinality::DataStatusUnknown,
            _ => {
                panic!("error: finality must be Pending | Accepted | Finalized | Unknown");
            }
        })
        .with_batch_size(conf.apibara.batch_size)
        .with_starting_block(conf.apibara.starting_block)
        .with_filter(build_filter);

    // connnect to the mainnet stream
    let uri = conf.apibara.stream.parse()?;
    let (mut data_stream, data_client) = ClientBuilder::<Filter, Block>::default()
        .connect(uri)
        .await
        .unwrap();

    // send starting stream configuration to server
    data_client.send(apibara_conf).await.unwrap();

    // stream data from server
    while let Some(message) = data_stream.try_next().await.unwrap() {
        // messages can be either data or invalidate
        // - data: new data produced
        // - invalidate: a chain reorganization happened and some previously sent data is not valid
        // anymore
        match message {
            DataMessage::Data {
                cursor,
                end_cursor,
                finality,
                batch,
            } => {
                // cursor that generated the batch. if cursor = `None`, then it's the start of the
                // chain (includes genesis block).
                let start_block = cursor.map(|c| c.order_key).unwrap_or_default();
                // cursor that will be used to generate the next batch
                let end_block = end_cursor.order_key;

                //println!("Received data from block {start_block} to {end_block} with finality {finality:?}");

                // go through all blocks in the batch
                for block in batch {
                    // get block header and timestamp
                    let header = block.header.unwrap_or_default();
                    let timestamp: DateTime<Utc> =
                        header.timestamp.unwrap_or_default().try_into()?;
                    //println!("  Block {:>6} ({})", header.block_number, timestamp);

                    // go through all events in the block
                    for event_with_tx in block.events {
                        // event includes the tx that triggered the event emission
                        // it also include the receipt in `event_with_tx.receipt`, but
                        // it's not used in this example
                        let event = event_with_tx.event.unwrap_or_default();
                        let tx = event_with_tx.transaction.unwrap_or_default();
                        let tx_hash = tx
                            .meta
                            .unwrap_or_default()
                            .hash
                            .unwrap_or_default()
                            .to_hex();

                        let from_addr = event.data[0].to_hex();
                        let to_addr = &event.data[1];

                        if to_addr == &conf.contract.address {
                            println!("sent money to starknetid");
                        }
                    }
                }
            }
            DataMessage::Invalidate { cursor } => {
                println!("Chain reorganization detected: {cursor:?}");
            }
        }
    }

    Ok(())
}
