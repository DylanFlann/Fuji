use anyhow::Result;
use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{Block, FieldElement, Filter, HeaderFilter},
};
use apibara_sdk::{ClientBuilder, Configuration, DataMessage};
use bigdecimal::{num_bigint::BigUint, BigDecimal};
use chrono::{DateTime, Datelike, Utc};
use csv::Writer;
use std::str::FromStr;
use tokio_stream::StreamExt;
mod config;

#[derive(serde::Serialize)]
struct Row<'a> {
    date: &'a str,
    revenue: &'a str,
    gdp_share: &'a str,
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
        .with_filter(|f: Filter| -> Filter {
            // pedersen hash of `Transfer`.
            let transfer_key = FieldElement::from_hex(
                "0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9",
            )
            .unwrap();
            // filter all transfers from the eth address, include the block header
            f.with_header(HeaderFilter::weak()).add_event(|ev| {
                ev.with_from_address(conf.contract.token.clone())
                    .with_keys(vec![transfer_key.clone()])
            })
        });

    // connnect to the mainnet stream
    let uri = conf.apibara.stream.parse()?;
    let (mut data_stream, data_client) = ClientBuilder::<Filter, Block>::default()
        .connect(uri)
        .await
        .unwrap();

    // send starting stream configuration to server
    data_client.send(apibara_conf).await.unwrap();
    let mut wtr = Writer::from_path("output.csv")?;
    let mut current_date = "none".to_string();
    let mut current_amount = BigDecimal::from_str("0")?;
    let mut current_gdp = BigDecimal::from_str("0")?;

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
                if finality != DataFinality::DataStatusFinalized {
                    println!("shutting down");
                    break;
                }

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

                    let date = format! {
                        "{}/{}/{}",
                        timestamp.day(),
                        timestamp.month(),
                        timestamp.year()
                    };
                    if date != current_date {
                        if current_date != "none" {
                            let gdp_share = current_amount.clone() / current_gdp;
                            wtr.serialize(Row {
                                date: &current_date,
                                revenue: &current_amount.to_string(),
                                gdp_share: &gdp_share.to_string(),
                            })?;
                            println!(
                                "{}, revenue: {} gdp_share: {}",
                                date, current_amount, gdp_share
                            );
                        }

                        current_date = date.clone();
                        current_amount = BigDecimal::from_str("0")?;
                        current_gdp = BigDecimal::from_str("0")?;
                    } else {
                        for event_with_tx in block.events {
                            let event = event_with_tx.event.unwrap_or_default();
                            let tx = event_with_tx.transaction.unwrap_or_default();
                            let tx_hash = tx
                                .meta
                                .unwrap_or_default()
                                .hash
                                .unwrap_or_default()
                                .to_hex();
                            let to_addr = &event.data[1];
                            // we won't transfer more than 2**128 gwei so no need to check [3]
                            if to_addr == &conf.contract.recipient {
                                current_amount += BigDecimal::new(
                                    BigUint::from_bytes_be(&event.data[2].to_bytes()).into(),
                                    18,
                                );
                            } else {
                                current_gdp += BigDecimal::new(
                                    BigUint::from_bytes_be(&event.data[2].to_bytes()).into(),
                                    18,
                                );
                            }
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
