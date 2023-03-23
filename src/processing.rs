use crate::apibara::{STARKNET_ID_UPDATE, TRANSFER_KEY};
use crate::config;
use crate::listeners;
use crate::models::Row;
use anyhow::Result;
use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{Block, Filter},
};
use apibara_sdk::{DataMessage, DataStream};
use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use chrono::Datelike;
use chrono::{DateTime, Utc};
use csv::Writer;
use tokio_stream::StreamExt;

pub async fn process_data_stream(
    data_stream: &mut DataStream<Filter, Block>,
    conf: &config::Config,
) -> Result<()> {
    let mut wtr = Writer::from_path("output.csv")?;
    let mut current_date = "none".to_string();
    let mut current_amount = Zero::zero();
    let mut current_gdp = Zero::zero();

    while let Some(message) = data_stream.try_next().await.unwrap() {
        match message {
            DataMessage::Data {
                cursor: _,
                end_cursor: _,
                finality,
                batch,
            } => {
                if finality != DataFinality::DataStatusFinalized {
                    println!("shutting down");
                    break;
                }

                for block in batch {
                    process_block(
                        &conf,
                        block,
                        &mut current_date,
                        &mut current_amount,
                        &mut current_gdp,
                        &mut wtr,
                    )
                    .await?;
                }
            }
            DataMessage::Invalidate { cursor } => {
                panic!("chain reorganization detected: {cursor:?}");
            }
        }
    }

    Ok(())
}

async fn process_block(
    conf: &config::Config,
    block: Block,
    current_date: &mut String,
    current_amount: &mut BigDecimal,
    current_gdp: &mut BigDecimal,
    wtr: &mut csv::Writer<std::fs::File>,
) -> Result<()> {
    let header = block.header.unwrap_or_default();
    let timestamp: DateTime<Utc> = header.timestamp.unwrap_or_default().try_into()?;
    let date = format! {
        "{}/{}/{}",
        timestamp.day(),
        timestamp.month(),
        timestamp.year()
    };

    if date != *current_date {
        if current_date != "none" {
            let gdp_share = current_amount.clone() / current_gdp.clone();
            wtr.serialize(Row {
                date: &current_date,
                revenue: &current_amount.to_string(),
                gdp_share: gdp_share.to_f32().unwrap(),
            })?;
            wtr.flush().unwrap();
            println!(
                "date: {}, revenue: {:.2} ETH, gdp_share: {:.2}%",
                date,
                current_amount,
                gdp_share.to_f32().unwrap() * 100.
            );
        }

        *current_date = date.clone();
        *current_amount = Zero::zero();
        *current_gdp = Zero::zero();
    }

    let mut last_transfer_tx = "none".to_string();
    let mut last_transfer_amount: BigDecimal = Zero::zero();
    for event_with_tx in block.events {
        let event = event_with_tx.event.unwrap_or_default();
        let tx = event_with_tx.transaction.unwrap_or_default();
        let tx_hash: String = tx
            .meta
            .unwrap_or_default()
            .hash
            .unwrap_or_default()
            .to_hex();

        let key = &event.keys[0];
        if key == &*TRANSFER_KEY {
            last_transfer_tx = tx_hash;
            last_transfer_amount =
                listeners::on_funds_sent(&conf, &event.data, current_amount, current_gdp);
        } else if key == &*STARKNET_ID_UPDATE && last_transfer_tx == tx_hash {
            // we check if last_transfer_tx == tx_hash to make sure this update is linked to a purchase
            listeners::on_starknet_id_update(&conf, &event.data, &last_transfer_amount);
        }
    }

    Ok(())
}
