use crate::config::Config;
use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{FieldElement, Filter, HeaderFilter},
};
use apibara_sdk::Configuration;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref TRANSFER_KEY: FieldElement =
        FieldElement::from_hex("0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9",)
            .unwrap();
    pub static ref STARKNET_ID_UPDATE: FieldElement =
        FieldElement::from_hex("0x440fc089956d79d058ea92abe99c718a6b1441e3aaec132cc38a01e9b895cb",)
            .unwrap();
}

pub fn create_apibara_config(conf: &Config) -> Configuration<Filter> {
    Configuration::<Filter>::default()
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
            f.with_header(HeaderFilter::weak())
                .add_event(|ev| {
                    ev.with_from_address(conf.contract.token.clone())
                        .with_keys(vec![TRANSFER_KEY.clone()])
                })
                .add_event(|ev| {
                    ev.with_from_address(conf.contract.recipient.clone())
                        .with_keys(vec![STARKNET_ID_UPDATE.clone()])
                })
        })
}
