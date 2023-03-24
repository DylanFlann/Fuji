use crate::config;
use apibara_core::starknet::v1alpha2::FieldElement;
use bigdecimal::{num_bigint::BigUint, BigDecimal, FromPrimitive};
use starknet::{core::types, id::decode};

pub fn on_funds_sent(
    conf: &config::Config,
    event_data: &Vec<FieldElement>,
    current_amount: &mut BigDecimal,
    current_gdp: &mut BigDecimal,
) -> BigDecimal {
    let to_addr = &event_data[1];
    let amount = BigDecimal::new(BigUint::from_bytes_be(&event_data[2].to_bytes()).into(), 18);
    if to_addr == &conf.contract.recipient {
        *current_amount += amount.clone();
    } else {
        *current_gdp += amount.clone();
    }
    amount
}

pub fn on_starknet_id_update(
    _: &config::Config,
    block_timestamp: i64,
    event_data: &Vec<FieldElement>,
    price: &BigDecimal,
    current_small_letters: &mut BigDecimal,
    current_long_range: &mut BigDecimal,
) {
    let domain_len = &event_data[0];
    if domain_len != &FieldElement::from_u64(1) {
        return;
    }
    let domain_str = decode(types::FieldElement::from_bytes_be(&event_data[1].to_bytes()).unwrap());
    //let owner: &FieldElement = &event_data[2];
    let expiry = BigUint::from_bytes_be(&event_data[3].to_bytes());
    // seconds in two years = 3600*24*365*2 = 63072000

    if expiry > BigUint::from_u64((block_timestamp as u64) + 63072000).unwrap() {
        *current_long_range += price;
    }

    if domain_str.len() < 5 {
        *current_small_letters += price;
    }
}
