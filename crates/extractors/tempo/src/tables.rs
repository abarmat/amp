use datasets_common::network_id::NetworkId;
use datasets_raw::{
    dataset::Table,
    evm::tables::logs,
    tempo::tables::{blocks, transactions},
};

pub fn all(network: &NetworkId) -> Vec<Table> {
    vec![
        blocks::table(network.clone()),
        transactions::table(network.clone()),
        logs::table(network.clone()),
    ]
}
