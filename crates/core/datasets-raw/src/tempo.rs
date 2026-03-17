//! Tempo-specific types and Arrow schema definitions for blockchain data.
//!
//! Tempo extends the EVM with additional header fields (multiple gas limits,
//! millisecond timestamp precision) and a custom transaction type with batched
//! calls, 2D nonces, fee token selection, and sponsored transactions.
//! Logs are identical to standard EVM logs.

/// Table definitions for Tempo blockchain data (blocks, transactions).
pub mod tables;

// Re-export shared EVM types used by Tempo tables.
pub use crate::evm::{
    BYTES32_TYPE, Bytes32, EVM_ADDRESS_TYPE, EVM_CURRENCY_TYPE, EvmAddress, EvmCurrency,
    helpers::{Bytes32ArrayBuilder, EvmAddressArrayBuilder, EvmCurrencyArrayBuilder},
};
