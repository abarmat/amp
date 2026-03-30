---
name: "datasets-raw-tempo"
description: "Tempo raw dataset definition with blocks, transactions, and logs table schemas. Load when asking about Tempo datasets, Tempo table schemas, or tempo manifest format"
type: feature
status: stable
components: "crate:tempo-datasets"
---

# Tempo Dataset

## Summary

The Tempo dataset defines the table schemas for block, transaction, and event log data from Tempo-compatible blockchains. It declares three tables — `blocks`, `transactions`, and `logs` — extending the core EVM data model with Tempo-specific fields for account abstraction (AA) transactions, multi-type signatures, batched calls, and millisecond-precision timestamps. The dataset shares the EVM log schema from `datasets-raw` and adds Tempo-specific block and transaction fields.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Manifest](#manifest)
3. [Schema](#schema)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **Tempo RPC**: Tempo blockchain JSON-RPC API — this dataset kind defines schemas matching the Tempo RPC response format
- **Shared EVM Schemas**: The `logs` table uses the standard EVM log schema, ensuring consistent column definitions across EVM-compatible dataset kinds
- **Tempo Block Fields**: Additional block header fields (`general_gas_limit`, `shared_gas_limit`, `timestamp_millis_part`) beyond standard EVM blocks
- **AA Transactions**: Account abstraction support with multi-type signatures (secp256k1, p256, webAuthn), batched calls, fee token selection, and sponsored transactions

## Manifest

See the [raw dataset manifest schema](../schemas/manifest/raw.spec.json) for the complete field reference, types, defaults, and examples.

## Schema

This dataset declares three tables: `blocks`, `transactions`, and `logs`.

For detailed column definitions, see the [table schema](../schemas/tables/tempo.md).

## Implementation

### Source Files

- `crates/extractors/tempo/src/lib.rs` — `Manifest`, `dataset()` factory, re-exports
- `crates/extractors/tempo/src/tables.rs` — `all()` function returning blocks, transactions, logs tables
- `crates/core/datasets-raw/src/tempo/tables/blocks.rs` — Tempo block schema with Tempo-specific fields
- `crates/core/datasets-raw/src/tempo/tables/transactions.rs` — Tempo transaction schema with AA support
- `crates/core/datasets-raw/src/evm.rs` — Shared EVM logs schema

## References

- [datasets](datasets.md) - Base: Dataset system overview
- [datasets-raw](datasets-raw.md) - Base: Raw dataset architecture
- [datasets-manifest](datasets-manifest.md) - Related: Manifest format
- [provider-tempo](provider-tempo.md) - Related: Tempo RPC provider configuration
