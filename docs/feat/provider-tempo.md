---
name: "provider-tempo"
description: "Tempo RPC provider configuration for Tempo-compatible chains. Load when asking about Tempo providers, Tempo endpoints, rate limiting, or batch requests"
type: feature
status: stable
components: "crate:providers-tempo,crate:common"
---

# Tempo RPC Provider

## Summary

The Tempo RPC provider enables data access from Tempo-compatible blockchains via standard JSON-RPC endpoints. It supports HTTP, WebSocket, and IPC connections with configurable rate limiting, request batching, concurrent request management, and authentication.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Connection Types](#connection-types)
5. [Usage](#usage)
6. [Implementation](#implementation)
7. [Limitations](#limitations)
8. [References](#references)

## Key Concepts

- **Tempo RPC**: JSON-RPC API for Tempo blockchain data access
- **Batching**: Combining multiple RPC calls into a single HTTP request
- **Rate Limiting**: Throttling requests to avoid endpoint limits
- **Receipt Fetching**: Strategy for obtaining transaction receipts
- **Authentication**: Optional auth header and token for secured endpoints

## Architecture

The Tempo RPC provider integrates into Amp's data pipeline as a data source for Tempo-compatible blockchains:

```
Dataset Job → Provider Resolution → Tempo RPC Client → RPC Endpoint
                                           ↓
                                   Rate Limiter
                                           ↓
                                   Batch Processor
                                           ↓
                            Block Stream (blocks, txs, logs)
```

### Connection Management

- **Auto-detection**: URL scheme determines connection type (HTTP/WS/IPC)
- **Concurrency control**: Semaphore limits parallel requests
- **Rate limiting**: Token bucket algorithm enforces per-minute quotas

### Data Extraction Flow

1. Job requests block range from dataset manifest
2. Provider resolution finds matching `tempo` provider for network
3. Client streams blocks using batched or unbatched strategy
4. Receipt fetching uses bulk or per-tx strategy based on config
5. Data materialized as Parquet files (blocks, transactions, logs tables)

## Configuration

For the complete field reference, see the [config schema](../schemas/providers/tempo.spec.json).

### Minimal Configuration

```toml
kind = "tempo"
network = "mainnet"
url = "${TEMPO_RPC_URL}"
```

### Full Configuration

```toml
kind = "tempo"
network = "mainnet"
url = "${TEMPO_RPC_URL}"
concurrent_request_limit = 512
rpc_batch_size = 100
rate_limit_per_minute = 1000
fetch_receipts_per_tx = false
auth_header = "Authorization"
auth_token = "${TEMPO_AUTH_TOKEN}"
timeout_secs = 30
```

## Connection Types

The provider auto-detects connection type from URL scheme:

| Scheme | Type | Use Case |
|--------|------|----------|
| `http://`, `https://` | HTTP | Standard RPC endpoints |
| `ws://`, `wss://` | WebSocket | Persistent connections |
| `ipc://` | IPC Socket | Local node connections |

### Examples

```toml
# HTTP endpoint
url = "https://rpc.tempo.xyz"

# WebSocket endpoint
url = "wss://rpc.tempo.xyz"

# Local IPC socket
url = "ipc:///home/user/.tempo/tempo.ipc"
```

## Usage

### Receipt Fetching Strategies

**Bulk receipts** (default, `fetch_receipts_per_tx = false`):
- Uses `eth_getBlockReceipts` for all receipts at once
- Faster but requires RPC support

**Per-transaction receipts** (`fetch_receipts_per_tx = true`):
- Uses `eth_getTransactionReceipt` for each transaction
- Slower but more compatible with all endpoints

### Rate Limiting

Configure rate limiting for endpoints with quotas:

```toml
# 10 requests per second
rate_limit_per_minute = 600
```

### Batching

Enable batching to reduce HTTP overhead:

```toml
# Batch up to 100 RPC calls per request
rpc_batch_size = 100
```

## Implementation

### Extracted Tables

| Table | Description |
|-------|-------------|
| `blocks` | Block headers with Tempo-specific fields |
| `transactions` | Transaction data with AA and multi-signature support |
| `logs` | Event logs |

### Source Files

- `crates/core/providers-tempo/src/lib.rs` - ProviderConfig and client factory
- `crates/core/providers-tempo/src/client.rs` - Client with streaming
- `crates/core/providers-tempo/src/config.rs` - Provider configuration
- `crates/core/providers-tempo/src/convert.rs` - RPC response to Arrow row conversion

## Limitations

- IPC connections only work with local nodes
- `eth_getBlockReceipts` not supported by all endpoints
- Rate limiting applies per provider instance, not globally

## References

- [provider](provider.md) - Base: Provider system overview
- [provider-config](provider-config.md) - Related: Configuration format
