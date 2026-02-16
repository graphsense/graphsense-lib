# Delta Lake Cross-Version Compatibility Tests

Verifies that upgrading graphsense-lib dependencies (deltalake, pyarrow, etc.) doesn't break Delta Lake table compatibility. Compares output from a reference version against the current checkout.

## Prerequisites

- Docker (for MinIO testcontainer)
- `uv` (Python package manager)
- A blockchain node endpoint (ETH RPC, BTC RPC, TRX gRPC, etc.)

## Quick Start

```bash
cd tests/regressions

# ETH (default)
make test-deltalake NODE_URL=http://your-eth-node:8545

# BTC
make test-deltalake DELTA_CURRENCY=btc NODE_URL=http://your-btc-node:8332 DELTA_START_BLOCK=800000

# TRX
make test-deltalake DELTA_CURRENCY=trx NODE_URL=grpc://your-trx-node:50051 DELTA_START_BLOCK=40000000
```

Without `NODE_URL` the test skips automatically.

## What It Does

1. Creates two isolated venvs: **reference** (git tag) and **current** (local checkout)
2. Spins up a MinIO container
3. **Reference-only run**: ref ingests base blocks, ref appends more blocks
4. **Mixed run**: ref ingests same base blocks, **current** appends same blocks
5. Compares schema, row counts, and content hashes — any difference = test failure

## Configuration

All via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_URL` | *(required)* | Blockchain node endpoint |
| `DELTA_REF_VERSION` | `v25.11.18` | Reference git tag/commit |
| `DELTA_CURRENCY` | `eth` | Currency to test |
| `DELTA_START_BLOCK` | `1000000` | First block to ingest |
| `DELTA_BASE_BLOCKS` | `50` | Blocks for base ingestion |
| `DELTA_APPEND_BLOCKS` | `50` | Blocks for append ingestion |
| `GSLIB_PATH` | auto-detected | Path to local graphsense-lib |

## Cleanup

```bash
# Remove cached venvs
make clean-deltalake
```
