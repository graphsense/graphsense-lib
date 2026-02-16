# iknaio-tests-nightly

Nightly regression tests for GraphSense REST API.

## Overview

This repository contains automated regression tests that compare different versions of the GraphSense REST API to ensure backward compatibility and catch regressions.

## Quick Start

```bash
# Install dependencies
make install

# Build and test current HEAD against previous stable release
make regression-quick
```

## Usage

### Building Docker Images

Build from any version (tag, branch, or commit hash):

```bash
# Build current version (HEAD of local repo)
make build-current

# Build baseline version (auto-detected previous stable tag)
make build-baseline

# Build both
make build-both

# Build specific versions
make build-current CURRENT_VERSION=feature/new-api
make build-baseline BASELINE_VERSION=v25.11.16

# Build from a specific commit
make build-version VERSION=abc1234 IMAGE_TAG=test
```

### Running Servers

```bash
# Start both servers (requires config.yaml)
make serve-both

# Start individually
make serve-current   # port 9000
make serve-baseline  # port 9001

# Stop servers
make stop-all
```

### Running Tests

```bash
# Run all tests
make test

# Run regression tests (requires servers running)
make serve-both
make test-regression
make stop-all

# Full automated workflow: build → serve → test → stop
make regression-full

# Quick test using registry images (no build for baseline)
make regression-quick
```

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CURRENT_VERSION` | HEAD | Version for current server |
| `BASELINE_VERSION` | Previous stable tag | Version for baseline server |
| `CONFIG_FILE` | ./config.yaml | Path to server config |
| `CURRENT_PORT` | 9000 | Port for current server |
| `BASELINE_PORT` | 9001 | Port for baseline server |
| `GRAPHSENSE_LIB_LOCAL` | /home/tom/Documents/GitHub/graphsense/graphsense-lib | Local graphsense-lib path |

### Example config.yaml

```yaml
logging:
  level: INFO

database:
  driver: cassandra
  port: 9042
  nodes:
    - cassandra.example.com
  currencies:
    btc:
      raw: btc_raw
      transformed: btc_transformed
    eth:
      raw: eth_raw
      transformed: eth_transformed

gs-tagstore:
  url: postgresql+asyncpg://user:pass@postgres.example.com/tagstore
```

## Test Structure

```
tests/
└── rest/
    ├── conftest.py              # Fixtures (server setup, timing reports)
    ├── version_utils.py         # Version detection utilities
    └── test_baseline_regression.py  # Regression tests
```

## CI/CD Integration

For nightly CI runs:

```yaml
# GitHub Actions example
- name: Run regression tests
  run: |
    make pull-image-baseline  # Use cached baseline from registry
    make build-current
    make serve-both
    make test-regression
    make stop-all
  env:
    CONFIG_FILE: ${{ secrets.CONFIG_PATH }}
    BASELINE_VERSION: v25.11.17
```

## Reports

After running tests, timing reports are saved to `reports/regression_timing_report.json` with:
- Per-endpoint timing comparisons
- Speedup/slowdown analysis
- Pattern-based grouping
