# GraphSense Library

[![Test and Build Status](https://github.com/graphsense/graphsense-lib/actions/workflows/run_tests.yaml/badge.svg)](https://github.com/graphsense/graphsense-lib/actions) [![PyPI version](https://badge.fury.io/py/graphsense-lib.svg)](https://badge.fury.io/py/graphsense-lib) [![Python](https://img.shields.io/pypi/pyversions/graphsense-lib)](https://pypi.org/project/graphsense-lib/) [![Downloads](https://static.pepy.tech/badge/graphsense-lib)](https://pepy.tech/project/graphsense-lib)

A comprehensive Python library for the GraphSense crypto-analytics platform. It provides database access, data ingestion, maintenance tools, and analysis capabilities for cryptocurrency transactions and networks.

> **Note:** This library uses optional dependencies. Use `graphsense-lib[all]` to install all features.

## Quick Start

### Installation

```bash
# Install with all features
pip install graphsense-lib[all]

# Install from source
git clone https://github.com/graphsense/graphsense-lib.git
cd graphsense-lib
make install
```

### Basic Usage

#### Database Access with Configuration File

```python
from graphsenselib.db import DbFactory

# Using GraphSense config file (default: ~/.graphsense.yaml)
with DbFactory().from_config("development", "btc") as db:
    highest_block = db.transformed.get_highest_block()
    print(f"Highest BTC block: {highest_block}")

    # Get block details
    block = db.transformed.get_block(100000)
    print(f"Block 100000: {block.block_hash}")
```

#### Direct Database Connection

```python
from graphsenselib.db import DbFactory

# Direct connection without config file
with DbFactory().from_name(
    raw_keyspace_name="eth_raw",
    transformed_keyspace_name="eth_transformed",
    schema_type="account",
    cassandra_nodes=["localhost"],
    currency="eth"
) as db:
    print(f"Highest block: {db.transformed.get_highest_block()}")
```

#### Async Database Services

```python
import asyncio
from graphsenselib.db.asynchronous.services import BlocksService, AddressesService

async def analyze_address():
    # Initialize services with database connection
    blocks_service = BlocksService(db, rates_service, config, logger)
    addresses_service = AddressesService(db, rates_service, config, logger)

    # Get address information
    address = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
    address_info = await addresses_service.get_address("btc", address)
    print(f"Address balance: {address_info.balance}")

    # Get address transactions
    txs = await addresses_service.list_address_txs("btc", address)
    print(f"Transaction count: {len(txs)}")

asyncio.run(analyze_address())
```

## Command Line Interface

GraphSense-lib exposes a comprehensive CLI tool: `graphsense-cli`

### Basic Commands

```bash
# Show help and available commands
graphsense-cli --help

# Check version
graphsense-cli version

# Show current configuration
graphsense-cli config show

# Generate config template
graphsense-cli config template > ~/.graphsense.yaml

# Show config file path
graphsense-cli config path
```

## Modules

### Database Management

Query and manage the GraphSense database state.

```bash
# Show database management options
graphsense-cli db --help

# Check database state/summary
graphsense-cli db state -e development

# Get block information
graphsense-cli db block info -e development -c btc --height 100000

# Query logs (for Ethereum-based chains)
graphsense-cli db logs -e development -c eth --from-block 1000000 --to-block 1000100
```

### Schema Operations

Create and validate database schemas.

```bash
# Show schema options
graphsense-cli schema --help

# Create database schema for a currency
graphsense-cli schema create -e dev -c btc

# Validate existing schema
graphsense-cli schema validate -e dev -c btc

# Show expected schema for currency
graphsense-cli schema show-by-currency btc

# Show schema by type (utxo/account)
graphsense-cli schema show-by-schema-type utxo
```

### Data Ingestion

Ingest raw cryptocurrency data from nodes.

```bash
# Show ingestion options
graphsense-cli ingest --help

# Ingest blocks from cryptocurrency node
graphsense-cli ingest from-node \
    -e dev \
    -c btc \
    --start-block 0 \
    --end-block 1000 \
    --create-schema

# Ingest with custom batch size
graphsense-cli ingest from-node \
    -e dev \
    -c eth \
    --start-block 1000000 \
    --end-block 1001000 \
    --batch-size 100
```

### Delta Updates

Update transformed keyspace from raw keyspace.

```bash
# Show delta update options
graphsense-cli delta-update --help

# Check update status
graphsense-cli delta-update status -e dev -c btc

# Perform delta update
graphsense-cli delta-update update -e dev -c btc

# Validate delta update consistency
graphsense-cli delta-update validate -e dev -c btc

# Patch exchange rates for specific blocks
graphsense-cli delta-update patch-exchange-rates \
    -e dev \
    -c btc \
    --start-block 100000 \
    --end-block 200000
```

### Exchange Rates

Fetch and ingest exchange rates from various sources.

```bash
# Show exchange rate options
graphsense-cli exchange-rates --help

# Fetch from CoinDesk
graphsense-cli exchange-rates coindesk -e dev -c btc

# Fetch from CoinMarketCap (requires API key in config)
graphsense-cli exchange-rates coinmarketcap -e dev -c btc
```

### Monitoring

Monitor GraphSense infrastructure health and state.

```bash
# Show monitoring options
graphsense-cli monitoring --help

# Get database summary
graphsense-cli monitoring get-summary -e dev

# Get summary for specific currency
graphsense-cli monitoring get-summary -e dev -c btc

# Send notifications to configured handlers
graphsense-cli monitoring notify \
    --topic "database-update" \
    --message "BTC ingestion completed"
```

### Event Watching (Alpha)

Watch for cryptocurrency events and generate notifications.

```bash
# Show watch options
graphsense-cli watch --help

# Watch for money flows on specific addresses
graphsense-cli watch money-flows \
    -e dev \
    -c btc \
    --address 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa \
    --threshold 1000000  # satoshis
```

### File Conversion Tools

Convert between different file formats.

```bash
# Show conversion options
graphsense-cli convert --help
```

## Configuration

GraphSense-lib uses a YAML configuration file that defines database connections and environment settings. Default locations: `./.graphsense.yaml`, `~/.graphsense.yaml`.

### Generate Configuration Template

```bash
graphsense-cli config template > ~/.graphsense.yaml
```

### Example Configuration Structure

```yaml
# Optional: default environment to use
default_environment: dev

environments:
  dev:
    # Cassandra cluster configuration
    cassandra_nodes: ["localhost"]
    port: 9042
    # Optional authentication
    # username: "cassandra"
    # password: "cassandra"

    # Currency/keyspace configurations
    keyspaces:
      btc:
        raw_keyspace_name: "btc_raw"
        transformed_keyspace_name: "btc_transformed"
        schema_type: "utxo"

        # Node connection for ingestion
        ingest_config:
          node_reference: "http://localhost:8332"
          # Optional authentication for node
          # username: "rpcuser"
          # password: "rpcpassword"

        # Keyspace setup for schema creation
        keyspace_setup_config:
          raw:
            replication_config: "{'class': 'SimpleStrategy', 'replication_factor': 1}"
          transformed:
            replication_config: "{'class': 'SimpleStrategy', 'replication_factor': 1}"

      eth:
        raw_keyspace_name: "eth_raw"
        transformed_keyspace_name: "eth_transformed"
        schema_type: "account"

        ingest_config:
          node_reference: "http://localhost:8545"

        keyspace_setup_config:
          raw:
            replication_config: "{'class': 'SimpleStrategy', 'replication_factor': 1}"
          transformed:
            replication_config: "{'class': 'SimpleStrategy', 'replication_factor': 1}"

  prod:
    cassandra_nodes: ["cassandra1.prod", "cassandra2.prod", "cassandra3.prod"]
    username: "gs_user"
    password: "secure_password"

    keyspaces:
      btc:
        raw_keyspace_name: "btc_raw"
        transformed_keyspace_name: "btc_transformed"
        schema_type: "utxo"

        ingest_config:
          node_reference: "http://bitcoin-node.internal:8332"

        keyspace_setup_config:
          raw:
            replication_config: "{'class': 'NetworkTopologyStrategy', 'datacenter1': 3}"
          transformed:
            replication_config: "{'class': 'NetworkTopologyStrategy', 'datacenter1': 3}"

# Optional: Slack notification configuration
slack_topics:
  database-update:
    hooks: ["https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"]

  payment_flow_notifications:
    hooks: ["https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"]

# Optional: API keys for external services
coingecko_api_key: ""
coinmarketcap_api_key: "YOUR_CMC_API_KEY"

# Optional: cache directory for temporary files
cache_directory: "~/.graphsense/cache"
```

## Advanced Features

### Tagpack Management

GraphSense-lib includes comprehensive tagpack management tools (formerly standalone tagpack-tool). For detailed documentation, see [Tagpack README](tagpacks/docs/README.md).

```bash
# Validate tagpacks
graphsense-cli tagpack-tool tagpack validate /path/to/tagpack

# Insert tagpack into tagstore
graphsense-cli tagpack-tool insert \
    --url "postgresql://user:pass@localhost/tagstore" \
    /path/to/tagpack

# Show quality measures
graphsense-cli tagpack-tool quality show-measures \
    --url "postgresql://user:pass@localhost/tagstore"
```

### Tagstore Operations

```bash
# Initialize tagstore database
graphsense-cli tagstore init

# Initialize with custom database URL
graphsense-cli tagstore init --db-url "postgresql://user:pass@localhost/tagstore"

# Get DDL SQL for manual setup
graphsense-cli tagstore get-create-sql
```

### Cross-chain Analysis

```python
from graphsenselib.db.asynchronous.services import AddressesService

async def cross_chain_analysis():
    addresses_service = AddressesService(db, rates_service, config, logger)

    # Get cross-chain related addresses
    related = await addresses_service.get_cross_chain_pubkey_related_addresses(
        "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
    )

    for addr in related:
        print(f"Network: {addr.network}, Address: {addr.address}")
```

### Function Call Parsing

```python
from graphsenselib.utils.function_call_parser import parse_function_call

# Parse Ethereum function calls
function_signatures = {
    "0xa9059cbb": [{
        "name": "transfer",
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"}
        ]
    }]
}

parsed = parse_function_call(tx_input_bytes, function_signatures)
if parsed:
    print(f"Function: {parsed['name']}")
    print(f"Parameters: {parsed['parameters']}")
```

## Installation

### From PyPI

```bash
# Install with all features
pip install graphsense-lib[all]
```

### From Source

```bash
# Clone and install
git clone https://github.com/graphsense/graphsense-lib.git
cd graphsense-lib
make install

# Or using pip directly
pip install .
```

## Development

**Important:** Python 3.12 is currently not supported. Please use Python 3.11.

### Setup Development Environment

```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Initialize development environment
make dev

# Install in development mode
make install-dev
```

### Code Quality and Testing

Before committing, please format, lint, and test your code:

```bash
# Format code
make format

# Lint code
make lint

# Run fast tests
make test

# Or run all steps at once
make pre-commit
```

For comprehensive testing:

```bash
# Run complete test suite (including slow tests)
make test-all
```

### Documentation

```bash
# Generate documentation
make docs
```

**Note:** Building docs requires Python development headers. See [this StackOverflow answer](https://stackoverflow.com/questions/21530577/fatal-error-python-h-no-such-file-or-directory) for installation instructions.

### Release Process

1. Update CHANGELOG.md with new features and fixes
2. Update version numbers (RELEASESEM and RELEASE) in the main Makefile
3. Create and push tag:

```bash
make tag-version
```

## Troubleshooting

### OpenSSL Errors

Some components use OpenSSL hash functions that aren't available by default in OpenSSL 3.0+ (e.g., ripemd160). This can cause test suite failures. To fix this, enable legacy providers in your OpenSSL configuration. See the "fix openssl legacy mode" step in `.github/workflows/run_tests.yaml` for an example.

### Common Issues

1. **Connection Refused**: Verify Cassandra is running and accessible
2. **Schema Validation Errors**: Ensure database schema matches expected version
3. **Import Errors**: Install with `[all]` option for complete feature set
4. **Python Version**: Use Python 3.11 (3.12 not currently supported)

### Getting Help

- Check [GitHub Issues](https://github.com/graphsense/graphsense-lib/issues)
- Review [GraphSense Documentation](https://graphsense.github.io/)
- Use `--help` with any CLI command for detailed usage information
- For tagpack-specific issues, see [Tagpack Documentation](tagpack/docs/README.md)

## License

See LICENSE file for licensing details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `make pre-commit` to ensure code quality
5. Submit a pull request

---

**GraphSense** - Open Source Crypto Analytics Platform
Website: https://graphsense.github.io/
