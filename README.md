# graphsense-lib

[![Test and Build Status](https://github.com/graphsense/graphsense-lib/actions/workflows/run_tests.yaml/badge.svg)](https://github.com/graphsense/graphsense-lib/actions)

A central repository for python utility functions and everything that deals with the graphsense backend. Its CLI interface can be used to control important graphsense maintainance tasks.

## Use

### As a library

Access the graphsense cassandra db. This requires a graphsense config (see Modules/Config)
```python3
from graphsenselib.db import DbFactory

with DbFactory().from_config(env, currency) as db:
    print(db.transformed.get_highest_block())
```

Alternatively, the database can also be accessed without relying on the graphsense config file.
```python
from graphsenselib.db import DbFactory

with DbFactory().from_name(
        raw_keyspace_name="eth_raw",
        transformed_keyspace_name="eth_tranformed",
        schema_type="utxo",
        cassandra_nodes = ["test.cassandra1", "test.cassandra2"]
    ) as db:
    print(db.transformed.get_highest_block())
```

### From the commandline.

Graphsenslib exposes a commandline interface - graphsence-cli.

Help can be viewed by:
```bash
> graphsense-cli --help
Usage: graphsense-cli [OPTIONS] COMMAND [ARGS]...

  Commandline interface of graphsense-lib

  graphsense-cli exposes many tools and features to manager your graphsense
  crypto-analytics database.

Options:
  -v, --verbose  One v for warning, two for info etc.
  --help         Show this message and exit.

Commands:
  config          Inspect the current configuration of graphsenselib.
  convert         Useful file convertions tools for the graphsense...
  db              Query related functions.
  delta-update    Updating the transformed keyspace from the raw keyspace.
  exchange-rates  Fetching and ingesting exchange rates.
  ingest          Ingesting raw cryptocurrency data from nodes into the...
  monitoring      Tools to monitor the graphsense infrastructure.
  schema          Creating and validating the db schema.
  version         Display the current version.
  watch           Commands for permanently watching cryptocurrency events.

  GraphSense - https://graphsense.github.io/
```


## Modules

### DB

Assess the current database state.

```bash
> graphsense-cli db --help
Usage: graphsense-cli db [OPTIONS] COMMAND [ARGS]...

  DB-management related functions.

Options:
  --help  Show this message and exit.

Commands:
  block  Special db query functions regarding blocks.
  logs   Special db query functions regarding logs.
  state  Summary Prints the current state of the graphsense database.

```

### Schema

Extracting and validating the current database schema.

```bash
> graphsense-cli schema --help
Usage: graphsense-cli schema [OPTIONS] COMMAND [ARGS]...

  Creating and validating the db schema.

Options:
  --help  Show this message and exit.

Commands:
  create               Creates the necessary graphsense tables in Cassandra.
  show-by-currency     Prints the current db schema expected from
                       graphsenselib
  show-by-schema-type  Prints the current db schema expected from
                       graphsenselib
  validate             Validates if the expected schema matches the database.

```

### Ingest

Loading raw data from cryptocurrency nodes into the graphsense raw keyspace.

```bash
> graphsense-cli ingest --help
Usage: graphsense-cli ingest [OPTIONS] COMMAND [ARGS]...

  Ingesting raw cryptocurrency data from nodes into the graphsense database

Options:
  --help  Show this message and exit.

Commands:
  from-node  Ingests cryptocurrency data form the client/node to the...
```


### Delta Update

Updates the data in the transformed keyspace based on the raw keyspace

```bash
> graphsense-cli delta-update --help
Usage: graphsense-cli delta-update [OPTIONS] COMMAND [ARGS]...

  Updating the transformed keyspace from the raw keyspace.

Options:
  --help  Show this message and exit.

Commands:
  patch-exchange-rates  Rewrites the transformed exchange rate at a...
  status                Shows the status of the delta updater.
  update                Updates transformed from raw, if possible.
  validate              Validates the current delta update status and its...
```

### Exchange Rates

Fetches exchange_rates from different sources and adds them to the database.

```bash
> graphsense-cli exchange-rates --help
Usage: graphsense-cli exchange-rates [OPTIONS] COMMAND [ARGS]...

  Fetching and ingesting exchange rates.

Options:
  --help  Show this message and exit.

Commands:
  coindesk       From coindesk.
  coinmarketcap  From coinmarketcap.
```


### Config

Shows the currently used configuration. Configurations reside per default in ~/graphsense.yaml. It contains the cassandra and keyspace configurations for different environments. A template config can be generated via ```graphsense-cli config template```

```bash
> graphsense-cli config --help
Usage: graphsense-cli config [OPTIONS] COMMAND [ARGS]...

  Inspect the current configuration of graphsenselib.

Options:
  --help  Show this message and exit.

Commands:
  path      Prints the path where the config is loaded from.
  show      Prints the configuration used in the environment.
  template  Generates a configuration template.
```

### Monitoring

Helpful functions to keep an eye on the state and health of your graphsense database state.

```bash
> graphsense-cli monitoring --help
Usage: graphsense-cli monitoring [OPTIONS] COMMAND [ARGS]...

  Tools to monitor the graphsense infrastructure.

Options:
  --help  Show this message and exit.

Commands:
  get-summary  Receives a summary record of the current database state.
  notify       Sends a message to the configured handlers (e.g. a slack
               channel) by topic.

```

### Watch (Alpha)

Provides functions to generate notifications on cryptocurrency events like value flows on certain addresses.

```bash
> graphsense-cli watch --help
Usage: graphsense-cli watch [OPTIONS] COMMAND [ARGS]...

  Commands for permanently watching cryptocurrency events.

Options:
  --help  Show this message and exit.

Commands:
  money-flows  Watches for movements money flows and generates...

```

## Install

To install run

```bash
make install
```
or
```bash
pip install .
```

## Development


it is advised to use a virtual environment (venv) for development. Run the following command to initialize one
```bash
    > python3 -m venv .venv
```

and activate it (in bash) using

```bash
    > source .venv/bin/activate
```
 For more information refer to ([venv](https://docs.python.org/3/library/venv.html)). Run

```bash
    > make dev
```

to initialize the dev environment.
If you want to install graphsenselib in development mode run

```bash
    > make install-dev
```

Before committing anything to the repository please format, lint and test your code in that order. Fix all linter warnings and make sure all test are passing before a commit.

Use the following commands for that:
```bash
    > make format
    > make lint
    > make test
```

or equivalently run
```bash
    > make pre-commit
```

Some slow tests are excluded when running make test. Occasionally, one should run
```bash
    > make test-all
```

to run the entire test-suite.

Linting and formatting should be automatically executed on every git commit, using pre-commit.

To create the documentation please run:
```bash
    > make docs
```

Creating the docs need python dev dependencies to build see ([Stackoverflow](https://stackoverflow.com/questions/21530577/fatal-error-python-h-no-such-file-or-directory}))

### Tagging a release

To tag a new release please update the changelog first. Afterwards, update the Version numbers RELEASESEM and RELEASE in the main Makefile.

To apply the tags run
```bash
    > make tag-version
```
