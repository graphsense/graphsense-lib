# graphsense-lib
A central repository for python utility functions and everything that deals with the graphsense backend. Its CLI interface can be used to control important graphsense maintainance tasks.

## Use

### As a library

Access the graphsense cassandra db. This requires a graphsense config (see Modules -> Config)
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

Graphsenslib exposes an commandline interface graphsence-cli.

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
  db              DB-management related functions.
  delta-update    Updating the transformed keyspace from the raw keyspace.
  exchange-rates  Fetching and ingesting exchange rates.
  schema          Creating and validating the db schema.
  version         Display the current version.

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
  state  Prints the current state of the graphsense database.

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

### Delta Update

Updates the data in the transformed keyspace based on the raw keyspace

```bash
> graphsense-cli delta-update --help
Usage: graphsense-cli delta-update [OPTIONS] COMMAND [ARGS]...

  Updating the transformed keyspace for the raw keyspace.

Options:
  --help  Show this message and exit.

Commands:
  status    Shows the status of the delta updater.
  update    Updates transformed from raw, if possible.
  validate  Validates the current delta update status and its history.
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

### Ingest

To be implemented. Should deal with ingesting data into the raw keyspaces.


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

Creating the docs need python dev dependencies to build see ([Stackoverflow_](https://stackoverflow.com/questions/21530577/fatal-error-python-h-no-such-file-or-directory}))
