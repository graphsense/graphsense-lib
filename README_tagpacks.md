# GraphSense TagPack Management Tool

[![Test and Build Status](https://github.com/graphsense/graphsense-tagpack-tool/actions/workflows/test_and_build.yaml/badge.svg)](https://github.com/graphsense/graphsense-tagpack-tool/actions) [![PyPI version](https://badge.fury.io/py/tagpack-tool.svg)](https://badge.fury.io/py/graphsense-lib) [![Python](https://img.shields.io/pypi/pyversions/tagpack-tool)](https://pypi.org/project/tagpack-tool/) [![Downloads](https://static.pepy.tech/badge/tagpack-tool)](https://pepy.tech/project/tagpack-tool)

This repository provides a command line tool for managing [GraphSense TagPacks](https://github.com/graphsense/graphsense-tagpacks/wiki/GraphSense-TagPacks). It can be used for

1. [validating TagPacks against the TagPack schema](#validation)
2. [finding suitable actors for tags](#actors-for-tags-and-tagpacks)
3. [validating ActorPacks against the ActorPack schema](#actorpack_validation)
4. [handling taxonomies and concepts](#taxonomies)
5. [ingesting TagPacks and related data into a TagStore](#tagstore)
6. [calculating the quality of the tags in the TagStore](#quality)

Please note that the last feature requires (installation of) a [Postgresql](https://www.postgresql.org/) database.

# Quickstart

## Installation

    pip install git+https://github.com/graphsense/graphsense-tagpack-tool.git

## Prepare a TagStore database

Check out the options as described [below](#prerequisites-tagstore---postgresql-database).

## Sync TagPack repositories

Create a file containing the repositories you want to manage, one repository per line (commenting out lines is possible):

    git@github.com:graphsense/graphsense-tagpacks.git develop public
    # git@github.com:mycompany/graphsense-tagpacks-special.git master

If you want to import a certain branch add the branch name separated by a white-space as shown above. To indicate that the repository should be imported to seen by everybody then add the keyword after the branch specification. If no branch or public keyword is specified the default branch is used and the tags are treated as private.

Then run

    tagpack-tool sync -r ./tagpack-repos.config

to populate the TagStore with Actors and TagPacks.

Re-run the command to add newly added tagpack files from the repositories.

Add the `--force` option to re-insert TagPacks.

# Step-by-step overview

## Validate a TagPack <a name="validation"></a>

Validate a single TagPack file

    tagpack-tool tagpack validate tests/testfiles/simple/ex_addr_tagpack.yaml

Recursively validate all TagPacks in (a) given folder(s).

    tagpack-tool tagpack validate tests/testfiles/

TagPacks are validated against the [tagpack schema](src/tagpack/conf/tagpack_schema.yaml).

Confidence settings are validated against a set of acceptable [confidence](src/tagpack/db/confidence.csv) values.

## Actors for tags and TagPacks

[Actors](https://github.com/graphsense/graphsense-tagpacks/wiki/Graphsense-Actors) are defined in a curated actor tagpack.

It is highly encouraged to add suitable actors to TagPacks whenever possible,
and the tagpack-tool offers support for doing so.

### List suitable actors for a tag

For a specific tag string, actor suggestions can be listed by calling

    tagpack-tool tagpack suggest_actors <my_tag>

and if desired, the number of results can be restricted by adding the ``--max`` parameter

    tagpack-tool tagpack suggest_actors --max 1 <my_tag>

### Interactive TagPack update

It is also possible to interactively **update** an existing TagPack file with actors:

    tagpack-tool tagpack add_actors path/to/tagpack.yaml

or go through entire directories of TagPack files:

    tagpack-tool tagpack add_actors path/to/tagpacks

File by file, for each label, the tagpack-tool will suggest suitable actors if any are found:

    Choose for instadapp_InstaCompoundMapping
        0 instadapp
        1 compound
        ENTER to skip
    Your choice: 0

The ``--max`` option is available again to limit the number of candidate suggestions:

    tagpack-tool tagpack add_actors --max 1 path/to/tagpacks

If any actors have been selected, an updated TagPack is written that contains the users' selected actors:

    Writing updated Tagpack defi-protocols_instadapp_with_actors.yaml


## Validate an ActorPack <a name="actorpack_validation"></a>

Validate a single ActorPack file

    tagpack-tool actorpack validate tests/testfiles/actors/ex_actorpack.yaml

Recursively validate all TagPacks in (a) given folder(s).

    tagpack-tool actorpack validate tests/testfiles/actors/

Actorpacks are validated against the [actorpack schema](src/tagpack/conf/actorpack_schema.yaml).

Values in the field jurisdictions are validated against a set of [country codes](src/tagpack/db/countries.csv).

## View available taxonomies and concepts <a name="taxonomies"></a>

List configured taxonomy keys and URIs

    tagpack-tool taxonomy list

Fetch and show concepts of a specific remote/local taxonomy (referenced by key: concept, confidence, country)

    tagpack-tool taxonomy show concept

## Ingest TagPacks and related data into a TagStore <a name="tagstore"></a>

### Prerequisites: TagStore - PostgreSQL database

#### Option 1: Start a dockerized PostgreSQL database

- [Docker][docker], see e.g. https://docs.docker.com/engine/install/
- Docker Compose: https://docs.docker.com/compose/install/

First, copy `docker/env.template`
to `.env` and fill the fields `POSTGRES_PASSWORD` and `POSTGRES_PASSWORD_TAGSTORE`.

Run

    cp docker/postgres-conf.sql.template postgres-conf.sql

and modify the configuration parameters to your requirements. If no special config is needed an emtpy file is also valid.

    touch postgres-conf.sql

Start a PostgreSQL instance using Docker Compose:

    docker-compose up -d

This will automatically create the database with the nessesary permissions for the `POSTGRES_USER_TAGSTORE`.

    GS_TAGSTORE_DB_URL='postgresql://${POSTGRES_USER_TAGSTORE}:${POSTGRES_PASSWORD_TAGSTORE}@{HOST}:{PORT}/{DBNAME}' tagstore init

then generates the nessesary tables, views etc. and populates the database with some default entries.


#### Option 2: Use an existing PostgreSQL database

Create the schema and tables in a PostgreSQL instance of your choice also use `tagstore init` as above, make sure the user specified in the tagstore url has the permission to create tables and views.

### Export .env variables

tagpack-tool is able to use the variables configured in the `.env` file to avoid specifying the parameter `--url` each time it connects to the database. The `--url` parameter will override the environment values if needed. To export the environment variables in `.env` from a linux shell (e.g. bash), first use:

    source .env
    export $(grep --regexp ^[A-Z] .env | cut -d= -f1)

Or just export each variable using:

    export POSTGRES_USER=VALUE
    export POSTGRES_PASSWORD=VALUE
    export POSTGRES_HOST=VALUE
    export POSTGRES_DB=VALUE

    GS_TAGSTORE_DB_URL=value # For the newer tagstore cli

Then call tagpack-tool.

### Create and display a configuration file that defines which taxonomies to use

To create a default configuration `config.yaml` file from scratch - i.e. when config.yaml does not exist - use:

    tagpack-tool config

If a config.yaml already exists, it will not be replaced.

Show the contents of the config file:

    tagpack-tool config -v

To use a specific config file pass the file's location:

    tagpack-tool --config  path/to/config.yaml config

### Initialize the tagstore database

To initialize the database with all the taxonomies needed for ingesting the tagpacks, use:

    tagpack-tool tagstore init

set the db to connect to via the environment variable

    GS_TAGSTORE_DB_URL='postgresql://${POSTGRES_USER_TAGSTORE}:${POSTGRES_PASSWORD_TAGSTORE}@localhost:5432/tagstore'

### Ingest taxonomies and confidence scores
To insert all configured taxonomies at once, simply omit taxonomy name

    tagpack-tool taxonomy insert

Not tagpack-tool sync inserts taxonomies automatically

### Ingest TagPacks

Insert a single TagPack file or all TagPacks from a given folder

    tagpack-tool tagpack insert tests/testfiles/simple/ex_addr_tagpack.yaml
    tagpack-tool tagpack insert tests/testfiles/simple/multiple_tags_for_address.yaml
    tagpack-tool tagpack insert tests/testfiles/

By default, TagPacks are declared as non-public in the database.
For public TagPacks, add the `--public` flag to your arguments:

    tagpack-tool tagpack insert --public tests/testfiles/

If you try to insert tagpacks that already exist in the database, the ingestion process will be stopped.

To force **re-insertion** (if tagpack file contents have been modified), add the `--force` flag to your arguments:

    tagpack-tool tagpack insert --force tests/testfiles/

To ingest **new** tagpacks and **skip** over already ingested tagpacks, add the `--add_new` flag to  your arguments:

    tagpack-tool tagpack insert --add_new tests/testfiles/

By default, trying to insert tagpacks from a repository with **local** modifications will **fail**.
To force insertion despite local modifications, add the ``--no_strict_check`` command-line parameter

    tagpack-tool tagpack insert --no_strict_check tests/testfiles/

By default, tagpacks in the TagStore provide a backlink to the original tagpack file in their remote git repository.
To write local file paths instead, add the ``--no_git`` command-line parameter

    tagpack-tool tagpack insert --no_git --add_new tests/testfiles/

### Ingest ActorPacks

Insert a single ActorPack file or all ActorPacks from a given folder:

    tagpack-tool actorpack insert tests/testfiles/simple/ex_addr_actorpack.yaml
    tagpack-tool actorpack insert tests/testfiles/

You can use the parameters `--force`, `--add_new`, `--no_strict_check` and `--no_git` options in the same way as with the `tagpack` command.

### Align ingested attribution tags with GraphSense cluster Ids

The final step after inserting a tagpack is to fetch the corresponding
Graphsense cluster mapping ids for the crypto addresses in the tagpack.

Copy `src/tagpack/conf/ks_map.json.template` to `ks_map.json` and edit the file to
suit your Graphsense setup.

Then fetch the cluster mappings from your Graphsense instance and insert them
into the tagstore database:

    tagpack-tool tagstore insert_cluster_mappings -d $CASSANDRA_HOST -f ks_map.json

To update ALL cluster-mappings in your tagstore, add the `--update` flag:

    tagpack-tool tagstore insert_cluster_mappings --update -d $CASSANDRA_HOST -f ks_map.json

### Remove duplicate tags

Different tagpacks may contain identical tags - the same label and source for a particular address.
To remove such redundant information, run

    tagpack-tool tagstore remove_duplicates

### IMPORTANT: Keeping data consistency after tagpack insertion

After all required tagpacks have been ingested, run

    tagpack-tool tagstore refresh_views

to update all materialized views.
Depending on the amount of tags contained in the tagstore, this may take a while.


## Calculate the quality of the tags in the TagStore <a name="quality"></a>

To assess on the quality of address tags we define a quality measure.
For an address tag, it is calculated as the **weighted similarity distance** between all pairs of distinct tags assigned to the same address.

An address with a unique tag has a quality equal to 1, while an address with several similar tags has a quality close to 0.

To calculate the quality measure for all the tags in the database, run:

    tagpack-tool quality calculate

To show the quality measures of all the tags in the database, or those of a specific crypto-currency, run:

    tagpack-tool quality show [--network [BCH|BTC|ETH|LTC|ZEC|...]]

## Show tagstore contents/contributions

To list all tagpack creators and their contributions to a tagstore's content use:

    tagpack-tool tagstore show_composition

# REST API

To provide REST endpoints for accessing tags, start the service

```
make run
```

and check out http://localhost:8000/docs

# For developers

## Working in development / testing mode

    git clone https://github.com/graphsense/graphsense-tagpack-tool.git
    cd graphsense-tagpack-tool

### Using Pip locally

Create and activate a python environment for required dependencies and activate it

#### Venv

    python3 -m venv venv
    source venv/bin/activate

#### Conda

    conda create -n tagpack-tool
    conda activate tagpack-tool


Install package and dependencies in local environment

    conda install pip
    make install-dev

### Linting and Formatting

The code in this repos will be autoformated via black and linted via a pre-commit hook. To manually format and lint the code run:

    make format && make pre-commit

Or linting via tox

    tox -l lint

### Build for Publishing

    make build

### Create Html Docs

    make docs

### Testing

Run tests

    make test

Or via tox

    uv run tox

Check test coverage (optional)

    make test
    coverage report

Use [act][act] to check if test via [Github action](https://github.com/features/actions) pass.

[act]: https://github.com/nektos/act
[docker]: https://www.docker.com
[graphsense-transformation]: https://github.com/graphsense/graphsense-transformation
