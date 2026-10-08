# GraphSense TagPack Management Tool

[![Test and Build Status](https://github.com/graphsense/graphsense-lib/actions/workflows/run_tests.yaml/badge.svg)](https://github.com/graphsense/graphsense-lib/actions) [![PyPI version](https://badge.fury.io/py/graphsense-lib.svg)](https://badge.fury.io/py/graphsense-lib) [![Python](https://img.shields.io/pypi/pyversions/graphsense-lib)](https://pypi.org/project/graphsense-lib/) [![Downloads](https://static.pepy.tech/badge/graphsense-lib)](https://pepy.tech/project/graphsense-lib)

`graphsense-cli tagpack-tool`, part of [graphsense-lib](https://github.com/graphsense/graphsense-lib), manages [GraphSense TagPacks](https://github.com/graphsense/graphsense-tagpacks/wiki/GraphSense-TagPacks). It can be used for

1. [validating TagPacks against the TagPack schema](#validation)
2. [finding suitable actors for tags](#actors-for-tags-and-tagpacks)
3. [validating ActorPacks against the ActorPack schema](#actorpack_validation)
4. [handling taxonomies and concepts](#taxonomies)
5. [ingesting TagPacks and related data into a TagStore](#tagstore)
6. [checking the quality of the tags in the TagStore](#quality)

Suggesting actors and the last two features require a [PostgreSQL](https://www.postgresql.org/) database with a TagStore.

# Quickstart


## Prepare a TagStore database

Check out the options as described [below](#prerequisites-tagstore---postgresql-database).

## Sync TagPack repositories

Create a file containing the repositories you want to manage, one repository per line (commenting out lines is possible):

    git@github.com:graphsense/graphsense-tagpacks.git master public
    # git@github.com:mycompany/graphsense-tagpacks-special.git master

To import a certain branch, add the branch name separated by a space as shown above. To make the repository's tags visible to everybody, add the keyword `public` after the branch. Without a branch the default branch is used; without `public` the tags are private.

Then run

    graphsense-cli tagpack-tool sync -r ./tagpack-repos.config

to populate the TagStore with Actors and TagPacks.

Re-run the command to add new or changed tagpack files from the repositories.

Add the `--force` option to re-insert TagPacks.

# Step-by-step overview

## Validate a TagPack <a name="validation"></a>

Validate a single TagPack file

    graphsense-cli tagpack-tool tagpack validate tests/testfiles/simple/ex_addr_tagpack.yaml

Recursively validate all TagPacks in (a) given folder(s).

    graphsense-cli tagpack-tool tagpack validate tests/testfiles/

TagPacks are validated against the [tagpack schema](../../src/graphsenselib/tagpack/conf/tagpack_schema.yaml).

Confidence settings are validated against a set of acceptable [confidence](../../src/graphsenselib/tagpack/db/confidence.csv) values.

Validate many packs in parallel with `--jobs N` (`0` = one process per CPU).
With more than one job every pack is validated and every failure reported; the
default `1` stops at the first invalid pack. Each process holds one pack in
memory, up to 1–2 GB for packs of several 100k tags, so prefer a small number
on a laptop.

    graphsense-cli tagpack-tool tagpack validate --jobs 4 packs/

### How TagPack files are parsed <a name="yaml-parsers"></a>

Two YAML parsers read TagPack files, the same way for `validate` and for
`insert`/`sync`:

- **PyYAML** for every pack in a directory tree with a `header.yaml`, for packs
  with an `!include`, and for all packs with `--use-pyyaml`.
- **rapidyaml** (faster) for all other packs, if it is installed.

They read most files identically, but not all. rapidyaml hands its result over
as JSON, which changes some unquoted values:

| In the file (unquoted) | PyYAML | rapidyaml |
| --- | --- | --- |
| `12E34` or `12e345678` (digits with an `e`, no dot) | text `"12E34"` | a number, or `inf` if too large |
| `inf`, `nan` | text `"inf"`, `"nan"` | text `".inf"`, `".nan"` |
| `.inf`, `.nan` | the float `inf`, `nan` | text `".inf"`, `".nan"` |
| `yes`, `no`, `on`, `off` | `True` / `False` | text |
| `0x1A`, `012`, `1_000` | the numbers 26, 10, 1000 | text |
| `~` | `None` | text `"~"` |
| an alias `*id001` (written by `yaml.dump` for repeated lists or dicts) | the anchored value | the text `"*id001"` |
| `'2025-03-24'` (a quoted date) | text (`lastmod`: read as midnight that day) | a date |

Unquoted `true`/`false`, `null` and `YYYY-MM-DD` dates are read the same by
both. A comparison over a large repository also found a few `context` fields
whose JSON text differed; the cause is not yet known.

To make a pack read the same by both parsers:

- quote every string that is not plain text, in particular addresses, hashes
  and labels that look like numbers;
- write booleans as `true`/`false`;
- generators: dump without anchors, e.g. with a `yaml.SafeDumper` subclass
  whose `ignore_aliases` returns `True`.

Check a pack with both: `validate` and `validate --use-pyyaml` must agree.

## Actors for tags and TagPacks

[Actors](https://github.com/graphsense/graphsense-tagpacks/wiki/Graphsense-Actors) are defined in a curated ActorPack, [`actors/graphsense.actorpack.yaml`](https://github.com/graphsense/graphsense-tagpacks/blob/master/actors/graphsense.actorpack.yaml) in graphsense-tagpacks.

It is highly encouraged to add suitable actors to TagPacks whenever possible,
and the tagpack-tool offers support for doing so. Both commands below look up
the actors in a TagStore (`-u`/`--url` or the `POSTGRES_*` variables, see
[below](#export-env-variables)).

### List suitable actors for a tag

For a specific tag string, actor suggestions can be listed by calling

    graphsense-cli tagpack-tool tagpack suggest-actors <my_tag>

and if desired, the number of results can be restricted by adding the ``--max`` parameter

    graphsense-cli tagpack-tool tagpack suggest-actors --max 1 <my_tag>

### Interactive TagPack update

It is also possible to interactively **update** an existing TagPack file with actors:

    graphsense-cli tagpack-tool tagpack add-actors path/to/tagpack.yaml

or go through entire directories of TagPack files:

    graphsense-cli tagpack-tool tagpack add-actors path/to/tagpacks

File by file, for each label, the tagpack-tool will suggest suitable actors if any are found:

    Choose for instadapp_InstaCompoundMapping
        0 instadapp
        1 compound
        ENTER to skip
    Your choice: 0

The ``--max`` option is available again to limit the number of candidate suggestions:

    graphsense-cli tagpack-tool tagpack add-actors --max 1 path/to/tagpacks

If any actors have been selected, an updated TagPack is written next to the original, with the selected actors (`--inplace` overwrites the original instead):

    Writing updated Tagpack defi-protocols_instadapp_with_actors.yaml


## Validate an ActorPack <a name="actorpack_validation"></a>

Validate a single ActorPack file

    graphsense-cli tagpack-tool actorpack validate tests/testfiles/actors/ex.actorpack.yaml

Recursively validate all ActorPacks in (a) given folder(s).

    graphsense-cli tagpack-tool actorpack validate tests/testfiles/actors/

Actorpacks are validated against the [actorpack schema](../../src/graphsenselib/tagpack/conf/actorpack_schema.yaml).

Values in the field jurisdictions are validated against a set of [country codes](../../src/graphsenselib/tagpack/db/countries.csv).

### Aliases

`aliases` lists other ids or spellings under which **this same actor
entry** appears in tagpacks, e.g. after a rename or for common variants:

```yaml
- id: woonetwork
  label: Woo Network
  aliases:
  - woofi
  - woox
  ...
```

When a tagpack is inserted, a tag whose `actor` is an alias is stored with
the actor's id (`woofi` → `woonetwork`), so all its tags end up on one
actor. `aliases` can be written at actor level (as above) or inside
`context`; both are read. An alias must not be the id of another actor:
validation rejects that ("Collision detected: Actor ids and aliases share
…").

### Relations between actors

Four optional `context` fields record that two actor entries may tag the
same addresses without that being a conflict. Unlike aliases, both entries
stay separate actors with their own tags:

- `same_as`: the other actor is the same organisation, e.g. after a rebrand
  or for a duplicate entry. Use it instead of deleting the old entry: tags
  still refer to it by id.
- `sub_service_of`: this actor is a product or division of another one, i.e.
  the same organisation, e.g. an exchange's mining pool. Takes one actor id.
- `nested_in`: this actor is a separate organisation that runs on the
  addresses or accounts of the listed ones, e.g. an exchange that keeps its
  funds with a custodian, or a service that operates through accounts at an
  exchange. Takes a list.
- `related_actors`: the other actor is a different organisation that
  legitimately appears on the same addresses for another reason, e.g. the
  custodian of a wrapped token and the token.

```yaml
- id: oldname
  label: Old Name
  ...
  context:
    same_as:
    - newname         # rebranded
- id: exchangepool
  label: Exchange Pool
  ...
  context:
    sub_service_of: exchange   # the exchange's own mining pool
- id: smallexchange
  label: Small Exchange
  ...
  context:
    nested_in:
    - custodian       # keeps its funds with this custodian
    notes:
    - 'nested_in custodian: deposits swept into the custodian''s wallets'
- id: wrappedtoken
  label: Wrapped Token
  ...
  context:
    related_actors:
    - custodian       # holds the token's reserves
```

Relations must name **actor ids, not aliases**: tags are stored with the
resolved actor id, so a relation to an alias never matches a tag.

Declaring a pair on one of the two actors is enough; `sub_service_of` and
`nested_in` go on the product or the nested service. Validation rejects an
actor that lists itself, lists the same actor in both `same_as` and
`related_actors` or in both `sub_service_of` and `nested_in`, gives
`sub_service_of` more than one id, or makes `sub_service_of` go round in a
cycle; it warns about ids that are not in the same actorpack (they may be
defined in another one). Actors declared `same_as` or `sub_service_of` get no
"consider merging" warning for a shared domain or handle; `nested_in` ones
do, since they are different organisations.

`graphsense-cli tagpack-tool quality actor-conflicts` does not report pairs
declared in any of the four fields. `quality ranking-conflicts` accepts a tag
summary that shows the same organisation (`same_as`, `sub_service_of`) or a
service nested in it instead of the address's exchange tag; a summary that
shows the host and hides the nested service is still reported. None of the
fields changes the tag summary itself.

Like all context fields, they are only read by tools that know them: older
graphsense-lib versions ignore them, so adding them to an actorpack does not
break validation or insertion anywhere.

### Which one to use

| Situation | Use |
|---|---|
| Same entry, other spelling or old id; tags should merge onto one actor | `aliases` on the actor that stays |
| Rebrand or duplicate where both entries are kept (each has tags, history) | `same_as` |
| A product or division of the same company (mining pool, wallet product) | `sub_service_of` on the product |
| A separate company running on another's addresses or accounts (exchange at a custodian, service using exchange accounts) | `nested_in` on that company |
| Separate companies legitimately on the same addresses for another reason (token and its custodian) | `related_actors` |

Record the evidence for a relation next to it, e.g. in `context.notes` and
`context.refs`.

## View available taxonomies and concepts <a name="taxonomies"></a>

List configured taxonomy keys and URIs

    graphsense-cli tagpack-tool taxonomy list

Fetch and show concepts of a specific remote/local taxonomy (referenced by key: concept, confidence, country)

    graphsense-cli tagpack-tool taxonomy show concept

## Ingest TagPacks and related data into a TagStore <a name="tagstore"></a>

### Prerequisites: TagStore - PostgreSQL database

#### Option 1: Start a dockerized PostgreSQL database

- [Docker][docker], see e.g. https://docs.docker.com/engine/install/
- Docker Compose: https://docs.docker.com/compose/install/

First, copy `tagpack/env.template`
to `tagpack/.env` and fill the fields `POSTGRES_PASSWORD` and `POSTGRES_PASSWORD_TAGSTORE`.

Run

    cp tagpack/postgres-conf.sql.template postgres-conf.sql

and modify the configuration parameters to your requirements. If no special config is needed an emtpy file is also valid.

    touch postgres-conf.sql

Then, create a network for the docker container:

    docker network create graphsense

Start a PostgreSQL instance using Docker Compose:

    docker compose -f tagpack/docker-compose.yml up -d

This will automatically create the database with the nessesary permissions for the `POSTGRES_USER_TAGSTORE`.

    GS_TAGSTORE_DB_URL='postgresql://${POSTGRES_USER_TAGSTORE}:${POSTGRES_PASSWORD_TAGSTORE}@{HOST}:{PORT}/{DBNAME}' graphsense-cli tagstore init

then generates the nessesary tables, views etc. and populates the database with some default entries.


#### Option 2: Use an existing PostgreSQL database

Create the schema and tables in a PostgreSQL instance of your choice also use `tagstore init` as above, make sure the user specified in the tagstore url has the permission to create tables and views.

### Export .env variables

graphsense-cli tagpack-tool is able to use the variables configured in the `.env` file to avoid specifying the parameter `--url` each time it connects to the database. The `--url` parameter will override the environment values if needed. To export the environment variables in `.env` from a linux shell (e.g. bash), first use:

    source .env
    export $(grep --regexp ^[A-Z] .env | cut -d= -f1)

Or just export each variable using:

    export POSTGRES_USER=VALUE
    export POSTGRES_PASSWORD=VALUE
    export POSTGRES_HOST=VALUE
    export POSTGRES_DB=VALUE
    export POSTGRES_PORT=VALUE   # optional, default 5432

    GS_TAGSTORE_DB_URL=value # For the newer tagstore cli

Then call tagpack-tool.

### Create and display a configuration file that defines which taxonomies to use

To create a default configuration `config.yaml` file from scratch - i.e. when config.yaml does not exist - use:

    graphsense-cli tagpack-tool config

If a config.yaml already exists, it will not be replaced.

Show the contents of the config file:

    graphsense-cli tagpack-tool config -v

To use a specific config file pass the file's location:

    graphsense-cli tagpack-tool --config  path/to/config.yaml config

### Initialize the tagstore database

Create the tables and views with `graphsense-cli tagstore init` (see
[above](#option-1-start-a-dockerized-postgresql-database)). It reads the
database URL from `GS_TAGSTORE_DB_URL` or `--db-url`:

    graphsense-cli tagstore init --db-url "postgresql://${POSTGRES_USER_TAGSTORE}:${POSTGRES_PASSWORD_TAGSTORE}@localhost:5432/tagstore"

The older `graphsense-cli tagpack-tool tagstore init` is retired and only
points to this command.

### Ingest taxonomies and confidence scores
To insert all configured taxonomies at once, simply omit taxonomy name

    graphsense-cli tagpack-tool taxonomy insert

Note: `tagpack-tool sync` inserts the taxonomies automatically.

### Ingest TagPacks

Insert a single TagPack file or all TagPacks from a given folder

    graphsense-cli tagpack-tool tagpack insert tests/testfiles/simple/ex_addr_tagpack.yaml
    graphsense-cli tagpack-tool tagpack insert tests/testfiles/simple/multiple_tags_for_address.yaml
    graphsense-cli tagpack-tool tagpack insert tests/testfiles/

By default, TagPacks are declared as non-public in the database.
For public TagPacks, add the `--public` flag to your arguments:

    graphsense-cli tagpack-tool tagpack insert --public tests/testfiles/

If you try to insert tagpacks that already exist in the database, the ingestion process will be stopped.

To force **re-insertion** (if tagpack file contents have been modified), add the `--force` flag to your arguments:

    graphsense-cli tagpack-tool tagpack insert --force tests/testfiles/

To ingest **new** tagpacks and **skip** over already ingested tagpacks, add the `--add-new` flag to your arguments:

    graphsense-cli tagpack-tool tagpack insert --add-new tests/testfiles/

By default, trying to insert tagpacks from a repository with **local** modifications will **fail**.
To force insertion despite local modifications, add the ``--no-strict-check`` command-line parameter

    graphsense-cli tagpack-tool tagpack insert --no-strict-check tests/testfiles/

By default, tagpacks in the TagStore provide a backlink to the original tagpack file in their remote git repository.
To write local file paths instead, add the ``--no-git`` command-line parameter

    graphsense-cli tagpack-tool tagpack insert --no-git --add-new tests/testfiles/

### Ingest ActorPacks

Insert a single ActorPack file or all ActorPacks from a given folder:

    graphsense-cli tagpack-tool actorpack insert tests/testfiles/actors/ex.actorpack.yaml
    graphsense-cli tagpack-tool actorpack insert tests/testfiles/actors/

You can use the options `--force`, `--add-new`, `--no-strict-check` and `--no-git` in the same way as with the `tagpack` command.

### Align ingested attribution tags with GraphSense cluster Ids

The final step after inserting a tagpack is to fetch the corresponding
Graphsense cluster mapping ids for the crypto addresses in the tagpack.

Copy `../../src/graphsenselib/tagpack/conf/ks_map.json.template` to `ks_map.json` and edit the file to
suit your Graphsense setup.

Then fetch the cluster mappings from your Graphsense instance and insert them
into the tagstore database:

    graphsense-cli tagpack-tool tagstore insert-cluster-mappings -d $CASSANDRA_HOST -f ks_map.json

To update ALL cluster-mappings in your tagstore, add the `--update` flag:

    graphsense-cli tagpack-tool tagstore insert-cluster-mappings --update -d $CASSANDRA_HOST -f ks_map.json

#### Conditional rerun: cluster mapping staleness check

When you re-run clustering on the GraphSense backend, addresses can be
re-assigned to different `cluster_id`s, and the `cluster_id` column in
`address_cluster_mapping` becomes stale. Running an unconditional `--update`
on every sync is expensive; running only on first-insert leaves stale ids in
place until the next manual rerun.

To bridge that, the tool can sample mapped addresses (biased toward the
largest clusters via `gs_cluster_no_addr`), look up their current `cluster_id`
in the GraphSense graph datastore, compute a divergence rate, and only do a
full rerun if it crosses a threshold. Eth-like networks (`ETH`/`TRX`) are
skipped — they have no real clustering (`cluster_id == address_id`).

Inside `sync` (uses graphsense-lib's named environment for the lookup):

    graphsense-cli tagpack-tool sync \
        --auto-rerun-cluster-mapping-with-env <env> \
        [--cluster-staleness-sample-size 2000] \
        [--cluster-staleness-threshold 0.05]

Standalone insert with the same logic:

    graphsense-cli tagpack-tool tagstore insert-cluster-mappings \
        --use-gs-lib-config-env <env> \
        --auto-rerun-if-stale \
        [--staleness-sample-size 2000] \
        [--staleness-threshold 0.05]

Diagnostic-only check (no DB writes), prints a per-network divergence table:

    graphsense-cli tagpack-tool tagstore check-cluster-mapping-staleness \
        --use-gs-lib-config-env <env> \
        [--sample-size 2000]

Sampling is biased toward large clusters because that's where stale
`cluster_id`s are most user-visible. Drift confined to small clusters won't
trigger a rerun, so a periodic unconditional `--rerun-cluster-mapping-with-env`
(e.g. weekly cron) is still recommended as a backstop. The existing
`--rerun-cluster-mapping-with-env` and `--run-cluster-mapping-with-env` flags
are unchanged and still bypass the staleness check.

### Remove duplicate tags

Different tagpacks may contain identical tags - the same label and source for a particular address.
To remove such redundant information, run

    graphsense-cli tagpack-tool tagstore remove-duplicates

### IMPORTANT: Keeping data consistency after tagpack insertion

After all required tagpacks have been ingested, run

    graphsense-cli tagpack-tool tagstore refresh-views

to update all materialized views.
Depending on the amount of tags contained in the tagstore, this may take a while.


## Check the quality of the tags in the TagStore <a name="quality"></a>

To assess on the quality of address tags we define a quality measure.
For an address tag, it is calculated as the **weighted similarity distance** between all pairs of distinct tags assigned to the same address.

An address with a unique tag has a quality equal to 1, while an address with several similar tags has a quality close to 0.

To calculate the quality measure for all the tags in the database, run:

    graphsense-cli tagpack-tool quality calculate

To show the quality measures of all the tags in the database, or those of a specific network, run:

    graphsense-cli tagpack-tool quality [--network BTC] show

Further read-only checks under `graphsense-cli tagpack-tool quality`:

- `ranking-conflicts --network BTC`: exchange-tagged addresses whose tag
  summary shows something other than their exchange tag, with a reason per
  address, and clusters whose label goes against their own tags.
- `actor-conflicts --network BTC`: addresses and clusters attributed to more
  than one actor (pairs declared in the actorpack, see
  [Relations between actors](#relations-between-actors), are skipped).
- `list-bad-text`: garbled (mis-encoded) or non-printable text in tags and
  actors, with the probably intended text.
- `list-labels-without-actor`, `list-actors-without-jur`,
  `list-addresses-with-actor-collisions`, `list-addresses-with-low-quality`.

The checks write CSV or JSON (`--format`, `--out`); see `--help` of each.

## Show tagstore contents/contributions

To list all tagpack creators and their contributions to a tagstore's content use:

    graphsense-cli tagpack-tool tagstore show-composition

# For developers

## Working in development / testing mode

    git clone https://github.com/graphsense/graphsense-lib.git
    cd graphsense-lib

### Using Pip locally

Create and activate a python environment for required dependencies and activate it

    make install-dev

### Linting and Formatting

The code is formatted and linted with ruff in a pre-commit hook (`make dev` installs it). To format and lint manually run:

    make format && make pre-commit


### Build for Publishing

    make build

### Testing

Run the fast subset (what the pre-commit hook runs) or the whole suite, which
needs Docker for the database tests:

    make test-fast
    make test-ci

`make test` runs the whole suite with coverage.

[docker]: https://www.docker.com
