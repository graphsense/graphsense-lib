The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


## [25.08.0] 2025-08-11
### added
- assign unique id on tag creation
- save user id in context

## [25.07.7] 2025-06-20
### fixed
- error on multiple git remotes

## [25.07.6] 2025-06-20
### fixed
- enable user and password authentication for cassandra

## [25.07.5] 2025-06-20
### fixed
- fix: Special handling of unique key violations of add-user-reported-tag

## [25.07.4] 2025-06-20
### fixed
- Special handling of unique key violations of add-user-reported-tag

## [25.07.3] 2025-06-18
### fixed
- exports of errors

## [25.07.2] 2025-06-18
### fixed
- Errorhandling when tag already exists

## [25.07.1] 2025-06-16
### added
- function to add simple tags by users.

## [25.07.0] 2025-06-04
### added
- Added new category `defi_dex_pair`

## [25.06.0] 2025-06-02
### changed
- exchange tags get slight confidence boost in tag digest

## [25.04.0] 2025-04-23
### fixed
- division by zero on tag digest word count


## [25.03.4] 2025-04-01
### fixed
- Fixed typo that caused an issue when adding a tag_type

### changed
- Sort tagpacks by name (additionally to depth) when validating


## [25.03.3] 2025-03-28
### fixed
- bug when all addresses in a cluster mapping batch are not found in the gs db

## [25.03.2] 2025-03-28
### fixed
- Improved validation performance issues

## [25.03.1] 2025-03-27
### added
- Added concepts

### fixed
- Fixed resolve mapping of actors

## [25.03.0] 2025-03-26
### added
- Added field `aliases` in actor schema to store aliases of actors
- Validation of aliases and ids

## [25.01.2] 2025-01-23
### fixed
- fixed commandline list tags

### changed
- dependency mgmt. and development now based on uv instead of pyscaffold
- tx tags now possible via tx_hash field, which is mutually exclusive with address
- better unit testing via testcontainers
- removed upper bounds on dependencies where possible to make lib more compatible

## [25.01.1] 2025-01-14
### fixed
- best cluster tag selected wrong tag; missing order on confidence column.

## [25.01.0] 2025-01-10
BREAKING: Changes in the schema require a resync of the database, cli is compatible to the old one.
### changed
- new database layout, tables are now defined as sql alchemy classes
- restructured taxonomies as one fk class per taxonomy, retired user defined taxonomies
- currency is no handled in two field (asset and network)
- introduced new tag fields tag_type (mention, actor, event)
- introduced new tag field tag_subject to allow tax on tx too (address, tx)
- refreshed dev stack (flake8, black -> ruff)
### new
- standalone REST interface for the tagstore
- New module tagstore, meant to be integrated as lib in other tools
- tables are now defined as sql alchemy classes
- mvp UI for viewing tags delivered as SPA with the rest interface
- better tests now parsing real tagpack yaml files
- tag-digest algorithm to get a summary (weighted) out of a list of tags.

## [24.10.0] 2024-10-10
### changed
- liftet upperbound < 2.0 on pandas dependency
- tagpack without any tags does not raise an validation error anymore

## [24.01.8] 2024-08-12
### fixed
- update coinaddrvalidator dep to avoid error on install
### added
- support for python 3.11

## [24.01.7] 2024-07-19
### fixed
- fix cluster mapping errors on invalid addresses. Instead just skip them.

## [24.01.6] 2024-06-20
### fixed
- avoid errors with numpy 2.0 setting version restriction to < 2.0

## [24.01.5] 2024-06-03
### fixed
- avoid install errors by setting proper python version upper bound to 3.10

## [24.01.4] 2024-05-29
### fixed
- clustermappings for eth did not work

## [24.01.3] 2024-03-29
### changed
- add new concept for tokens (defi_token)

## [24.01.2] 2024-03-22
### changed
- allow all concepts to be used in category field (align category and concepts)

## [24.01.1] 2024-01-09
### changed
- switched to calver

## [24.01.0/1.9.0] 2023-12-21
### added
- add cluster mapping for tron

## [23.09/1.8.0] 2023-10-31
### added
- new --create-db flag for init and sync which tries to automatically create the database if it does not exist

## [23.09/1.7.4] 2023-09-21
### fixed
- Bump cassandra driver version 3.27, import lz4 to enable cassandra compression

## [23.09/1.7.3] 2023-09-20
### fixed
- setup automatic pypi publish with github actions

## [23.09/1.7.2] 2023-09-14
### fixed
- cluster mapping does not use new rerun-cluster-mapping-with-env env

## [23.06/1.7.1] 2023-09-13
### fixed
- error on insert when no tagpacks are loaded

## [23.06/1.7.0] 2023-09-12
### Added
- parallel tagpack insert (-n-workers parameter, default 1 worker)
- new --rerun-cluster-mapping-with-env flag on sync command to update all cluster mappings

## [23.06/1.6.1] 2023-08-16
### Fixed
- old concepts (entity and abuse) have precedence over new concepts (concepts.yaml)

## [23.06/1.6.0] 2023-07-06
### Added
- new db field concepts (list) to store multiple categories with a tag (requires db-resync)

## [23.06/1.5.3] 2023-07-06
### Fixed
- fix bug in ks_map handling

## [23.06/1.5.2] 2023-07-05
### Fixed
- fix field not found error in keyspace check

## [23.06/1.5.1] 2023-07-04
### Fixed
- handle if keyspace is not found gracefully

## [23.06/1.5.0] 2023-06-12
### Added
- new confidence score forensic_investigation (70), [#87](https://github.com/graphsense/graphsense-tagpack-tool/issues/87)
- cluster mapping can now use gs-lib config for import (--use-gs-lib-env option) [#84](https://github.com/graphsense/graphsense-tagpack-tool/issues/84)
- Sync command has option to directly run cluster mapping via gs-lib (--run-cluster-mapping-with-env) [#84](https://github.com/graphsense/graphsense-tagpack-tool/issues/84)
- better handling for large yaml tagpack files [#85](https://github.com/graphsense/graphsense-tagpack-tool/issues/85)
- Support for altering postgres config params in docker-compose setup [#83](https://github.com/graphsense/graphsense-tagpack-tool/issues/83)
### Changed
- Using cSafeLoader for yaml files when possible for better performance
- Deprecate is_public in config.yaml, in favor of cmd flag [#82](https://github.com/graphsense/graphsense-tagpack-tool/issues/82)
- Fixed compatibilty issues with python 3.8 (importlib.files)

## [23.03] 2023-03-30

### Added
- Full support for supporting ActorPacks [#41](https://github.com/graphsense/graphsense-tagpack-tool/issues/41)
  - actor pack validation and insertion for the extensive actor pack in [public repository](https://github.com/graphsense/graphsense-tagpacks/)
  - interactive process of adding suitable actors to existing tagpacks
  - calculation of data quality measures
- Auto update data feature to streamline insertion of updates
  - clone tagpack repositories and insert all tags and actors in the TagStore [#73](https://github.com/graphsense/graphsense-tagpack-tool/issues/73)
- new confidence scores unknown (5) heuristic (10), ledger_immanent (100)

## [23.01] 2023-01-30

### Added

- add --by-currency option for tagstore composition query
- add --csv option for selected commands
- add tag-version pseudo target to Makefile

## [22.11] 2022-11-24
### Changed
- Harmonised command structure [#59](https://github.com/graphsense/graphsense-tagpack-tool/issues/59)
- Moved TagPack documentation to [GraphSense public tagpacks repo](https://github.com/graphsense/graphsense-tagpacks/wiki/GraphSense-TagPacks)
- Removed unnecessary columns in cluster mapping table [#45](https://github.com/graphsense/graphsense-tagpack-tool/issues/45)
- ETH addresses are normalized to lower-case before inserting them to the DB [#39](https://github.com/graphsense/graphsense-tagpack-tool/issues/39)
- Parallel import of cluster mapping [#4](https://github.com/graphsense/graphsense-tagpack-tool/issues/4)
- Improve `cluster_defining_tags_by_frequency_and_maxconfidence` view
### Added
- Command to show tagstore content composition: listing of creators and their contributions
- Command to calculate tag quality measures [#49](https://github.com/graphsense/graphsense-tagpack-tool/issues/49)
- Address validation for currencies supported by `coinaddrvalidator` library [#22](https://github.com/graphsense/graphsense-tagpack-tool/issues/22)
- Confidence scoring handling: ingest confidence scores from local file [#35](https://github.com/graphsense/graphsense-tagpack-tool/issues/35)
### Fixed
- Add `colorama` dependency to disable coloring on file redirect

## [22.10] 2022-10-11
### Added
- Support for PostgreSQL connection pooling
- Removal of duplicate tags
### Fixed
- Fix cluster mapping of ETH addresses without external txs
- consolidate tagpack-level properties
- `conda` setup

## [1.0.1] 2022-08-26
### Added
- Optional prefix for tagpack
- View for tag count by cluster
- View for cluster defining tags by frequency and maxconfidence
### Fixed
- Bug with `--add_new` flag

## [1.0.0] 2022-07-14
### Changed
- Improved ingest: improved schema validation feedback for user, remove duplicates in tagpacks
- Generate tagpack base URI automatically (no longer necessary to keep config.yaml up-to-date)
- Updated confidence.csv
### Added
- YAML file inclusion to share a header file between multiple tagpack files
- Encourage clean tagpack repository when ingesting tagpacks (can be disabled with command line options `--no_strict_check` and `--no_git`)
- URI field in TagStore database to support backlink to tagpack repository in the dashboard
- Option to ingest all taxonomies at once instead of one taxonomy at a time
- Command-line option `--config` to supply path to config file
- Command-line option `--force` to force re-ingest if tagpack already is present in database
- Command-line option `--add_new` to skip over ingested tagpacks and ingest only new ones

## [0.5.2] 2022-03-24
### Changed
- **Tagstore design:** migrate tag handling from Cassandra keyspaces
  to external tag store(s)
- Tagpack validation of confidence value, which is now categorical
  instead of numerical
### Added
- Check for duplicate entries
- Database view for tag statistics
### Removed
- Entity-related components
### Fixed
- Bug in tag validation

# [0.5.1] 2021-11-17
## Changed
- Cersions of third party library dependencies
## Added
- Additional schema fields
- Command line tool usability fixes
- Support for dev version numbers

## [0.5.0] 2021-05-31
### Changed
- Switched to GitHub action workflows
- Added support for entity tags
- Move TagPack documentation to TagPack rep
- Update package dependencies

### Added
- Add additional TagPack validation tests
- Add support for Entity Tags

### Removed
- Removed unnecessary lookup tables

## [0.4.4] - 2020-06-12
### Fixed
- Fixed PEP8 warnings (`flake8`)

## [0.4.3] - 2020-05-11
### Changed
- Separated TagPack Management Tool from public TagPacks
- Refactored scripts into TagPack Management tool
- Re-implemented validation and ingest procedures

### Added
- Support for ingestion / validation of remote taxonomy concepts

## [0.4.2] - 2019-12-19
### Added
- New tagpacks
- Abuses field

### Changed
- Splitted config from schema
- Improved argparse
- Renaming categories

### Removed
- Jupyter notebooks

## [0.4.1] - 2019-06-28
### Added
- Tagpacks: walletexplorer, ransomware, sextortion (Talos), miners
- Schema creation, validate and ingest scripts
- Documentation, License, etc.
