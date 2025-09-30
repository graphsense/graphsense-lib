# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [25.09.4/2.7.4] 2025-09-30
### fixed
- add exponential backoff for ingest retries.

## [25.09.3/2.7.3] 2025-09-16
### fixed
- allow datetime values in lastmod of tagpack and tag, instead of only date

## [25.09.2/2.7.2] 2025-09-15
### changed
- added better handling for thorchain bridge
- updated btc/eth-etl dependencies (added new fields txinwitness, vrs)

### added
- code to compute pubkey from vrs eth
- typing checks via ty


## [25.09.1/2.7.1] 2025-09-05
### changed
- added retry logic for bridging requests

## [25.09.0/2.7.0] 2025-09-04
### added
- added services layer form gs-rest
- added tagpack-tool and gs-tagstore-cli functionality (See Readme)
### changed
- improved swaps and bridge decoding support


## [25.08.0/2.6.0] 2025-08-07
### added
- bridging support to conversions endpoint
- moved database access to gslib from gs-rest

### changed
- renamed swap extra dependencies to conversions
- support for python 3.11

### fixed
- uniform tx id handling in rest-interface

## [25.07.3/2.5.3] 2025-07-08
### added
- add optional environment to slack logging handler
- add default_environment to gs_config

## [25.07.2/2.5.2] 2025-07-04
### added
- Slack logging handler

## [25.07.1/2.5.1] 2025-06-26
### added
- monitoring monitor-raw-ingest cli command

## [25.07.0/2.5.0] 2025-06-25
### added
- some utility functions to harmonize with gs-rest
- added support for cassandra user and password authentication
### changed
- improved algo for swap detection and analysis
- added optional dependencies swaps, ingest, all

## [25.06.0/2.4.11] 2025-06-02
### added
- event signatures/decoding for swaps and trading pair creation
### changed
- change from pyScaffold -> uv, black; isort; flake8 -> ruff

## [25.03.2/2.4.10] 2025-03-28
### changed
- higher default timeout to avoid errors on big inserts

## [25.03.2/2.4.10] 2025-03-28
### changed
- higher default timeout to avoid errors on big inserts

## [25.03.1/2.4.9] 2025-03-14
### changed
- better retry handling on big inserts

## [25.03.0/2.4.8] 2025-03-07
### changed
- updated dependencies, goodconf, pydantic etc.
### added
- database tests via testcontainer
- testing of exchange rates import
- vcr for tests with web dependencies
- ruff instead of flake8, black and isort

## [25.01.0/2.4.7] 2025-01-02
### fixed
- Delta updater now marks contract addresses for eth and tron

### changed
- Updated deltalake dependency to 0.22.3

## [24.08.5/2.4.6] 2024-12-11
### fixed
- parse address for anchor output

## [24.08.5/2.4.5] 2024-12-11
### fixed
- allow anchor script type in btc-like currencies

## [24.08.4/2.4.4] 2024-11-11
### fixed
- delta update failed after erigon 3 update, missing reward traces

## [24.08.3/2.4.3] 2024-11-1
### fixed
- handle no tx > int32 max for trx (truncate)

## [24.08.2/2.4.2] 2024-10-31
### fixed
- allow null values in binary columns for delta tables

## [24.08.1/2.4.1] 2024-08-22
### fixed
- cleanup of print and log statements

## [24.08.0/2.4.0] 2024-08-20
### changed
- removed ingest to-csv, replacement is export to delta lake, which is more efficient
- renamed delta lake-commands ingest dump-rawdata -> ingest delta-lake ingest; ingest optimize deltalake -> ingest delta-lake optimize
- removed fs-cache helper for trx and eth delta-update, now uses delta lake directly
- removed typechecked dependency, removed disk-cache dependency

## [24.07.7/2.3.7] 2024-07-16
### fixed
- tron delta-dump: fix missing transferto_address in some tron traces

## [24.07.6/2.3.6] 2024-07-15
### fixed
- tron delta-dump freezes on grpc asyncio requests
- safer handling of ctrl-c on delta-dumps

## [24.07.5/2.3.5] 2024-07-08
### fixed
- increase timeout limit for s3 requests from the default 30s to 300s

## [24.07.4/2.3.4] 2024-07-08
### added
- Allow optimizing single delta table
### fixed
- Add timeout for grpc calls to fix freezing of trx ingest

## [24.07.3/2.3.3] 2024-07-02
### fixed
- limit compaction parallelism delta lake

## [24.07.2/2.3.2] 2024-07-02
### fixed
- evaluating tables to fix in optimize deltalake step, remove direct boto3 dep.

## [24.07.1/2.3.1] 2024-07-02
### fixed
- passing s3 credentials to boto3

## [24.07.0/2.3.0] 2024-07-02
### added
- Write raw data to delta tables on s3 or local using graphsense-cli dump-rawdata
- graphsense-cli optimize-deltalake to optimize tables of a currency (vacuum and/or compact)

## [24.02.10/2.2.10] 2024-06-17
### fixed
- Fixing release tag issue

## [24.02.9/2.2.9] 2024-06-17
### fixed
- numpy 2.0.0 problem (numpy.dtype size changed error)
### added
- cryptocompare exchange rates to have a free version again (graphsense-cli exchange-rates cryptocompare)
- graphsense-cli trace event to print prettyfied event logs for tron and eth

## [24.02.8/2.2.8] 2024-05-28
### fixed
- fixed coingecko z-cash currency key to fetch exchange rates
### added
- coinmarketcap allow configuration of api key for pro api (free is not available anymore)

## [24.02.7/2.2.7] 2024-05-28
### added
- graphsense-cli exchange-rates coingecko to allow fetching exchange rates via coingecko pro api

## [24.02.6/2.2.6] 2024-04-10
### fixed
- csv export with new version of ethereum etl 2.4

## [24.02.5/2.2.5] 2024-04-08
### fixed
- performance problem (timeouts) on fetching transactions per block for utxo currencies.

## [24.02.4/2.2.4] 2024-03-19
### fixed
- tron delta update: missing tx_hash for traces in deployment txs.

## [24.02.3/2.2.3] 2024-03-11
### changed
- Changed number of backoff blocks used in ingestion to avoid spurious data (mostly lowered)

## [24.02.2/2.2.2] 2024-03-06
### fixed
- gracefully handle inconsistencies in address relations

## [24.02.1/2.2.1] 2024-03-04
### fixed
- Warning instead of exception on ingest filelock timeout

## [24.02.0/2.2.0] 2024-03-04
### changed
- full delta updates for tron and ethereum
- block tx table to long format instead of Cassandra lists (breaking)
### fixed
- off by one error in utxo delta updates

## [24.01.2/2.1.2] 2024-02-07
### changed
- change consistency level Cassandra, consistency_level=LOCAL_QUORUM, serial_consistency_level=LOCAL_SERIAL

## [24.01.1/2.1.1] 2024-02-07
### fixed
- address.first_tx_id and last_tx_id should be long type

## [24.01/2.1.0] 2024-01-09
### added
- ingest now works stores additional details/tables in raw keyspace (tx_type, fees)
- new field for address table, zero value tx stats (eth and trx)
- graphsense-cli config get --path function to access config values for scripting
### changed
- more robust retry handling on ingest
### fixed
- minor bug with system.exit handling and slack notifications
- timestamp micro instead of milliseconds bug trx transactions

## [23.09/2.0.0] 2023-11-21
### fixed
- new pk for summary stats to avoid duplicate entries. Breaking: needs recreation of table

## [23.09/1.8.3] 2023-11-07
### fixed
- ingest default config to raw keyspace on create to avoid problems.

## [23.09/1.8.2] 2023-10-24
### fixed
- handle error missing quotes field on coinmarketcap exchange rates ingest

## [23.09/1.8.1] 2023-10-06
### fixed
- handle zcash shielded inputs in import

## [23.09/1.8.0] 2023-10-02
### Added
- added flag forward-fill-rates to allow transform even if no current rates are available (last rate avail is used)

## [23.09/1.7.6] 2023-10-06
### fixed
- fix performance degradation on because of inefficient config lookups

## [23.09/1.7.5] 2023-10-02
### fixed
- (critical) delta update only inserts coinbase txs

## [23.09/1.7.3] 2023-09-21
### fixed
- setup automatic pypi publish with github actions

## [23.09/1.7.1] 2023-09-20
### Added
- ingest/delta update test script to setup a fully functional Cassandra instance for development (script/dev-ingest.sh)
### Fixed
- fixed bug on empty output list on coinbase txs.

## [23.06/1.7.0] 2023-09-12
### Added
- delta updater support for pseudo coinbase address

## [23.06/1.6.1] 2023-09-11
### Fixed
- inconsistent db state after write timeout -> added retry logic for delta updater on write timeouts

## [23.06/1.6.0] 2023-08-18
### Added
- ingest for utxo now creates new tables for transaction references

## [23.06/1.5.0] 2023-06-12
### Added
- added cli ingest command (ingest from-node) for ethereum-like currencies [#6](https://github.com/graphsense/graphsense-ethereum-etl/issues/6)
- added cli ingest command to export node data to csv
- added cli ingest commands (ingest from-node) for btc-like currencies [#4](https://github.com/graphsense/graphsense-bitcoin-etl/issues/4)
- add ingest to parquet files as ingest output option, additional to cassandra [#2](https://github.com/graphsense/graphsense-lib/issues/2)
- alpha support for transaction-monitoring [#4](https://github.com/graphsense/graphsense-lib/issues/4)
- compatibility with tron data in raw keyspaces [#3](https://github.com/graphsense/graphsense-lib/issues/3)

### Fixed
- delta updater bug with zero value and zero fee txs in btc

## [23.03/1.4.0] 2023-03-29
### Added
- added cli command graphsense-cli db logs get-decodeable-logs to decoded logs in a given block range.
- added all event definitions to decode all USDT event logs

## [23.01/1.3.0] 2023-01-30
### Added
- added keyspace name to monitoring output
- slack notifications and cli notify endpoint
- exception notification via slack
- bash completion file generation
- enable specifying a config file (allowing mulitple configs)
- initial support for decoding eth logs
- functions to efficiently find the closest block to a given date and vice versa

### Fixed
- delta updater fixed skipped blocks
- error when data is up to date
- getting highest block with exchange rates

## [22.11/1.2.0] 2022-11-23
### Added
- Delta updater v2 for utxo currencies
- Config flag to disable delta updater
- Simple monitoring of database state
- Colorized output
- More readable logger format

### Changed
- Changed schema files to reflect the current version of the graphsense db

## [1.1.0] 2022-10-11
### Changed
- Initial release
