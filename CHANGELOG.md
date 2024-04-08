# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


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
