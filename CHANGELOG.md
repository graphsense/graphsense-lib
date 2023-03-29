# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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
