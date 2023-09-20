#!/usr/bin/env bash
NW=${1:-bch}
EB=${2:-100000}
graphsense-cli -v ingest from-node -e dev -c ${NW} --end-block ${EB} --batch-size 15 --create-schema --mode='utxo_with_tx_graph' && \
graphsense-cli -v exchange-rates coindesk ingest -e dev -c ${NW} --abort-on-gaps && \
graphsense-cli -v exchange-rates coinmarketcap ingest -e dev -c ${NW} --abort-on-gaps && \
graphsense-cli -v delta-update update -e dev -c ${NW} --end-block ${EB} --write-batch-size 3 --updater-version 2 --create-schema --pedantic
