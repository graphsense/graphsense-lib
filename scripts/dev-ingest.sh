#!/usr/bin/env bash
NW=${1:-bch}
EB=${2:-100000}
UV=${3:-2}
WB=${4:-3}
RB=${5:-15}
# --end-date 2011-11-20

echo "Import on ${NW} till ${EB} with delta updater version ${UV}"

if [ "${NW}" = "eth" ] || [ "${NW}" = "trx" ]
then
    echo "account model ingest"
    graphsense-cli -v ingest from-node -e dev -c ${NW} --end-block ${EB} --batch-size ${RB} --version 2 --create-schema && \
    graphsense-cli -v exchange-rates coindesk ingest -e dev -c ${NW} --abort-on-gaps  && \
    graphsense-cli -v exchange-rates coinmarketcap ingest -e dev -c ${NW} --abort-on-gaps && \
    graphsense-cli -v delta-update update -e dev -c ${NW} --end-block ${EB} --write-batch-size ${WB} --updater-version ${UV} --create-schema --pedantic --forward-fill-rates
else
    echo "utxo model ingest"
    graphsense-cli -v ingest from-node -e dev -c ${NW} --end-block ${EB} --batch-size ${RB} --create-schema --mode='utxo_with_tx_graph' && \
    graphsense-cli -v exchange-rates coindesk ingest -e dev -c ${NW} --abort-on-gaps  && \
    graphsense-cli -v exchange-rates coinmarketcap ingest -e dev -c ${NW} --abort-on-gaps && \
    graphsense-cli -v delta-update update -e dev -c ${NW} --end-block ${EB} --write-batch-size ${WB} --updater-version ${UV} --create-schema --pedantic --forward-fill-rates
fi
