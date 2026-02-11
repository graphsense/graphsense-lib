from graphsenselib.web.models import (
    CurrencyStats,
    LabeledItemRef,
    SearchResult,
    SearchResultByCurrency,
    Stats,
)

stats = Stats(
    currencies=[
        CurrencyStats(
            name="btc",
            no_entities=7890,
            no_addresses=4560,
            no_blocks=3,
            timestamp=420,
            no_txs=110,
            no_labels=13,
            no_tagged_addresses=79,
            no_address_relations=1230,
            network_type="utxo",
        ),
        CurrencyStats(
            name="eth",
            no_entities=0,
            no_addresses=1,
            no_blocks=2300002,
            timestamp=16,
            no_txs=10,
            no_labels=4,
            no_tagged_addresses=90,
            no_address_relations=2,
            network_type="account",
        ),
        CurrencyStats(
            name="ltc",
            no_entities=789,
            no_addresses=456,
            no_blocks=3,
            timestamp=42,
            no_txs=11,
            no_labels=2,
            no_tagged_addresses=20,
            no_address_relations=123,
            network_type="utxo",
        ),
        CurrencyStats(
            name="trx",
            no_entities=0,
            no_addresses=1,
            no_blocks=3,
            timestamp=16,
            no_txs=10,
            no_labels=0,
            no_tagged_addresses=0,
            no_address_relations=2,
            network_type="account",
        ),
    ]
)


def base_search_results():
    return SearchResult(
        currencies=[
            SearchResultByCurrency(currency="btc", addresses=[], txs=[]),
            SearchResultByCurrency(currency="ltc", addresses=[], txs=[]),
            SearchResultByCurrency(currency="eth", addresses=[], txs=[]),
            SearchResultByCurrency(currency="trx", addresses=[], txs=[]),
        ],
        labels=[],
        actors=[],
    )
