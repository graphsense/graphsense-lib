from graphsenselib.db.asynchronous.services.heuristics import (
    CoinJoinHeuristics,
    JoinMarketHeuristic,
    UtxoHeuristics,
)
from graphsenselib.web.translators import to_api_utxo_heuristics


def test_to_api_utxo_heuristics_maps_joinmarket_denomination_field():
    heuristics = UtxoHeuristics(
        coinjoin_heuristics=CoinJoinHeuristics(
            joinmarket=JoinMarketHeuristic(
                detected=True,
                confidence=90,
                n_participants=5,
                pool_denomination=5_000_000,
            )
        )
    )

    api = to_api_utxo_heuristics(heuristics)

    assert api is not None
    assert api.coinjoin_heuristics is not None
    assert api.coinjoin_heuristics.joinmarket is not None
    assert api.coinjoin_heuristics.joinmarket.denomination_sat == 5_000_000
