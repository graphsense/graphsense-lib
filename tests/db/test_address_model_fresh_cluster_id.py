"""fresh_cluster_id must survive the service->API model conversion.

The address response is built as a service-layer ``Address``
(db/asynchronous/services/models) and converted to the API ``Address``
(web/models) via ``model_dump``/``model_validate``. Pydantic silently drops
kwargs that are not model fields, so if either model lacks the field the id
vanishes from the response without an error — exactly the bug this guards
against.
"""

from graphsenselib.db.asynchronous.services.models import Address as ServiceAddress
from graphsenselib.web.models.addresses import Address as ApiAddress
from graphsenselib.web.translators import to_api_address

_VALUES = {"value": 0, "fiat_values": [{"code": "eur", "value": 0.0}]}
_TX = {"height": 100, "timestamp": 1600000000, "tx_hash": "ab" * 32}


def _service_address(**overrides):
    return ServiceAddress(
        address="Laddr",
        currency="ltc",
        entity=1396178,
        first_tx=_TX,
        last_tx=_TX,
        total_received=_VALUES,
        total_spent=_VALUES,
        balance=_VALUES,
        **overrides,
    )


def test_fresh_cluster_id_survives_api_conversion():
    api = to_api_address(_service_address(fresh_cluster_id=1353379))
    assert isinstance(api, ApiAddress)
    assert api.fresh_cluster_id == 1353379
    assert api.entity == 1396178


def test_fresh_cluster_id_defaults_to_none():
    api = to_api_address(_service_address())
    assert api.fresh_cluster_id is None
