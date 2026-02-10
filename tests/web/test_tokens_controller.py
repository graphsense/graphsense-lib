from tests.web.helpers import get_json
from tests.web.testdata.tokens import btc_tokens, eth_tokens


async def test_list_supported_tokens(client):
    path = "/{currency}/supported_tokens"
    result = await get_json(client, path, currency="btc")
    assert result == btc_tokens.to_dict()

    result = await get_json(client, path, currency="eth")
    assert result == eth_tokens.to_dict()
