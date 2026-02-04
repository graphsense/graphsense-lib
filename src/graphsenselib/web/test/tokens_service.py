from graphsenselib.web.models import TokenConfig, TokenConfigs

btc_tokens = TokenConfigs(token_configs=[])
eth_tokens = TokenConfigs(
    token_configs=[
        TokenConfig(
            ticker="usdc",
            decimals=6,
            peg_currency="usd",
            contract_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        ),
        TokenConfig(
            ticker="weth",
            decimals=18,
            peg_currency="eth",
            contract_address="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        ),
        TokenConfig(
            ticker="usdt",
            decimals=6,
            peg_currency="usd",
            contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
        ),
    ]
)


async def list_supported_tokens(test_case):
    """Test case for list_supported_tokens"""
    path = "/{currency}/supported_tokens"
    result = await test_case.request(path, currency="btc")

    assert result == btc_tokens.to_dict()

    path = "/{currency}/supported_tokens"
    result = await test_case.request(path, currency="eth")

    assert result == eth_tokens.to_dict()
