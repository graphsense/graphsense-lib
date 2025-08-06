import asyncio

import aiohttp
from async_lru import alru_cache
from graphsenselib.utils.accountmodel import eth_address_to_hex
from graphsenselib.utils.defi import (
    create_base_balance_request_payload,
    create_token_balance_request_payload,
)


async def send_node_request(node_url: str, payload):
    async with aiohttp.ClientSession() as session:
        async with session.post(node_url, json=payload) as resp:
            return await resp.json()


@alru_cache(maxsize=1000)
async def get_token_balance(
    node_url: str, contract_address: str, account: str, block: str = "latest"
):
    resp = await send_node_request(
        node_url,
        payload=create_token_balance_request_payload(contract_address, account, block),
    )

    return int(resp["result"], 16)


@alru_cache(maxsize=1000)
async def get_base_balance(node_url: str, account: str, block: str = "latest"):
    resp = await send_node_request(
        node_url, payload=create_base_balance_request_payload(account, block)
    )

    return int(resp["result"], 16)


async def get_balances(
    node_url: str, network: str, account: str, tokenConfigs, block: str = "latest"
):
    bb = get_base_balance(node_url, eth_address_to_hex(account), block)

    tbs = [
        get_token_balance(
            node_url,
            eth_address_to_hex(tk["token_address"]),
            eth_address_to_hex(account),
            block,
        )
        for k, tk in tokenConfigs.items()
    ]

    balances = await asyncio.gather(bb, *tbs)

    return {
        k.upper(): {"balance": v}
        for k, v in zip([network, *tokenConfigs.keys()], balances)
    }
