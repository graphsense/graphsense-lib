from typing import Any, Dict, Protocol

from graphsenselib.utils.address import address_to_user_format

from ....utils.rest_utils import is_eth_like
from .models import TokenConfig, TokenConfigs


class DatabaseProtocol(Protocol):
    def get_token_configuration(self, currency: str) -> Dict[str, Any]: ...


class TokensService:
    def __init__(self, db: DatabaseProtocol, logger: Any):
        self.db = db
        self.logger = logger

    async def list_supported_tokens(self, currency: str) -> TokenConfigs:
        if is_eth_like(currency):
            token_configs = []
            for k, v in self.db.get_token_configuration(currency).items():
                token_config = TokenConfig(
                    ticker=k.lower(),
                    decimals=v["decimals"],
                    peg_currency=v["peg_currency"].lower(),
                    contract_address=address_to_user_format(
                        currency, v["token_address"]
                    ),
                )
                token_configs.append(token_config)
            return TokenConfigs(token_configs=token_configs)
        else:
            return TokenConfigs(token_configs=[])
