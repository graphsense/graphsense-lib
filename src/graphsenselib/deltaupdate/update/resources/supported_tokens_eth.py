from io import StringIO

import pandas as pd

data = """
asset,assettype,decimals,address,coin_equivalent,usd_equivalent
USDT,ERC20,6,0xdac17f958d2ee523a2206206994597c13d831ec7,0,1
USDC,ERC20,6,0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,0,1
WETH,ERC20,18,0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,1,0
"""

SUPPORTED_TOKENS = pd.read_csv(StringIO(data))
