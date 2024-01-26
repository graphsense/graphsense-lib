from io import StringIO

import pandas as pd

data = """asset,assettype,decimals,address,coin_equivalent,usd_equivalent
USDT,TRC20,6,0xa614f803b6fd780986a42c78ec9c7f77e6ded13c,0,1
USDC,TRC20,6,0x3487b63d30b5b2c87fb7ffa8bcfade38eaac1abe,0,1
WTRX,TRC20,6,0x891cdb91d149f23b1a45d9c5ca78a88d0cb44c18,1,0
"""
SUPPORTED_TOKENS = pd.read_csv(StringIO(data))
