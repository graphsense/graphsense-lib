from graphsenselib.web.models import Block

block = Block(
    height=1,
    currency="btc",
    block_hash="00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048",
    no_txs=1,
    timestamp=1231469665,
)

block2 = Block(
    height=2,
    currency="btc",
    block_hash="000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd",
    no_txs=1,
    timestamp=1231469744,
)

eth_block = Block(
    height=1, currency="eth", block_hash="123456", no_txs=2, timestamp=123
)

eth_block2 = Block(
    height=2300001, currency="eth", block_hash="234567", no_txs=0, timestamp=234
)
