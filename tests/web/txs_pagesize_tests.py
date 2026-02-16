from time import time

from tqdm import tqdm
from txs_pagesize_tester import txs_pagesize_tester

pagesizes = [100, 53, 7, 1]

test_sets = [
    {"currency": "eth", "id": "0x255c0dc1567739ceb2c8cd0fddcf1706563868d0"},
    {
        "currency": "eth",
        "id": "0x10c318b1d817396a8a66016438ac9dfb615ffcf1",
        "min_height": 7957441,
    },
    {
        "currency": "eth",
        "id": "0x10c318b1d817396a8a66016438ac9dfb615ffcf1",
        "max_height": 7957441,
    },
    {
        "currency": "eth",
        "id": "0x10c318b1d817396a8a66016438ac9dfb615ffcf1",
        "min_height": 7957441,
        "max_height": 9207262,
    },
    {"currency": "eth", "id": "0x09826189b333741c3542f12dfa6bb9e06c7237e3"},
    {
        "currency": "eth",
        "id": "0x09826189b333741c3542f12dfa6bb9e06c7237e3",
        "token_currency": "eth",
    },
    {
        "currency": "eth",
        "id": "0x09826189b333741c3542f12dfa6bb9e06c7237e3",
        "token_currency": "usdc",
    },
    {
        "currency": "eth",
        "id": "0x09826189b333741c3542f12dfa6bb9e06c7237e3",
        "token_currency": "weth",
    },
    {
        "currency": "eth",
        "id": "0x09826189b333741c3542f12dfa6bb9e06c7237e3",
        "token_currency": "weth",
        "min_height": 9010434,
    },
    {
        "currency": "eth",
        "id": "0x09826189b333741c3542f12dfa6bb9e06c7237e3",
        "token_currency": "weth",
        "max_height": 12878677,
    },
    {
        "currency": "eth",
        "id": "0x09826189b333741c3542f12dfa6bb9e06c7237e3",
        "token_currency": "weth",
        "min_height": 9010434,
        "max_height": 12878677,
    },
    {
        "currency": "eth",
        "id": "0xd75821a3e9a7878869bfd854d1f99f5e2d360790",
        "neighbor": "0x641faa71fa1545b7dd398fba8bf94d2f6d8e766d",
    },
    {
        "currency": "eth",
        "id": "0xd75821a3e9a7878869bfd854d1f99f5e2d360790",
        "neighbor": "0x641faa71fa1545b7dd398fba8bf94d2f6d8e766d",
        "min_height": 6495332,
    },
    {
        "currency": "eth",
        "id": "0xd75821a3e9a7878869bfd854d1f99f5e2d360790",
        "neighbor": "0x641faa71fa1545b7dd398fba8bf94d2f6d8e766d",
        "max_height": 6545717,
    },
    {
        "currency": "eth",
        "id": "0xd75821a3e9a7878869bfd854d1f99f5e2d360790",
        "neighbor": "0x641faa71fa1545b7dd398fba8bf94d2f6d8e766d",
        "min_height": 6495332,
        "max_height": 6545717,
    },
    {"currency": "trx", "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB"},
    {
        "currency": "trx",
        "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB",
        "max_height": 40216462,
    },
    {
        "currency": "trx",
        "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB",
        "min_height": 20429760,
    },
    {
        "currency": "trx",
        "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB",
        "max_height": 40216462,
        "min_height": 20429760,
    },
    {
        "currency": "trx",
        "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB",
        "token_currency": "trx",
    },
    {
        "currency": "trx",
        "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB",
        "token_currency": "trx",
        "min_height": 20429760,
    },
    {
        "currency": "trx",
        "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB",
        "token_currency": "trx",
        "max_height": 40216462,
    },
    {
        "currency": "trx",
        "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB",
        "token_currency": "trx",
        "max_height": 40216462,
        "min_height": 20429760,
    },
    {
        "currency": "trx",
        "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB",
        "token_currency": "usdt",
    },
    {
        "currency": "trx",
        "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB",
        "token_currency": "usdt",
        "min_height": 20429760,
    },
    {
        "currency": "trx",
        "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB",
        "token_currency": "usdt",
        "max_height": 40216462,
    },
    {
        "currency": "trx",
        "id": "TUzemi3pXCBAUJpoPTYfVwHucsbk6tT5GB",
        "token_currency": "usdt",
        "max_height": 40216462,
        "min_height": 20429760,
    },
    {
        "currency": "trx",
        "id": "TJsC8UgrLaEJz3vgKsyNFDDYFEM42K3GjC",
        "neighbor": "TNdK2XuTpzjcM9vns8LG9258bxaLap5gih",
    },
    {
        "currency": "trx",
        "id": "TJsC8UgrLaEJz3vgKsyNFDDYFEM42K3GjC",
        "neighbor": "TNdK2XuTpzjcM9vns8LG9258bxaLap5gih",
        "max_height": 39913745,
    },
    {
        "currency": "trx",
        "id": "TJsC8UgrLaEJz3vgKsyNFDDYFEM42K3GjC",
        "neighbor": "TNdK2XuTpzjcM9vns8LG9258bxaLap5gih",
        "min_height": 39696021,
    },
    {
        "currency": "trx",
        "id": "TJsC8UgrLaEJz3vgKsyNFDDYFEM42K3GjC",
        "neighbor": "TNdK2XuTpzjcM9vns8LG9258bxaLap5gih",
        "min_height": 39696021,
        "max_height": 39913745,
    },
    {
        "currency": "btc",
        "id": "1FN9eZU2b2S6u6mJy8nrVjPxQhCdkmE7mr",
    },
    {
        "currency": "btc",
        "id": "1FN9eZU2b2S6u6mJy8nrVjPxQhCdkmE7mr",
        "min_height": 718034,
    },
    {
        "currency": "btc",
        "id": "1FN9eZU2b2S6u6mJy8nrVjPxQhCdkmE7mr",
        "max_height": 731854,
    },
    {
        "currency": "btc",
        "id": "1FN9eZU2b2S6u6mJy8nrVjPxQhCdkmE7mr",
        "min_height": 718034,
        "max_height": 731854,
    },
    {
        "currency": "btc",
        "id": "1FN9eZU2b2S6u6mJy8nrVjPxQhCdkmE7mr",
        "neighbor": "bc1qtg3rejp0vw7mvkm03wc8fttjteaeqseq8046gm",
    },
    {
        "currency": "btc",
        "id": "1FN9eZU2b2S6u6mJy8nrVjPxQhCdkmE7mr",
        "neighbor": "bc1qtg3rejp0vw7mvkm03wc8fttjteaeqseq8046gm",
        "min_height": 730710,
    },
    {
        "currency": "btc",
        "id": "1FN9eZU2b2S6u6mJy8nrVjPxQhCdkmE7mr",
        "neighbor": "bc1qtg3rejp0vw7mvkm03wc8fttjteaeqseq8046gm",
        "max_height": 736094,
    },
    {
        "currency": "btc",
        "id": "1FN9eZU2b2S6u6mJy8nrVjPxQhCdkmE7mr",
        "neighbor": "bc1qtg3rejp0vw7mvkm03wc8fttjteaeqseq8046gm",
        "min_height": 730710,
        "max_height": 736094,
    },
    {
        "currency": "btc",
        "id": "890145530",
    },
    {
        "currency": "btc",
        "id": "890145530",
        "min_height": 718034,
    },
    {
        "currency": "btc",
        "id": "890145530",
        "max_height": 731854,
    },
    {
        "currency": "btc",
        "id": "890145530",
        "min_height": 718034,
        "max_height": 731854,
    },
    {"currency": "btc", "id": "890145530", "neighbor": "887478908"},
    {
        "currency": "btc",
        "id": "890145530",
        "neighbor": "887478908",
        "min_height": 730710,
    },
    {
        "currency": "btc",
        "id": "890145530",
        "neighbor": "887478908",
        "max_height": 736094,
    },
    {
        "currency": "btc",
        "id": "890145530",
        "neighbor": "887478908",
        "min_height": 730710,
        "max_height": 736094,
    },
]


for order in ["desc", "asc"]:
    for kwargs in tqdm(test_sets):
        direction_set = [None]
        if "neighbor" not in kwargs:
            direction_set = [None, "out", "in"]
        for direction in direction_set:
            now = time()
            prev_timestamps = None
            if direction:
                kwargs["direction"] = direction
            else:
                kwargs.pop("direction", None)
            # print(f"Testing {kwargs}, order = {order}, direction = {direction}")
            for pagesize in pagesizes:
                # print(f"pagesize = {pagesize}")
                timestamps = txs_pagesize_tester(
                    pagesize=pagesize, order=order, **kwargs
                )
                if prev_timestamps is None:
                    prev_timestamps = timestamps
                assert timestamps == prev_timestamps, "result lengths not equal"
                prev_timestamp = timestamps
                # print(f"length {len(timestamps)}")

            msg = (
                f"Test {kwargs}, order = {order}, "
                f"direction = {direction} took {time() - now} seconds"
            )
            print(msg)  # noqa
