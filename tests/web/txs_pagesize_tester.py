"""
Retrieves all rows iteratively in pages of given pagesize and verifies temporal
order.
"""

import argparse
import os
from functools import partial

import graphsense
from graphsense.api import addresses_api, entities_api


def txs_pagesize_tester(currency, id, **kwargs):
    configuration = graphsense.Configuration(
        host=os.environ.get("API_URL", "http://localhost:8000")
    )

    configuration.api_key["api_key"] = os.environ.get("API_KEY", "dummy")

    # Enter a context with an instance of the API client
    with graphsense.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        if id.isdigit():
            api_instance = entities_api.EntitiesApi(api_client)
            if "neighbor" in kwargs:
                request_method = partial(
                    api_instance.list_entity_links,
                    currency,
                    int(id),
                    int(kwargs["neighbor"]),
                )
                kwargs.pop("neighbor")
            else:
                request_method = partial(
                    api_instance.list_entity_txs, currency, int(id)
                )
        else:
            api_instance = addresses_api.AddressesApi(api_client)
            if "neighbor" in kwargs:
                request_method = partial(
                    api_instance.list_address_links, currency, id, kwargs["neighbor"]
                )
                kwargs.pop("neighbor")
            else:
                request_method = partial(api_instance.list_address_txs, currency, id)
        page = ""
        # example passing only required values which don't have defaults set
        min_timestamp = None
        timestamps = []
        desc = kwargs.get("order") == "desc"
        while True:
            # print(f'page {page}')
            # Get all transactions an address has been involved in
            kwargs["page"] = page
            api_response = request_method(**kwargs)
            page = getattr(api_response, "next_page", None)
            # print(page)
            resp = (
                api_response.links
                if hasattr(api_response, "links")
                else api_response.address_txs
            )
            assert len(resp) <= kwargs["pagesize"], (
                f"response length bigger than pagesize {len(resp)}"
            )
            for tx in resp:
                # print(f'{tx}')
                timestamps.append(tx.timestamp)
                if min_timestamp is None:
                    min_timestamp = tx.timestamp
                    continue
                if (
                    desc
                    and tx.timestamp > min_timestamp
                    or not desc
                    and tx.timestamp < min_timestamp
                ):
                    raise RuntimeError(
                        f"tx {tx.tx_hash} {tx.currency} {tx.timestamp} {min_timestamp}"
                    )
                min_timestamp = tx.timestamp

            if page is None:
                break
        return timestamps


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some input parameters.")

    parser.add_argument(
        "--currency", type=str, required=True, help="Specify the currency as a string."
    )
    parser.add_argument(
        "--token_currency",
        type=str,
        required=False,
        help="Specify the token currency as a string.",
    )
    parser.add_argument(
        "--id", type=str, required=True, help="Specify the ID as a string."
    )

    parser.add_argument(
        "--neighbor", type=str, help="Specify the neighbor as a string (optional)."
    )

    parser.add_argument(
        "--pagesize",
        type=int,
        required=True,
        help="Specify the page size as an integer.",
    )
    parser.add_argument(
        "--min", type=int, required=False, help="Specify the min height as an integer."
    )
    parser.add_argument(
        "--max", type=int, required=False, help="Specify the max height as an integer."
    )
    parser.add_argument(
        "--desc",
        action="store_true",
        required=False,
        help="Specify if results should be retrieved in descending order (default: ascending).",
    )

    # Parse the arguments
    args = parser.parse_args()

    kwargs = {}
    kwargs["pagesize"] = args.pagesize
    if args.min:
        kwargs["min_height"] = args.min
    if args.max:
        kwargs["max_height"] = args.max
    if args.desc:
        kwargs["order"] = "desc"
    if not args.neighbor and args.token_currency:
        kwargs["token_currency"] = args.token_currency

    txs_pagesize_tester(args.currency, args.id, **kwargs)
