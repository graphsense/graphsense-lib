"""Bulk API routes"""

import asyncio
import importlib
import inspect
import json
import logging
import traceback
from csv import DictWriter
from csv import Error as CSVError
from functools import reduce
from typing import Any, Dict

from fastapi import APIRouter, Depends, Path, Query, Request
from fastapi.responses import StreamingResponse
from graphsenselib.errors import BadUserInputException, NotFoundException

from graphsenselib.web.dependencies import ServiceContainer
from graphsenselib.web.models import AddressTag, Entity, Values
from graphsenselib.web.routes.base import (
    RequestAdapter,
    get_services,
    get_tagstore_access_groups,
)

router = APIRouter()
logger = logging.getLogger(__name__)

restrict_concurrency_on = [
    "list_entity_txs",
    "list_entity_addresses",
    "list_tags_by_address",
]
default_concurrency_by_operation = {
    "list_tags_by_address": 2,
}

apis = ["addresses", "entities", "blocks", "txs", "rates", "tags"]

error_field = "_error"
info_field = "_info"
request_field_prefix = "_request_"


class writer:
    def write(self, str):
        self.str = str

    def get(self):
        return self.str


def flatten(item, name="", flat_dict=None, format=None):
    if format == "json":
        if isinstance(item, dict):
            return item
        return item.to_dict()
    if flat_dict is None:
        flat_dict = {}
    if isinstance(item, Entity) and item.best_address_tag is None:
        item.best_address_tag = AddressTag()
    if isinstance(item, Values):
        flat_dict[name + "value"] = item.value
        for rate in item.fiat_values:
            flat_dict[name + rate.code] = rate.value
        return
    if "to_dict" in dir(item):
        item = item.to_dict(shallow=True)
    if isinstance(item, dict):
        for sub_item in item:
            flatten(item[sub_item], name + sub_item + "_", flat_dict, format)
    elif isinstance(item, list):
        if format == "csv":
            name = name[:-1]
            item = [i if isinstance(i, str) else str(i) for i in item if i]
            flat_dict[name] = ",".join(item)
            if not name == "actors":
                flat_dict[f"{name}_count"] = len(item)
        else:
            flat_dict[name[:-1]] = [
                flatten(sub_item, format=format) for sub_item in item
            ]
    else:
        flat_dict[name[:-1]] = item
    return flat_dict


async def wrap(
    request,
    operation,
    currency,
    params,
    keys,
    num_pages,
    format,
    max_concurrency_sem_context,
):
    params = dict(params)
    for k, v in keys.items():
        params[k] = v
    try:
        async with max_concurrency_sem_context:
            result = await operation(request, currency, **params)
    except NotFoundException:
        result = {error_field: "not found"}
    except BadUserInputException as e:
        traceback.print_exception(type(e), e, e.__traceback__)
        result = {error_field: str(e)}
    except TypeError as e:
        traceback.print_exception(type(e), e, e.__traceback__)
        result = {error_field: str(e)}
    except Exception as e:
        traceback.print_exception(type(e), e, e.__traceback__)
        result = {error_field: "internal error"}
    if isinstance(result, list):
        rows = result
        page_state = None
    elif not hasattr(result, "next_page"):
        rows = [result]
        page_state = None
    else:
        result = result.to_dict(shallow=True)
        for k in result:
            if k != "next_page":
                rows = result[k]
                break
        page_state = result.get("next_page", None)
    flat = []

    def append_keys(fl):
        for k, v in keys.items():
            fl[request_field_prefix + k] = v

    for row in rows:
        fl = flatten(row, format=format)
        append_keys(fl)
        flat.append(fl)
    if not rows:
        fl = {}
        append_keys(fl)
        fl[info_field] = "no data"
        flat.append(fl)
    num_pages -= 1
    if num_pages > 0 and page_state:
        params["page"] = page_state
        more = await wrap(
            request,
            operation,
            currency,
            params,
            keys,
            num_pages,
            format,
            max_concurrency_sem_context,
        )
        for row in more:
            flat.append(row)
    return flat


def stack(request, currency, operation, body, num_pages, format):
    operation_name = operation
    operation_func = None
    for api in apis:
        try:
            mod = importlib.import_module(f"graphsenselib.web.service.{api}_service")
            if hasattr(mod, operation):
                operation_func = getattr(mod, operation)
                break
        except ModuleNotFoundError:
            raise NotFoundException(f"API {api} not found")
        except AttributeError:
            raise NotFoundException(f"{api}.{operation} not found")

    if operation_func is None:
        raise BadUserInputException(
            f"Unknown operation '{operation_name}'. Check /openapi.json for available bulk operations."
        )
    operation = operation_func
    aws = []

    max_concurrency_bulk_operation = request.app["config"].get_max_concurrency_bulk(
        operation_name,
        default_concurrency_by_operation.get(operation_name, 10),
    )

    params = {}
    keys = {}
    check = {"request": None, "currency": currency}
    ln = 0
    for attr, a in body.items():
        if a is None:
            continue
        if attr == "only_ids" or not isinstance(a, list):
            params[attr] = a
            check[attr] = a
        elif len(a) > 0:
            keys[attr] = a
            le = len(a)
            ln = min(le, ln) if ln > 0 else le
            check[attr] = a[0]

    if not keys:
        raise TypeError("Keys need to be passed as list")
    inspect.getcallargs(operation, **check)

    context = asyncio.Semaphore(max_concurrency_bulk_operation)

    for i in range(0, ln):
        the_keys = {}
        for k, v in keys.items():
            the_keys[k] = v[i]
        aw = wrap(
            request, operation, currency, params, the_keys, num_pages, format, context
        )

        aws.append(aw)

    return asyncio.as_completed(aws)


async def to_csv_generator(the_stack):
    wr = writer()

    def is_count_column(row, key):
        postfix = "_count"
        return (
            key.endswith(postfix)
            and key[: -len(postfix)] in row
            and isinstance(row.get(key, None), int)
        )

    def write_csv_row(csvwriter, buffer_writer, row, header_columns):
        try:
            out_row = {
                k: v
                for k, v in row.items()
                if (k in header_columns or not is_count_column(row, k))
            }

            csvwriter.writerow(out_row)
        except (BadUserInputException, CSVError) as e:
            logger.error(f"Error writing bulk row {row}: ({type(e)}) {e}")
            request_fields = {
                k: v for k, v in row.items() if k.startswith(request_field_prefix)
            }
            error_and_request_fields = {
                **{error_field: "internal error - can't produce csv"},
                **request_fields,
            }
            csvwriter.writerow(error_and_request_fields)
        return buffer_writer.get()

    NR_REGULAR_ROWS_USED_TO_INFER_HEADER = 100
    rows_to_infer_header = []
    regular_rows = 0
    ops_rest = []
    for op in the_stack:
        if regular_rows < NR_REGULAR_ROWS_USED_TO_INFER_HEADER:
            rows = await op
            rows_to_infer_header.extend(rows)
            regular_rows += sum(
                1 for r in rows if info_field not in r and error_field not in r
            )
        else:
            ops_rest.append(op)

    # Infer header
    headerfields = sorted(
        list(
            reduce(
                set.union, [set(r.keys()) for r in rows_to_infer_header], set()
            ).union(set([error_field, info_field]))
        )
    )

    csv = DictWriter(wr, headerfields, restval="", extrasaction="ignore")

    # write header
    csv.writeheader()
    head = wr.get()
    yield head

    # write header infer rows
    for row in rows_to_infer_header:
        yield write_csv_row(csv, wr, row, headerfields)

    # write the rest
    for op in ops_rest:
        rows = await op
        for row in rows:
            yield write_csv_row(csv, wr, row, headerfields)


async def to_json_generator(the_stack):
    started = False
    yield "["
    for op in the_stack:
        try:
            rows = await op
        except NotFoundException:
            continue
        if started and rows:
            yield ","
        else:
            started = True

        s = False
        for row in rows:
            if s:
                yield ","
            else:
                s = True
            yield json.dumps(row)
    yield "]"


@router.post(
    "/bulk.csv/{operation}",
    summary="Get data as CSV in bulk",
    operation_id="bulk_csv",
)
async def bulk_csv(
    request: Request,
    currency: str = Path(..., description="The cryptocurrency code (e.g., btc)"),
    operation: str = Path(..., description="The operation to perform"),
    num_pages: int = Query(..., description="Number of pages to fetch"),
    body: Dict[str, Any] = ...,
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get data as CSV in bulk"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    try:
        the_stack = stack(adapted_request, currency, operation, body, num_pages, "csv")
    except TypeError as e:
        traceback.print_exception(type(e), e, e.__traceback__)
        text = (
            str(e).replace("positional ", "").replace("()", "").replace("keyword ", "")
        )
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=text)

    async def generate():
        async for row in to_csv_generator(the_stack):
            yield row

    return StreamingResponse(
        generate(),
        media_type="text/csv",
    )


@router.post(
    "/bulk.json/{operation}",
    summary="Get data as JSON in bulk",
    operation_id="bulk_json",
)
async def bulk_json(
    request: Request,
    currency: str = Path(..., description="The cryptocurrency code (e.g., btc)"),
    operation: str = Path(..., description="The operation to perform"),
    num_pages: int = Query(..., description="Number of pages to fetch"),
    body: Dict[str, Any] = ...,
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get data as JSON in bulk"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    try:
        the_stack = stack(adapted_request, currency, operation, body, num_pages, "json")
    except TypeError as e:
        traceback.print_exception(type(e), e, e.__traceback__)
        text = (
            str(e).replace("positional ", "").replace("()", "").replace("keyword ", "")
        )
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=text)

    async def generate():
        async for row in to_json_generator(the_stack):
            yield row

    return StreamingResponse(
        generate(),
        media_type="application/json",
    )
