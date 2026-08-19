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
    apply_plugin_hooks,
    get_services,
    get_tagstore_access_groups,
    make_ctx,
)

router = APIRouter()
logger = logging.getLogger(__name__)

restrict_concurrency_on = [
    "list_entity_txs",
    "list_entity_addresses",
    "list_cluster_txs",
    "list_cluster_addresses",
    "list_tags_by_address",
]
default_concurrency_by_operation = {
    "list_tags_by_address": 2,
}

# Upper bound on how many wrap() tasks may exist at once, expressed relative to
# the per-operation backend concurrency. The semaphore in wrap() caps how many
# backend calls run concurrently; this caps how many coroutines are *created*.
# Keeping a few multiples of the semaphore size in flight leaves the semaphore
# saturated while the finished ones flatten their rows.
tasks_in_flight_factor = 4
min_tasks_in_flight = 64

# `clusters` is listed before `entities` so new cluster-named operations resolve
# against clusters_service first. `entities_service` is still present as a
# back-compat shim re-exporting the same functions under their legacy names.
apis = ["addresses", "clusters", "entities", "blocks", "txs", "rates", "tags"]

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
    ctx,
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
            result = await operation(ctx, currency, **params)
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
    # Apply plugin response hooks (e.g. private-tag obfuscation) to the model
    # object here, mirroring PluginRoute for non-streaming routes. StreamingResponse
    # bypasses PluginRoute, so without this bulk would leak un-obfuscated tag data.
    # Run on the model object before it is unwrapped/flattened below so the
    # type-based dispatch in the hooks matches (e.g. AddressTags, Entity).
    if not (isinstance(result, dict) and error_field in result):
        apply_plugin_hooks(request, result)
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
            ctx,
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


def stack(request, ctx, currency, operation, body, num_pages, format):
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

    max_concurrency_bulk_operation = ctx.config.get_max_concurrency_bulk(
        operation_name,
        default_concurrency_by_operation.get(operation_name, 10),
    )
    max_bulk_items = getattr(ctx.config, "max_bulk_items", 10_000)

    params = {}
    keys = {}
    check = {"ctx": None, "currency": currency}
    ln = 0
    for attr, a in body.items():
        if a is None:
            continue
        if attr == "only_ids" or not isinstance(a, list):
            params[attr] = a
            check[attr] = a
        elif len(a) > 0:
            le = len(a)
            # Bound the fan-out: stack() creates one coroutine per item, so an
            # unbounded list is an unbounded allocation.
            # Reject rather than silently truncate — a caller that sent more
            # than it can get back should hear about it.
            if 0 < max_bulk_items < le:
                raise BadUserInputException(
                    f"Too many values for '{attr}': {le} exceeds the limit of "
                    f"{max_bulk_items} items per bulk request. Split the "
                    f"request into smaller batches."
                )
            keys[attr] = a
            ln = min(le, ln) if ln > 0 else le
            check[attr] = a[0]

    if not keys:
        raise TypeError("Keys need to be passed as list")
    inspect.getcallargs(operation, **check)

    context = asyncio.Semaphore(max_concurrency_bulk_operation)

    def make_task(i):
        the_keys = {}
        for k, v in keys.items():
            the_keys[k] = v[i]
        return wrap(
            request,
            ctx,
            operation,
            currency,
            params,
            the_keys,
            num_pages,
            format,
            context,
        )

    max_in_flight = max(
        max_concurrency_bulk_operation * tasks_in_flight_factor,
        min_tasks_in_flight,
    )
    return bounded_as_completed(make_task, ln, max_in_flight)


async def bounded_as_completed(make_task, total, max_in_flight):
    """Yield `total` tasks in completion order, `max_in_flight` alive at a time.

    `asyncio.as_completed` is not usable here: it calls `ensure_future` on every
    awaitable up front, so the number of scheduled Tasks — and the memory they
    hold — grows with the caller-supplied list length instead of with the
    configured concurrency. This creates coroutines lazily
    instead, so a large request costs bounded memory no matter how long the list.

    Yields the completed Task rather than its result so callers keep awaiting
    each item themselves, and per-item exceptions surface at the same place they
    did before.
    """
    pending = set()
    started = 0
    try:
        while started < total or pending:
            while started < total and len(pending) < max_in_flight:
                pending.add(asyncio.ensure_future(make_task(started)))
                started += 1
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                yield task
    finally:
        # The consumer may stop early (client disconnect, CSV writer error);
        # nothing else holds these, so they must not be left running.
        for task in pending:
            task.cancel()


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
    # Consume only as far as the header inference needs, then hand the same
    # iterator to the streaming loop below. Collecting the remaining operations
    # up front (as this did) buffers every result before the first byte goes
    # out, which defeats the streaming response on large requests.
    ops = the_stack.__aiter__()
    async for op in ops:
        rows = await op
        rows_to_infer_header.extend(rows)
        regular_rows += sum(
            1 for r in rows if info_field not in r and error_field not in r
        )
        if regular_rows >= NR_REGULAR_ROWS_USED_TO_INFER_HEADER:
            break

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
    async for op in ops:
        rows = await op
        for row in rows:
            yield write_csv_row(csv, wr, row, headerfields)


async def to_json_generator(the_stack):
    started = False
    yield "["
    async for op in the_stack:
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
    summary="Stream bulk operation results as CSV",
    description=(
        "Executes a supported operation for multiple key values and streams "
        "flattened result rows as CSV. Each key list in the request body is "
        "capped (10,000 items by default), as is the body itself (8 MiB by "
        "default); split longer requests into several calls."
    ),
    operation_id="bulk_csv",
    responses={
        200: {
            "description": "Stream of flattened CSV rows for each requested key set."
        },
        400: {
            "description": (
                "Invalid operation name or request body parameters, including a "
                "key list above the per-request item limit."
            )
        },
        413: {"description": "Request body larger than the configured limit."},
        422: {"description": "Validation error in path/query/body input."},
    },
)
async def bulk_csv(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    operation: str = Path(
        ..., description="The operation to perform", examples=["get_block"]
    ),
    num_pages: int = Query(
        ..., ge=1, le=100, description="Number of pages to fetch", examples=[1]
    ),
    body: Dict[str, Any] = ...,
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Streams flattened CSV rows for a bulk operation."""
    currency = currency.lower()
    ctx = make_ctx(request, services, tagstore_groups)

    try:
        the_stack = stack(request, ctx, currency, operation, body, num_pages, "csv")
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
    summary="Stream bulk operation results as JSON",
    description=(
        "Executes a supported operation for multiple key values and streams "
        "flattened result rows as JSON. Each key list in the request body is "
        "capped (10,000 items by default), as is the body itself (8 MiB by "
        "default); split longer requests into several calls."
    ),
    operation_id="bulk_json",
    responses={
        200: {
            "description": "Stream of flattened JSON rows for each requested key set."
        },
        400: {
            "description": (
                "Invalid operation name or request body parameters, including a "
                "key list above the per-request item limit."
            )
        },
        413: {"description": "Request body larger than the configured limit."},
        422: {"description": "Validation error in path/query/body input."},
    },
)
async def bulk_json(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    operation: str = Path(
        ..., description="The operation to perform", examples=["get_block"]
    ),
    num_pages: int = Query(
        ..., ge=1, le=100, description="Number of pages to fetch", examples=[1]
    ),
    body: Dict[str, Any] = ...,
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Streams flattened JSON rows for a bulk operation."""
    currency = currency.lower()
    ctx = make_ctx(request, services, tagstore_groups)

    try:
        the_stack = stack(request, ctx, currency, operation, body, num_pages, "json")
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
