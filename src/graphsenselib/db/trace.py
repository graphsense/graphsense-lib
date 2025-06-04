import re
from datetime import datetime

from rich import print as printRich

from ..datatypes.abi import decode_logs_db


def log_sort_key(dlog, log):
    return (log.block_id, log.transaction_index, log.log_index)


def is_matching_row(query_terms, dlog_str, dlog_str_rep):
    return all(
        query.lower() in dlog_str.lower() or query.lower() in dlog_str_rep.lower()
        for query in query_terms
    )


def decoded_log_to_str(decoded_log) -> str:
    name = decoded_log["name"]
    addr = decoded_log["address"].lower()
    params = ",".join([f"{x['name']}={x['value']}" for x in decoded_log["data"]])
    return f"{addr}|{name}|{params}".replace("\n", "")


def make_replacer(**replacements):
    if len(replacements) != 0:
        locator = re.compile("|".join(re.escape(s) for s in replacements))

        def _doreplace(mo):
            return replacements[mo.group()]

        def replace(s):
            return locator.sub(_doreplace, s)

    else:

        def replace(s):
            return s

    return replace


def fetch_data(db, b, bts, contract, replace_names, query_terms, rowfn):
    if b not in bts:
        bts[b] = db.raw.get_block_timestamp(b)

    dtstring = bts[b].isoformat()
    decode_logs = decode_logs_db(db.raw.get_logs_in_block(b, contract=contract))

    decode_logs = sorted(
        [(dlog, log) for dlog, log in decode_logs],
        key=lambda x: log_sort_key(x[0], x[1]),
    )

    for dlog, log in decode_logs:
        # c = "0x" + log.address.hex()
        dlog_str_original = decoded_log_to_str(dlog)
        dlog_str = (
            f"{dtstring}|{b}|{log.log_index}|0x{log.tx_hash.hex()}|{dlog_str_original}"
        )
        dlog_str_rep = replace_names(dlog_str)
        if is_matching_row(query_terms, dlog_str, dlog_str_rep):
            rowfn(dlog, log, dlog_str_original, dlog_str, dlog_str_rep)


def trace(db, query_terms, contract, start, end, names, output_format):
    replace_names = make_replacer(**{k.lower(): v for k, v in names.items()})
    tx_mode = False
    if len(query_terms) == 1 and len(query_terms[0]) == 66:
        tx = query_terms[0]
        tx_data = db.raw.get_tx(tx)
        if tx_data is None:
            printRich("tx not found")
            return
        startblock = tx_data.block_id
        endblock = tx_data.block_id
        tx_mode = True

    if start is not None and end is not None:
        if start.isdigit():
            startblock = start
        else:
            startDt = datetime.fromisoformat(start)
            startblock = db.raw.find_block_nr_for_date(startDt)

        if end.isdigit():
            endblock = end
        else:
            endDt = datetime.fromisoformat(end)
            endblock = db.raw.find_block_nr_for_date(endDt)

    bts = {}

    if not tx_mode:
        from rich.progress import Progress

        with Progress() as progress:
            task1 = progress.add_task("[red]Searching...", total=endblock - startblock)
            for b in range(startblock, endblock + 1):
                progress.update(task1, advance=1)
                fetch_data(
                    db,
                    b,
                    bts,
                    contract,
                    replace_names,
                    query_terms,
                    lambda _, _1, _2, dlog_str, _3: printRich(dlog_str),
                )
    else:
        tx_mode_rows = []

        def append_row(dlog, log, dlog_str_original, dlog_str, dlog_str_rep):
            dlog_str = f"{log.log_index}|{replace_names(dlog_str_original)}"
            if output_format == "table":
                x = dlog_str.split("|")
                x[3] = x[3].replace(",", "\n")
                tx_mode_rows.append(x)
            else:
                printRich(dlog_str)

        for b in range(startblock, endblock + 1):
            fetch_data(db, b, bts, contract, replace_names, query_terms, append_row)

        if tx_mode and output_format == "table":
            from rich.console import Console
            from rich.table import Table

            dt = datetime.fromtimestamp(tx_data.block_timestamp)
            title = replace_names(
                f"{tx}: 0x{tx_data.from_address.hex()} "
                f"-> 0x{tx_data.to_address.hex()} @ {dt.isoformat()}"
            )

            table = Table(title=title, highlight=True)

            table.add_column("index", justify="right", style="cyan", no_wrap=True)
            table.add_column("project", style="magenta")
            table.add_column("event", style="red")
            table.add_column("params", justify="left", max_width=100)

            for x in tx_mode_rows:
                table.add_row(*x)

            console = Console()
            console.print(table)
