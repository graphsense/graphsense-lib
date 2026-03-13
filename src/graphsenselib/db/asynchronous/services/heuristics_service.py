from collections import defaultdict
from typing import Any, Dict, Optional

from .common import cannonicalize_address
from .models.heuristics import OneTimeChangeDetails, OneTimeChangeHeuristic, TxHeuristics


async def _one_time_change_heuristic(
    tx, currency, get_address
) -> OneTimeChangeHeuristic:
    empty_details = OneTimeChangeDetails(
        same_script_type=[],
        not_nicely_divisible=[],
        output_less_than_input=[],
        not_reused=[],
    )

    if (
        tx.get("coinbase")
        or len(tx.get("outputs", [])) > 10
        or len(tx.get("outputs", [])) < 2
    ):
        summary = {
            outp.address[0]: False
            for outp in tx.get("outputs", [])
            if outp is not None and outp.address
        }
        return OneTimeChangeHeuristic(summary=summary, details=empty_details)

    cond_same_script = set()
    cond_not_nicely_divisible = set()
    cond_out_less_than_in = set()

    min_input_value = min([inp.value for inp in tx.get("inputs", []) if inp is not None])

    script_type_input = None
    for inp in tx.get("inputs", []):
        if inp is None:
            continue
        if script_type_input is None:
            script_type_input = inp.address_type
        elif inp.address_type != script_type_input:
            script_type_input = False

        if not script_type_input:
            break

    counts: Dict[str, int] = defaultdict(int)
    for outp in tx.get("outputs", []):
        if outp is None or not outp.address:
            continue

        if len(outp.address) >= 1:
            outp_addr = outp.address[0]
            counts[outp_addr] += 1

            if script_type_input == outp.address_type:
                cond_same_script.add(outp_addr)

            if not outp.value % 1000 == 0:
                cond_not_nicely_divisible.add(outp_addr)

            if outp.value < min_input_value:
                cond_out_less_than_in.add(outp_addr)

    # check addresses for reuse
    change_candidates = (
        cond_same_script
        .intersection(cond_not_nicely_divisible)
        .intersection(cond_out_less_than_in)
    )
    not_change = set()
    for cand in change_candidates:
        addr = await get_address(currency, cannonicalize_address(currency, cand))
        if addr is None:
            not_change.add(cand)
            continue

        first_tx_height = addr["first_tx"].height if addr.get("first_tx") else None

        if (
            addr.get("no_incoming_txs", 0) > 1
            or addr.get("no_outgoing_txs", 0) > 1
            or (first_tx_height is not None and first_tx_height < tx["block_id"])
        ):
            not_change.add(cand)

    same_addr_more_than_once = set(addr for addr, count in counts.items() if count > 1)
    all_candidates = change_candidates.copy()
    change_candidates = change_candidates.difference(not_change).difference(
        same_addr_more_than_once
    )
    if len(change_candidates) != 1:
        change_candidates = set()

    summary = {}
    for outp in tx.get("outputs", []):
        if outp is None or not outp.address:
            continue
        outp_addr = outp.address[0]
        summary[outp_addr] = outp_addr in change_candidates

    return OneTimeChangeHeuristic(
        summary=summary,
        details=OneTimeChangeDetails(
            same_script_type=list(cond_same_script.difference(not_change)),
            not_nicely_divisible=list(cond_not_nicely_divisible.difference(not_change)),
            output_less_than_input=list(cond_out_less_than_in.difference(not_change)),
            not_reused=list(all_candidates.difference(not_change)),
        ),
    )


async def calculate_heuristics(tx: Any, currency: str, get_address, heuristics: list) -> TxHeuristics:
    if "one_time_change" in heuristics:
        return TxHeuristics(one_time_change=await _one_time_change_heuristic(tx, currency, get_address))
    else:
        return TxHeuristics()
