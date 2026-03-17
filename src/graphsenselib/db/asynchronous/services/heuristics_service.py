import asyncio
from collections import defaultdict
from typing import Dict


from .common import cannonicalize_address
from graphsenselib.db.asynchronous.services.heuristics import (
    AddressOutput,
    ChangeHeuristics,
    ConsensusEntry,
    DirectChangeHeuristic,
    MultiInputChangeDetails,
    MultiInputChangeHeuristic,
    MultiInputClusterEvidence,
    OneTimeChangeDetails,
    OneTimeChangeHeuristic,
    UtxoHeuristics,
)


async def _one_time_change_heuristic(
    tx, currency, get_address
) -> OneTimeChangeHeuristic:
    """
    Combining multiple heuristics to identify one time change addresses.
    Needs to be one time otherwise too many conditions will be true.
    """
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
        return OneTimeChangeHeuristic(summary=[], details=empty_details)

    cond_same_script = set()
    cond_not_nicely_divisible = set()
    cond_out_less_than_in = set()
    cond_not_reused = set()

    min_input_value = min(
        (inp.value for inp in tx.get("inputs", []) if inp is not None), default=0
    )

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
    change_candidates = cond_same_script.intersection(
        cond_not_nicely_divisible
    ).intersection(cond_out_less_than_in)
    not_change = set()
    for cand in tx.get("outputs", []):
        if cand is None or not cand.address:
            continue

        addr = cand.address[0]
        addr_info = await get_address(currency, cannonicalize_address(currency, addr))
        if addr_info is None:
            cond_not_reused.add(addr)
            continue

        first_tx_height = (
            addr_info["first_tx"].height if addr_info.get("first_tx") else None
        )

        if (
            addr_info.get("no_incoming_txs", 0) > 1
            or addr_info.get("no_outgoing_txs", 0) > 1
            or (first_tx_height is not None and first_tx_height < tx.get("block_id"))
        ):
            not_change.add(addr)
        else:
            cond_not_reused.add(addr)

    # since we have this, outputs do not matter for this heuristic --> only adding the output at the end
    same_addr_more_than_once = set(addr for addr, count in counts.items() if count > 1)
    change_candidates = change_candidates.intersection(cond_not_reused).difference(
        same_addr_more_than_once
    )
    if len(change_candidates) != 1:
        change_candidates = set()

    # converting simple address to AddressOutput
    summary = []
    same_script_type_addr_out = list()
    not_nicely_divisible_addr_out = list()
    output_less_than_input_addr_out = list()
    not_reused_addr_out = list()
    for idx, outp in enumerate(tx.get("outputs", [])):
        if outp is None or not outp.address:
            continue

        outp_addr = outp.address[0]
        addr = AddressOutput(address=outp_addr, index=idx)
        if outp_addr in change_candidates:
            summary.append(addr)

        if outp_addr in cond_same_script:
            same_script_type_addr_out.append(addr)
        if outp_addr in cond_not_nicely_divisible:
            not_nicely_divisible_addr_out.append(addr)
        if outp_addr in cond_out_less_than_in:
            output_less_than_input_addr_out.append(addr)
        if outp_addr in cond_not_reused:
            not_reused_addr_out.append(addr)

    return OneTimeChangeHeuristic(
        summary=summary,
        details=OneTimeChangeDetails(
            same_script_type=same_script_type_addr_out,
            not_nicely_divisible=not_nicely_divisible_addr_out,
            output_less_than_input=output_less_than_input_addr_out,
            not_reused=not_reused_addr_out,
        ),
    )


async def _multi_input_change_heuristic(
    tx, currency, get_address
) -> MultiInputChangeHeuristic:
    """
    Checks if the output address can be mapped to a cluster from the input addresses. If yes, it is marked as change.
    """
    result = MultiInputChangeHeuristic(summary=[], details=None)
    details = MultiInputChangeDetails(cluster={})
    if tx.get("coinbase"):
        return result

    inputs = tx.get("inputs", [])
    outputs = tx.get("outputs", [])

    for idx, outp in enumerate(outputs):
        if outp is None or not outp.address:
            continue

        outp_addr = outp.address[0]
        addr_out_info = await get_address(
            currency, cannonicalize_address(currency, outp_addr)
        )
        addr = AddressOutput(address=outp_addr, index=idx)

        if addr_out_info.get("cluster_id", -1) == -1:
            continue

        for in_idx, inp in enumerate(inputs):
            if inp is None or not inp.address:
                continue

            inp_addr = inp.address[0]
            addr_inp_info = await get_address(
                currency, cannonicalize_address(currency, inp_addr)
            )

            if addr_inp_info.get("cluster_id", -1) == -1:
                continue

            # same cluster
            if addr_inp_info.get("cluster_id") == addr_out_info.get("cluster_id"):
                result.summary.append(addr)

                cluster_id = addr_inp_info.get("cluster_id")
                cluster_evidence = MultiInputClusterEvidence(
                    matching_input_address=inp_addr, output=addr
                )
                if cluster_id not in details.cluster:
                    details.cluster[cluster_id] = [cluster_evidence]
                else:
                    details.cluster[cluster_id].append(cluster_evidence)

                break

    result.details = details
    return result


async def _direct_change_heuristic(tx) -> DirectChangeHeuristic:
    """
    marks an address as change if it is used both as input and output in the same transaction.
    """
    result = DirectChangeHeuristic(summary=[])
    if tx.get("coinbase"):
        return result

    inputs = tx.get("inputs", [])
    outputs = tx.get("outputs", [])
    addr_inputs = set(
        [inp.address[0] for inp in inputs if inp is not None and inp.address]
    )
    addr_outputs = set(
        [outp.address[0] for outp in outputs if outp is not None and outp.address]
    )
    intersection = addr_inputs.intersection(addr_outputs)

    for addr in intersection:
        for idx, outp in enumerate(outputs):
            if outp is not None and outp.address and outp.address[0] == addr:
                result.summary.append(AddressOutput(address=addr, index=idx))

    return result


def _build_change_consensus_map(
    heuristic_map: dict[
        str,
        OneTimeChangeHeuristic | DirectChangeHeuristic | MultiInputChangeHeuristic,
    ],
) -> dict[str, ConsensusEntry]:
    """Aggregate per-heuristic outputs into one consensus map keyed by address."""
    consensus_map: dict[str, ConsensusEntry] = {}
    for key, result in heuristic_map.items():
        for addr in result.summary:
            if addr.address not in consensus_map:
                consensus_map[addr.address] = ConsensusEntry(
                    output=addr,
                    confidence=result.confidence,
                    sources=[key],
                )
                continue

            entry = consensus_map[addr.address]
            consensus_map[addr.address] = ConsensusEntry(
                output=addr,
                confidence=max(entry.confidence, result.confidence),
                sources=entry.sources + [key],
            )
    return consensus_map


async def calculate_heuristics(
    tx, currency, get_address, heuristics: list
) -> UtxoHeuristics:
    tasks = []
    keys = []

    if "one_time_change" in heuristics or "all" in heuristics:
        tasks.append(_one_time_change_heuristic(tx, currency, get_address))
        keys.append("one_time_change")
    if "direct_change" in heuristics or "all" in heuristics:
        tasks.append(_direct_change_heuristic(tx))
        keys.append("direct_change")
    if "multi_input_change" in heuristics or "all" in heuristics:
        tasks.append(_multi_input_change_heuristic(tx, currency, get_address))
        keys.append("multi_input_change")

    results = await asyncio.gather(*tasks)
    heuristic_map = dict(zip(keys, results))
    consensus_map = _build_change_consensus_map(heuristic_map)

    return UtxoHeuristics(
        change_heuristics=ChangeHeuristics(
            consensus=list(consensus_map.values()),
            one_time_change=heuristic_map.get("one_time_change"),
            direct_change=heuristic_map.get("direct_change"),
            multi_input_change=heuristic_map.get("multi_input_change"),
        ),
    )
