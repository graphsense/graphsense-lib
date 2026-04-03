# noqa: E402
import asyncio
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Callable, Dict

from .common import cannonicalize_address  # noqa: E402
from graphsenselib.db.asynchronous.services.heuristics import (
    AddressOutput,
    ChangeHeuristics,
    CoinJoinConsensus,
    CoinJoinHeuristics,
    ConsensusEntry,
    DirectChangeHeuristic,
    MultiInputChangeDetails,
    MultiInputChangeHeuristic,
    MultiInputClusterEvidence,
    OneTimeChangeDetails,
    OneTimeChangeHeuristic,
    JoinMarketHeuristic,
    UtxoHeuristics,
    WasabiHeuristic,
    WhirlpoolCoinJoinHeuristic,
    WhirlpoolTx0Heuristic,
)  # noqa: E402


logger = logging.getLogger(__name__)


@dataclass
class CoinJoinDbCallbacks:
    get_spent_in: Callable
    get_tx: Callable
    get_tag_summary: Callable | None = None


# Whirlpool pools: (denomination_sat, coordinator_fee_sat)
WHIRLPOOL_POOLS = [
    (100_000, 5_000),
    (1_000_000, 50_000),
    (5_000_000, 175_000),
    (50_000_000, 1_750_000),
]
WASABI_10_DENOM_SAT = 10_000_000  # 0.1 BTC
WASABI_10_EPSILON_SAT = 500_000  # ±5% tolerance
WASABI_10_A_MAX = 7  # max inputs per participant

WASABI_11_A_MAX = 7  # same as 1.0

WASABI_20_A_MAX = 10  # max inputs per participant
WASABI_20_MIN_INPUTS = 50  # minimum total inputs
WASABI_20_V_MIN = 5_000  # minimum output value (sat)
WASABI_20_MIN_DENOM_FREQ = 2  # minimum frequency for a value to be a denomination

WHIRLPOOL_EPSILON_MIN = 100
WHIRLPOOL_EPSILON_MAX = 100_000
WHIRLPOOL_TX0_A_MAX = 70
WHIRLPOOL_ETA1 = 0.5
WHIRLPOOL_ETA2 = 3

WHIRLPOOL_TX0_MAX_FORWARD_CHECKS = 20
WHIRLPOOL_TX0_CONFIRMED_CONFIDENCE = 90


async def _prefetch_addresses(tx, currency, get_address) -> dict[str, dict]:
    """Batch-fetch all unique input+output addresses concurrently.

    get_address: async callable (currency, address) -> dict or object with
                 get_addresses_light(currency, addresses) -> dict for batch mode.

    Returns a dict mapping canonical address -> address info.
    """
    unique: set[str] = set()
    for io in list(tx.get("inputs") or []) + list(tx.get("outputs") or []):
        if io is None or not io.address:
            continue
        unique.add(cannonicalize_address(currency, io.address[0]))

    addrs = list(unique)

    if hasattr(get_address, "get_addresses_light"):
        return await get_address.get_addresses_light(currency, addrs)

    results = await asyncio.gather(*[get_address(currency, addr) for addr in addrs])
    return dict(zip(addrs, results))


def _one_time_change_heuristic(
    tx, currency, addr_cache: dict[str, dict]
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
        or len(tx.get("outputs") or []) > 10
        or len(tx.get("outputs") or []) < 2
    ):
        return OneTimeChangeHeuristic(summary=[], details=empty_details)

    cond_same_script = set()
    cond_not_nicely_divisible = set()
    cond_out_less_than_in = set()
    cond_not_reused = set()

    min_input_value = min(
        (inp.value for inp in tx.get("inputs") or [] if inp is not None), default=0
    )

    script_type_input = None
    for inp in tx.get("inputs") or []:
        if inp is None:
            continue
        if script_type_input is None:
            script_type_input = inp.address_type
        elif inp.address_type != script_type_input:
            script_type_input = False

        if not script_type_input:
            break

    counts: Dict[str, int] = defaultdict(int)
    for outp in tx.get("outputs") or []:
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
    for cand in tx.get("outputs") or []:
        if cand is None or not cand.address:
            continue

        addr = cand.address[0]
        addr_info = addr_cache.get(cannonicalize_address(currency, addr))
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
    for idx, outp in enumerate(tx.get("outputs") or []):
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


def _multi_input_change_heuristic(
    tx, currency, addr_cache: dict[str, dict]
) -> MultiInputChangeHeuristic:
    """
    Checks if the output address can be mapped to a cluster from the input addresses. If yes, it is marked as change.
    """
    result = MultiInputChangeHeuristic(summary=[], details=None)
    details = MultiInputChangeDetails(cluster={})
    if tx.get("coinbase"):
        return result

    inputs = tx.get("inputs") or []
    outputs = tx.get("outputs") or []

    # Build cluster_id -> first matching input address from pre-fetched cache
    cluster_to_inp: dict[int, str] = {}
    for inp in inputs:
        if inp is None or not inp.address:
            continue
        inp_addr = inp.address[0]
        canon = cannonicalize_address(currency, inp_addr)
        info = addr_cache.get(canon)
        if info is not None and info.get("cluster_id", -1) != -1:
            cid = info.get("cluster_id")
            if cid not in cluster_to_inp:
                cluster_to_inp[cid] = inp_addr

    for idx, outp in enumerate(outputs):
        if outp is None or not outp.address:
            continue

        outp_addr = outp.address[0]
        addr_out_info = addr_cache.get(cannonicalize_address(currency, outp_addr))
        if addr_out_info is None:
            continue
        addr = AddressOutput(address=outp_addr, index=idx)

        out_cluster = addr_out_info.get("cluster_id", -1)
        if out_cluster == -1:
            continue

        # Check if any input belongs to the same cluster
        matching_inp_addr = cluster_to_inp.get(out_cluster)
        if matching_inp_addr is not None:
            result.summary.append(addr)

            cluster_evidence = MultiInputClusterEvidence(
                matching_input_address=matching_inp_addr, output=addr
            )
            if out_cluster not in details.cluster:
                details.cluster[out_cluster] = [cluster_evidence]
            else:
                details.cluster[out_cluster].append(cluster_evidence)

    result.details = details
    return result


def _direct_change_heuristic(tx) -> DirectChangeHeuristic:
    """
    marks an address as change if it is used both as input and output in the same transaction.
    """
    result = DirectChangeHeuristic(summary=[])
    if tx.get("coinbase"):
        return result

    inputs = tx.get("inputs") or []
    outputs = tx.get("outputs") or []
    addr_inputs = set(
        inp.address[0] for inp in inputs if inp is not None and inp.address
    )

    # Single pass over outputs: check membership in input set (O(1) per lookup)
    for idx, outp in enumerate(outputs):
        if outp is not None and outp.address and outp.address[0] in addr_inputs:
            result.summary.append(AddressOutput(address=outp.address[0], index=idx))

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
                sources=sorted(list(set(list(entry.sources) + [key]))),
            )
    return consensus_map


def _build_coinjoin_consensus(coinjoin: CoinJoinHeuristics) -> CoinJoinConsensus | None:
    """Aggregate coinjoin heuristic results into a single consensus signal.

    whirlpool_tx0 is intentionally excluded — it is a pre-mix preparation
    transaction, not a CoinJoin itself.
    """
    sources = []
    confidence = 0
    candidates = [
        ("joinmarket_coinjoin", coinjoin.joinmarket),
        ("wasabi_coinjoin", coinjoin.wasabi),
        ("whirlpool_coinjoin", coinjoin.whirlpool_coinjoin),
    ]
    for name, result in candidates:
        if result is not None and result.detected:
            sources.append(name)
            confidence = max(confidence, result.confidence)

    if not sources:
        return None

    return CoinJoinConsensus(detected=True, confidence=confidence, sources=sources)


def _wasabi_10_heuristic(tx) -> WasabiHeuristic | None:
    """
    Structural check for Wasabi 1.0 (ZeroLink) CoinJoin transactions.
    Fixed denomination close to 0.1 BTC, one coordinator fee output,
    all output scripts distinct.
    """
    if tx.get("coinbase"):
        return None

    inputs = [i for i in tx.get("inputs") or [] if i is not None and i.address]
    outputs = [o for o in tx.get("outputs") or [] if o is not None and o.address]

    # find candidate denomination: most frequent output value within the window
    freq = Counter(
        o.value
        for o in outputs
        if abs(o.value - WASABI_10_DENOM_SAT) <= WASABI_10_EPSILON_SAT
    )
    if not freq:
        return None

    d, n = freq.most_common(1)[0]

    # 1. denomination window (explicit guard)
    if not (abs(d - WASABI_10_DENOM_SAT) <= WASABI_10_EPSILON_SAT):
        return None

    n_scripts_in = len({i.address[0] for i in inputs})
    n_scripts_out = len({o.address[0] for o in outputs})

    # 2. participant lower bound — coordinator fee accounts for one extra output
    if not (n >= (len(outputs) - 1) / 2):
        return None

    # 3. input bounds
    if not (n <= n_scripts_in <= WASABI_10_A_MAX * n):
        return None

    # 4. all output scripts distinct
    if n_scripts_out != len(outputs):
        return None

    return WasabiHeuristic(
        detected=True,
        confidence=70,
        version="1.0",
        n_participants=n,
        denominations=[d],
    )


JOINMARKET_MIN_PARTICIPANTS = 2
JOINMARKET_DUST_THRESHOLD = (
    2730  # outputs at or below this value excluded as denomination candidates
)
JOINMARKET_LOW_CONFIDENCE = 20  # confidence when only 2 equal-value outputs are found
JOINMARKET_CONFIDENCE = 49  # confidence when 3+ equal-value outputs are found


def _joinmarket_heuristic(tx) -> JoinMarketHeuristic | None:
    """
    Structural check for JoinMarket CoinJoin transactions.
    The most frequent output value is the denomination; its count estimates
    participant count n. JoinMarket is a superset — Wasabi 1.x and Whirlpool
    CoinJoin txs also satisfy these conditions.

    n=2 (exactly 2 equal-value outputs) is detected with low confidence (20)
    since equal output values can occur by coincidence. n>=3 uses confidence 49.
    """
    if tx.get("coinbase"):
        return None

    inputs = [i for i in tx.get("inputs") or [] if i is not None and i.address]
    outputs = [o for o in tx.get("outputs") or [] if o is not None and o.address]

    if not outputs or not inputs:
        return None

    # find most frequent output value, excluding dust
    freq = Counter(o.value for o in outputs if o.value > JOINMARKET_DUST_THRESHOLD)
    if not freq:
        return None

    d, n = freq.most_common(1)[0]

    n_scripts_in = len({i.address[0] for i in inputs})
    n_scripts_out = len({o.address[0] for o in outputs})

    # 1. majority of outputs are post-mix
    if not (n >= len(outputs) / 2):
        return None

    # 2. at least 2 participants, each with distinct input
    if not (JOINMARKET_MIN_PARTICIPANTS <= n <= n_scripts_in):
        return None

    # 3. all output scripts distinct
    if n_scripts_out != len(outputs):
        return None

    confidence = JOINMARKET_CONFIDENCE if n >= 3 else JOINMARKET_LOW_CONFIDENCE

    return JoinMarketHeuristic(
        detected=True,
        confidence=confidence,
        n_participants=n,
        pool_denomination=d,
    )


def _wasabi_11_heuristic(tx) -> WasabiHeuristic | None:
    """
    Structural check for Wasabi 1.1 (mixing levels) CoinJoin transactions.
    Base denomination d ≈ 0.1 BTC; post-mix outputs appear at 2^i × d across
    multiple levels, each level requiring ≥2 outputs.
    Confidence is scored dynamically based on level count, post-mix coverage,
    and how tightly n is bounded.
    """
    if tx.get("coinbase"):
        return None

    inputs = [i for i in tx.get("inputs") or [] if i is not None and i.address]
    outputs = [o for o in tx.get("outputs") or [] if o is not None and o.address]

    if not outputs or not inputs:
        return None

    # base denomination is a protocol constant — no need to infer from level 0 outputs
    d = WASABI_10_DENOM_SAT

    # discover active levels: iterate until level_d exceeds max output value
    max_output_value = max(o.value for o in outputs)
    level_counts: dict[int, int] = {}
    i = 0
    while True:
        level_d = (2**i) * d
        if level_d > max_output_value:
            break
        count = sum(
            1
            for o in outputs
            if abs(o.value - level_d) <= (2**i) * WASABI_10_EPSILON_SAT
        )
        if count >= 2:
            level_counts[i] = count
        i += 1

    if not level_counts:
        return None

    total_postmix = sum(level_counts.values())
    n_scripts_in = len({inp.address[0] for inp in inputs})
    n_scripts_out = len({o.address[0] for o in outputs})

    # n lower bound: max count at any single level (all must be distinct participants)
    n = max(level_counts.values())

    # input bounds
    if not (n <= n_scripts_in <= WASABI_11_A_MAX * n):
        return None

    # postmix majority: in any real CoinJoin, postmix outputs dominate
    # (each participant has ≥1 postmix and ≤1 change, plus one coordinator fee)
    # subtract 1 for coordinator fee, same as Wasabi 1.0 condition
    postmix_majority = total_postmix >= (len(outputs) - 1) / 2

    # original sum condition (paper eq. 22): uses n_scripts_in as proxy
    # can be too permissive when n_scripts_in >> n
    sum_condition = total_postmix >= len(outputs) - n_scripts_in - 1

    # at least one must hold, otherwise reject
    if not postmix_majority and not sum_condition:
        return None

    # all output scripts distinct
    if n_scripts_out != len(outputs):
        return None

    # confidence scoring: 0-100
    # n_tightness: how well participant count is bounded (1.0 = exact, single-level case)
    n_tightness = n / n_scripts_in

    # coverage: ratio of post-mix outputs to total outputs
    # expected ~2/3 if change is uniform in [0,1] per participant
    # normalized: coverage_score = min(coverage / expected_coverage, 1.0)
    # where expected_coverage = n / (1.5 * n + 1)
    # commented out until change distribution is better understood
    # coverage = total_postmix / len(outputs)
    # expected_coverage = n / (1.5 * n + 1)
    # coverage_score = min(coverage / expected_coverage, 1.0)

    min_tightness = 1 / WASABI_11_A_MAX
    confidence = int(50 + 50 * (n_tightness - min_tightness) / (1 - min_tightness))

    # penalize if only one structural condition holds
    if not (postmix_majority and sum_condition):
        confidence = confidence // 2
    version = "1.0" if list(level_counts.keys()) == [0] else "1.1"

    return WasabiHeuristic(
        detected=True,
        confidence=confidence,
        version=version,
        n_participants=n,
        denominations=sorted((2**i) * d for i in level_counts),
    )


def _wasabi_20_heuristic(tx) -> WasabiHeuristic | None:
    """
    Structural check for Wasabi 2.0 (WabiSabi) CoinJoin transactions.
    Variable denomination set D derived from output values appearing ≥2 times.
    Large rounds (≥50 inputs), all outputs above v_min, distinct scripts.
    """
    if tx.get("coinbase"):
        return None

    inputs = [i for i in tx.get("inputs") or [] if i is not None and i.address]
    outputs = [o for o in tx.get("outputs") or [] if o is not None and o.address]

    if not outputs or not inputs:
        return None

    # 1. large round: minimum input count
    if len(inputs) < WASABI_20_MIN_INPUTS:
        return None

    # 2. no tiny outputs
    if any(o.value < WASABI_20_V_MIN for o in outputs):
        return None

    # 3. all output scripts distinct
    n_scripts_out = len({o.address[0] for o in outputs})
    if n_scripts_out != len(outputs):
        return None

    # 4. derive denomination set D: output values appearing ≥ min_freq times
    freq = Counter(o.value for o in outputs)
    denom_set = {v for v, c in freq.items() if c >= WASABI_20_MIN_DENOM_FREQ}

    if not denom_set:
        return None

    n_denom_outputs = sum(1 for o in outputs if o.value in denom_set)

    # 5. at least half of outputs (minus coordinator fee) are denomination outputs
    if not (n_denom_outputs >= (len(outputs) - 1) / 2):
        return None

    # 6. enough denomination outputs relative to inputs
    if not (n_denom_outputs >= len(inputs) / WASABI_20_A_MAX):
        return None

    # participant estimate: number of distinct input scripts / a_max as lower bound
    n_scripts_in = len({i.address[0] for i in inputs})
    n_participants = max(n_scripts_in // WASABI_20_A_MAX, 1)

    return WasabiHeuristic(
        detected=True,
        confidence=60,
        version="2.0",
        n_participants=n_participants,
        denominations=sorted(denom_set),
    )


def _whirlpool_coinjoin_heuristic(tx) -> WhirlpoolCoinJoinHeuristic | None:
    """
    Structural check for Whirlpool CoinJoin transactions.
    Exactly 5 inputs and 5 outputs, all distinct scripts, all outputs at a known
    pool denomination, all inputs in [d, d + epsilon_max] with 1-4 new entrants.
    """
    if tx.get("coinbase"):
        return None

    inputs = [i for i in tx.get("inputs") or [] if i is not None and i.address]
    outputs = [o for o in tx.get("outputs") or [] if o is not None and o.address]

    # amount check
    if len(inputs) != 5 or len(outputs) != 5:
        return None

    # uniqueness check
    if len({i.address[0] for i in inputs}) != 5:
        return None
    if len({o.address[0] for o in outputs}) != 5:
        return None

    input_values = [i.value for i in inputs]
    output_values = [o.value for o in outputs]

    for d, _ in WHIRLPOOL_POOLS:
        if not all(v == d for v in output_values):
            continue
        if not all(d <= v <= d + WHIRLPOOL_EPSILON_MAX for v in input_values):
            continue

        # each new entrant needs to pay a fee
        new_entrant_epsilons = [v - d for v in input_values if v > d]
        if not all(
            WHIRLPOOL_EPSILON_MIN <= e <= WHIRLPOOL_EPSILON_MAX
            for e in new_entrant_epsilons
        ):
            continue

        # at least one old remix required and at least one new entrant
        n_new_entrants = len(new_entrant_epsilons)
        if not (1 <= n_new_entrants <= 4):
            continue

        return WhirlpoolCoinJoinHeuristic(
            detected=True,
            confidence=60,
            pool_denomination_sat=d,
            n_remixers=5 - n_new_entrants,
            n_new_entrants=n_new_entrants,
        )

    return None


def _whirlpool_tx0_heuristic(tx) -> WhirlpoolTx0Heuristic | None:
    """
    Structural check for Whirlpool Tx0 transactions.
    Identifies pre-mix outputs (d + epsilon), exactly one zero-value output,
    exactly one coordinator fee output, and validates against known pools.
    """
    if tx.get("coinbase"):
        return None

    all_outputs = [outp for outp in tx.get("outputs") or [] if outp is not None]
    op_return_outputs = [outp for outp in all_outputs if not outp.address]
    outputs = [outp for outp in all_outputs if outp.address]

    # exactly one OP_RETURN required
    if len(op_return_outputs) != 1:
        return None

    # minimum spendable outputs: at least 1 pre-mix + 1 fee
    if len(outputs) < 2:
        return None

    for d, f in WHIRLPOOL_POOLS:
        # find candidate pre-mix value: most frequent output value in [d+emin, d+emax]
        premix_range = [
            outp.value
            for outp in outputs
            if d + WHIRLPOOL_EPSILON_MIN <= outp.value <= d + WHIRLPOOL_EPSILON_MAX
        ]
        if not premix_range:
            continue

        freq = defaultdict(int)
        for v in premix_range:
            freq[v] += 1
        d_tilde = max(freq, key=lambda v: (freq[v], v))
        epsilon = d_tilde - d

        if not (WHIRLPOOL_EPSILON_MIN <= epsilon <= WHIRLPOOL_EPSILON_MAX):
            continue

        n_premix = freq[d_tilde]
        if not (1 <= n_premix <= WHIRLPOOL_TX0_A_MAX):
            continue
        if (
            n_premix < len(outputs) - 2
        ):  # at most 2 non-premix spendable outputs: fee + optional change
            continue

        fee_outputs = [
            outp
            for outp in outputs
            if WHIRLPOOL_ETA1 * f <= outp.value <= WHIRLPOOL_ETA2 * f
        ]
        if len(fee_outputs) != 1:
            continue

        return WhirlpoolTx0Heuristic(
            detected=True,
            confidence=60,
            pool_denomination_sat=d,
            n_premix_outputs=n_premix,
        )

    return None


async def _verify_whirlpool_lineage(
    tx, get_tx, pool_denomination, depth, _cancel: asyncio.Event | None = None
) -> bool:
    """
    Recursively verify that all inputs of a Whirlpool CoinJoin trace back to
    valid Tx0 or CoinJoin transactions up to the given depth.
    All inputs must verify — a single failure cancels all sibling checks.

    New entrant inputs (value > d) are terminal: their source must be a Tx0.
    Remixer inputs (value == d) recurse: their source must be a CoinJoin.
    """
    if depth == 0:
        return True

    # shared cancellation flag across all recursive branches
    if _cancel is None:
        _cancel = asyncio.Event()

    inputs = [i for i in tx.get("inputs") or [] if i is not None and i.address]

    async def verify_input(inp) -> bool:
        # bail out early if a sibling already failed
        if _cancel.is_set():
            return False

        src_tx = await get_tx(inp.spent_tx_id)

        # check again after await — cancel may have been set while fetching
        if _cancel.is_set():
            return False

        if src_tx is None:
            _cancel.set()
            return False

        if inp.value > pool_denomination:
            # new entrant: d + ε input must come from a Tx0, no further recursion needed
            result = _whirlpool_tx0_heuristic(src_tx) is not None
        else:
            # remixer: d input must come from a previous CoinJoin, recurse one level deeper
            if _whirlpool_coinjoin_heuristic(src_tx) is None:
                result = False
            else:
                result = await _verify_whirlpool_lineage(
                    src_tx, get_tx, pool_denomination, depth - 1, _cancel
                )

        # signal failure to all sibling coroutines
        if not result:
            _cancel.set()
        return result

    # all inputs verified in parallel; all must pass
    results = await asyncio.gather(*[verify_input(inp) for inp in inputs])
    return all(results)


async def _verify_tx0_forward(tx, pool_denomination, get_spent_in, get_tx) -> bool:
    """
    Check if any pre-mix output of a Tx0 candidate was spent in a
    Whirlpool CoinJoin. One confirmed hit is enough.
    """
    tx_hash_raw = tx.get("tx_hash")
    if not tx_hash_raw:
        return False
    tx_hash = (
        tx_hash_raw.hex()
        if isinstance(tx_hash_raw, (bytes, bytearray))
        else tx_hash_raw
    )

    d = pool_denomination
    all_outputs = [outp for outp in tx.get("outputs") or [] if outp is not None]
    premix_indices = [
        idx
        for idx, outp in enumerate(all_outputs)
        if outp.address
        and d + WHIRLPOOL_EPSILON_MIN <= outp.value <= d + WHIRLPOOL_EPSILON_MAX
    ]

    # limit to avoid excessive DB calls on large Tx0s
    premix_indices = premix_indices[:WHIRLPOOL_TX0_MAX_FORWARD_CHECKS]

    async def check_output(io_index: int) -> bool:
        spent_refs = await get_spent_in(tx_hash, io_index)
        if not spent_refs:
            return False
        for ref in spent_refs:
            spending_tx = await get_tx(ref.tx_hash)
            if spending_tx and _whirlpool_coinjoin_heuristic(spending_tx) is not None:
                return True
        return False

    results = await asyncio.gather(*[check_output(idx) for idx in premix_indices])
    return any(results)


async def _any_input_is_exchange(tx, currency, get_tag_summary) -> bool:
    """Return True if any input address is tagged as an exchange (broad_category == 'exchange').

    Fetches tag summaries for all unique input addresses concurrently.
    """
    unique_addrs = []
    seen = set()
    for inp in tx.get("inputs") or []:
        if inp is None or not inp.address:
            continue
        addr = inp.address[0]
        if addr not in seen:
            seen.add(addr)
            unique_addrs.append(addr)

    if not unique_addrs:
        return False

    summaries = await asyncio.gather(
        *[get_tag_summary(currency, addr) for addr in unique_addrs]
    )
    return any(s is not None and s.broad_category == "exchange" for s in summaries)


async def calculate_heuristics(
    tx,
    currency,
    get_address,
    heuristics: list[str],
    coinjoin_callbacks: CoinJoinDbCallbacks | None = None,
) -> UtxoHeuristics:
    heuristics_set = set(heuristics)
    cur = {currency.lower()}

    needs_address_cache = {"one_time_change", "multi_input_change", "all", "all_change"}
    utxo_currencies = {"btc", "ltc", "bch"}

    # Batch-prefetch all addresses once if any change heuristic needs them
    addr_cache: dict[str, dict] = {}
    if needs_address_cache & heuristics_set and cur & utxo_currencies:
        addr_cache = await _prefetch_addresses(tx, currency, get_address)

    heuristic_map: dict[str, object] = {}

    if {
        "one_time_change",
        "all",
        "all_change",
    } & heuristics_set and cur & utxo_currencies:
        heuristic_map["one_time_change"] = _one_time_change_heuristic(
            tx, currency, addr_cache
        )
    if {
        "direct_change",
        "all",
        "all_change",
    } & heuristics_set and cur & utxo_currencies:
        heuristic_map["direct_change"] = _direct_change_heuristic(tx)
    if {
        "multi_input_change",
        "all",
        "all_change",
    } & heuristics_set and cur & utxo_currencies:
        heuristic_map["multi_input_change"] = _multi_input_change_heuristic(
            tx, currency, addr_cache
        )
    consensus_map = _build_change_consensus_map(heuristic_map)

    # only allow the highest confidence addr. match to be in the consensus
    if len(consensus_map) > 1:
        confidence_max = max(entry.confidence for entry in consensus_map.values())
        consensus_map = {
            addr: entry
            for addr, entry in consensus_map.items()
            if entry.confidence == confidence_max
        }

    coinjoin = None
    if {"whirlpool_coinjoin", "all", "all_coinjoin"} & heuristics_set and cur & {"btc"}:
        whirlpool_coinjoin = _whirlpool_coinjoin_heuristic(tx)
        whirlpool_tx0 = _whirlpool_tx0_heuristic(tx)

        # forward verification: if Tx0 detected and DB callbacks available,
        # check if pre-mix outputs were spent in Whirlpool CoinJoins
        if whirlpool_tx0 is not None and coinjoin_callbacks is not None:
            confirmed = await _verify_tx0_forward(
                tx,
                whirlpool_tx0.pool_denomination_sat,
                coinjoin_callbacks.get_spent_in,
                coinjoin_callbacks.get_tx,
            )
            if confirmed:
                whirlpool_tx0.confidence = WHIRLPOOL_TX0_CONFIRMED_CONFIDENCE
        elif whirlpool_tx0 is not None:
            logger.warning(
                "whirlpool_tx0 detected but coinjoin_callbacks not available"
            )

        if whirlpool_coinjoin is not None or whirlpool_tx0 is not None:
            coinjoin = CoinJoinHeuristics(
                whirlpool_coinjoin=whirlpool_coinjoin,
                whirlpool_tx0=whirlpool_tx0,
            )

    if {
        "wasabi_2_0_coinjoin",
        "wasabi_coinjoin",
        "all",
        "all_coinjoin",
    } & heuristics_set and cur & {"btc"}:
        wasabi_20_result = _wasabi_20_heuristic(tx)
        if wasabi_20_result is not None:
            if coinjoin is None:
                coinjoin = CoinJoinHeuristics()
            coinjoin.wasabi = wasabi_20_result

    if {
        "wasabi_1_0_coinjoin",
        "wasabi_1_1_coinjoin",
        "wasabi_coinjoin",
        "all",
        "all_coinjoin",
    } & heuristics_set and cur & {"btc"}:
        wasabi_result = _wasabi_11_heuristic(tx)
        # 1.x only overwrites if 2.0 didn't match (2.0 is more specific)
        if wasabi_result is not None and (coinjoin is None or coinjoin.wasabi is None):
            if coinjoin is None:
                coinjoin = CoinJoinHeuristics()
            coinjoin.wasabi = wasabi_result

    if {"joinmarket_coinjoin", "all", "all_coinjoin"} & heuristics_set and cur & {
        "btc",
        "ltc",
        "bch",
    }:
        joinmarket_result = _joinmarket_heuristic(tx)
        if joinmarket_result is not None:
            if coinjoin is None:
                coinjoin = CoinJoinHeuristics()
            coinjoin.joinmarket = joinmarket_result

    # Exchange false-positive suppression: if JoinMarket or Wasabi 1.x fired, check
    # whether any input comes from a known exchange. If yes, remove those results —
    # exchange batch payouts structurally resemble these CoinJoin protocols.
    # Wasabi 2.0 and Whirlpool are intentionally excluded (different FP profile).
    if (
        coinjoin is not None
        and coinjoin_callbacks is not None
        and coinjoin_callbacks.get_tag_summary is not None
        and (
            coinjoin.joinmarket is not None
            or (
                coinjoin.wasabi is not None
                and coinjoin.wasabi.version in ("1.0", "1.1")
            )
        )
        and await _any_input_is_exchange(
            tx, currency, coinjoin_callbacks.get_tag_summary
        )
    ):
        coinjoin.joinmarket = None
        if coinjoin.wasabi is not None and coinjoin.wasabi.version in ("1.0", "1.1"):
            coinjoin.wasabi = None

    if coinjoin is not None:
        coinjoin.consensus = _build_coinjoin_consensus(coinjoin)

    return UtxoHeuristics(
        change_heuristics=ChangeHeuristics(
            consensus=list(consensus_map.values()),
            one_time_change=heuristic_map.get("one_time_change"),
            direct_change=heuristic_map.get("direct_change"),
            multi_input_change=heuristic_map.get("multi_input_change"),
        ),
        coinjoin_heuristics=coinjoin,
    )
