"""
Tests for UTXO heuristics (change detection and CoinJoin detection).
All transactions are constructed as simple namespaces mirroring the object
structure used in heuristics_service.py (`.address`, `.value`, `.address_type`).
OP_RETURN outputs are objects with an empty address list and zero value.
"""

from types import SimpleNamespace

from graphsenselib.db.asynchronous.services.heuristics_service import (
    _joinmarket_heuristic,
    _wasabi_10_heuristic,
    _wasabi_11_heuristic,
    _wasabi_20_heuristic,
    _whirlpool_tx0_heuristic,
    _whirlpool_coinjoin_heuristic,
    JOINMARKET_DUST_THRESHOLD,
    WASABI_10_DENOM_SAT,
    WASABI_10_EPSILON_SAT,
    WASABI_10_A_MAX,
    WASABI_11_A_MAX,
    WHIRLPOOL_POOLS,
    WHIRLPOOL_EPSILON_MIN,
    WHIRLPOOL_EPSILON_MAX,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_output(value, address="addr_out"):
    return SimpleNamespace(value=value, address=[address], address_type="p2wpkh")


def make_op_return():
    """OP_RETURN output: spendable=False, empty address, zero value."""
    return SimpleNamespace(value=0, address=[], address_type=None)


def make_input(value, address="addr_in"):
    return SimpleNamespace(value=value, address=[address], address_type="p2wpkh")


def make_tx(inputs, outputs, coinbase=False):
    return {"inputs": inputs, "outputs": outputs, "coinbase": coinbase}


# ---------------------------------------------------------------------------
# Tx0 — happy path
# ---------------------------------------------------------------------------


class TestWhirlpoolTx0HappyPath:
    # Use the 0.01 BTC pool for all tests in this class
    D, F = WHIRLPOOL_POOLS[1]  # 1_000_000 sat, 50_000 sat fee
    EPS = 5_000  # a valid epsilon within [EPSILON_MIN, EPSILON_MAX]

    def test_minimal_valid_tx0(self):
        """Minimal Tx0: 1 pre-mix output + fee output + OP_RETURN."""
        tx = make_tx(
            inputs=[make_input(2_000_000, "wallet_addr")],
            outputs=[
                make_output(self.D + self.EPS, "premix_1"),  # pre-mix
                make_output(self.F, "coordinator"),  # fee
                make_op_return(),  # OP_RETURN
            ],
        )
        result = _whirlpool_tx0_heuristic(tx)
        assert result is not None
        assert result.detected is True
        assert result.pool_denomination_sat == self.D
        assert result.n_premix_outputs == 1

    def test_multiple_premix_outputs(self):
        """Typical Tx0: 4 pre-mix outputs + fee + OP_RETURN."""
        tx = make_tx(
            inputs=[make_input(5_000_000, "wallet_addr")],
            outputs=[
                make_output(self.D + self.EPS, "premix_1"),
                make_output(self.D + self.EPS, "premix_2"),
                make_output(self.D + self.EPS, "premix_3"),
                make_output(self.D + self.EPS, "premix_4"),
                make_output(self.F, "coordinator"),
                make_op_return(),
            ],
        )
        result = _whirlpool_tx0_heuristic(tx)
        assert result is not None
        assert result.n_premix_outputs == 4

    def test_tx0_with_change_output(self):
        """Tx0 with optional change output."""
        tx = make_tx(
            inputs=[make_input(5_000_000, "wallet_addr")],
            outputs=[
                make_output(self.D + self.EPS, "premix_1"),
                make_output(self.D + self.EPS, "premix_2"),
                make_output(self.D + self.EPS, "premix_3"),
                make_output(self.F, "coordinator"),
                make_output(300_000, "change"),  # outside fee range [25k, 150k]
                make_op_return(),
            ],
        )
        result = _whirlpool_tx0_heuristic(tx)
        assert result is not None
        assert result.n_premix_outputs == 3

    def test_discounted_coordinator_fee(self):
        """Coupon discount: fee at 0.5×f should still be accepted."""
        tx = make_tx(
            inputs=[make_input(2_000_000, "wallet_addr")],
            outputs=[
                make_output(self.D + self.EPS, "premix_1"),
                make_output(int(self.F * 0.5), "coordinator"),  # minimum accepted fee
                make_op_return(),
            ],
        )
        result = _whirlpool_tx0_heuristic(tx)
        assert result is not None

    def test_elevated_coordinator_fee(self):
        """Fee at 3×f should still be accepted."""
        tx = make_tx(
            inputs=[make_input(4_000_000, "wallet_addr")],
            outputs=[
                make_output(self.D + self.EPS, "premix_1"),
                make_output(self.F * 3, "coordinator"),
                make_op_return(),
            ],
        )
        result = _whirlpool_tx0_heuristic(tx)
        assert result is not None

    def test_epsilon_at_minimum(self):
        """Pre-mix value exactly at d + epsilon_min."""
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(self.D + WHIRLPOOL_EPSILON_MIN, "premix_1"),
                make_output(self.F, "coordinator"),
                make_op_return(),
            ],
        )
        result = _whirlpool_tx0_heuristic(tx)
        assert result is not None

    def test_epsilon_at_maximum(self):
        """Pre-mix value exactly at d + epsilon_max."""
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(self.D + WHIRLPOOL_EPSILON_MAX, "premix_1"),
                make_output(self.F, "coordinator"),
                make_op_return(),
            ],
        )
        result = _whirlpool_tx0_heuristic(tx)
        assert result is not None

    def test_correct_pool_selected(self):
        """Each pool denomination is detected correctly."""
        for d, f in WHIRLPOOL_POOLS:
            tx = make_tx(
                inputs=[make_input(d * 3)],
                outputs=[
                    make_output(d + self.EPS, "premix_1"),
                    make_output(d + self.EPS, "premix_2"),
                    make_output(f, "coordinator"),
                    make_op_return(),
                ],
            )
            result = _whirlpool_tx0_heuristic(tx)
            assert result is not None, f"Failed for pool denomination {d}"
            assert result.pool_denomination_sat == d


# ---------------------------------------------------------------------------
# Tx0 — rejection cases
# ---------------------------------------------------------------------------


class TestWhirlpoolTx0Rejection:
    D, F = WHIRLPOOL_POOLS[1]
    EPS = 5_000

    def test_coinbase_rejected(self):
        tx = make_tx(inputs=[], outputs=[], coinbase=True)
        assert _whirlpool_tx0_heuristic(tx) is None

    def test_no_op_return_rejected(self):
        """Without OP_RETURN the tx must be rejected."""
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(self.D + self.EPS, "premix_1"),
                make_output(self.F, "coordinator"),
            ],
        )
        assert _whirlpool_tx0_heuristic(tx) is None

    def test_two_op_returns_rejected(self):
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(self.D + self.EPS, "premix_1"),
                make_output(self.F, "coordinator"),
                make_op_return(),
                make_op_return(),  # second OP_RETURN — invalid
            ],
        )
        assert _whirlpool_tx0_heuristic(tx) is None

    def test_no_coordinator_fee_rejected(self):
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(self.D + self.EPS, "premix_1"),
                make_output(self.D + self.EPS, "premix_2"),
                make_op_return(),
            ],
        )
        assert _whirlpool_tx0_heuristic(tx) is None

    def test_fee_below_range_rejected(self):
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(self.D + self.EPS, "premix_1"),
                make_output(int(self.F * 0.4), "coordinator"),  # below 0.5×f
                make_op_return(),
            ],
        )
        assert _whirlpool_tx0_heuristic(tx) is None

    def test_fee_above_range_rejected(self):
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(self.D + self.EPS, "premix_1"),
                make_output(self.F * 4, "coordinator"),  # above 3×f
                make_op_return(),
            ],
        )
        assert _whirlpool_tx0_heuristic(tx) is None

    def test_epsilon_below_minimum_rejected(self):
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(self.D + WHIRLPOOL_EPSILON_MIN - 1, "premix_1"),
                make_output(self.F, "coordinator"),
                make_op_return(),
            ],
        )
        assert _whirlpool_tx0_heuristic(tx) is None

    def test_epsilon_above_maximum_rejected(self):
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(self.D + WHIRLPOOL_EPSILON_MAX + 1, "premix_1"),
                make_output(self.F, "coordinator"),
                make_op_return(),
            ],
        )
        assert _whirlpool_tx0_heuristic(tx) is None

    def test_too_few_outputs_rejected(self):
        """Less than 2 spendable outputs (fee + at least 1 pre-mix)."""
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(self.F, "coordinator"),
                make_op_return(),
            ],
        )
        assert _whirlpool_tx0_heuristic(tx) is None

    def test_premix_value_is_exactly_d_rejected(self):
        """Pre-mix must be d + ε, not d itself."""
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(self.D, "premix_1"),  # exactly d — no epsilon
                make_output(self.F, "coordinator"),
                make_op_return(),
            ],
        )
        assert _whirlpool_tx0_heuristic(tx) is None

    def test_wrong_pool_denomination_rejected(self):
        """Pre-mix value not matching any pool denomination."""
        tx = make_tx(
            inputs=[make_input(2_000_000)],
            outputs=[
                make_output(
                    500_000 + self.EPS, "premix_1"
                ),  # between 100k and 1M pools
                make_output(self.F, "coordinator"),
                make_op_return(),
            ],
        )
        assert _whirlpool_tx0_heuristic(tx) is None


# ---------------------------------------------------------------------------
# CoinJoin — happy path
# ---------------------------------------------------------------------------


class TestWhirlpoolCoinJoinHappyPath:
    D, F = WHIRLPOOL_POOLS[1]  # 1_000_000 sat, 50_000 sat fee
    EPS = 5_000  # valid epsilon for new entrant inputs

    def _make_coinjoin(self, n_new_entrants, pool=None, size=5):
        """Build a valid Whirlpool CoinJoin with n_new_entrants new entrants."""
        d = pool or self.D
        inputs = [
            make_input(d + self.EPS, f"new_{i}") for i in range(n_new_entrants)
        ] + [make_input(d, f"remix_{i}") for i in range(size - n_new_entrants)]
        outputs = [make_output(d, f"out_{i}") for i in range(size)]
        return make_tx(inputs, outputs)

    def test_one_new_entrant(self):
        """Minimum new entrants: 1 new entrant + 4 remixers."""
        result = _whirlpool_coinjoin_heuristic(self._make_coinjoin(1))
        assert result is not None
        assert result.detected is True
        assert result.n_new_entrants == 1
        assert result.n_remixers == 4
        assert result.pool_denomination_sat == self.D

    def test_two_new_entrants(self):
        """Typical case: 2 new entrants + 3 remixers."""
        result = _whirlpool_coinjoin_heuristic(self._make_coinjoin(2))
        assert result is not None
        assert result.n_new_entrants == 2
        assert result.n_remixers == 3

    def test_three_new_entrants(self):
        result = _whirlpool_coinjoin_heuristic(self._make_coinjoin(3))
        assert result is not None
        assert result.n_new_entrants == 3
        assert result.n_remixers == 2

    def test_four_new_entrants(self):
        """Maximum new entrants: 4 new entrants + 1 remixer."""
        result = _whirlpool_coinjoin_heuristic(self._make_coinjoin(4))
        assert result is not None
        assert result.n_new_entrants == 4
        assert result.n_remixers == 1

    def test_epsilon_at_minimum(self):
        """New entrant input with epsilon at exactly ε_min."""
        inputs = [make_input(self.D + WHIRLPOOL_EPSILON_MIN, "new_0")] + [
            make_input(self.D, f"remix_{i}") for i in range(4)
        ]
        outputs = [make_output(self.D, f"out_{i}") for i in range(5)]
        result = _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs))
        assert result is not None

    def test_epsilon_at_maximum(self):
        """New entrant input with epsilon at exactly ε_max."""
        inputs = [make_input(self.D + WHIRLPOOL_EPSILON_MAX, "new_0")] + [
            make_input(self.D, f"remix_{i}") for i in range(4)
        ]
        outputs = [make_output(self.D, f"out_{i}") for i in range(5)]
        result = _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs))
        assert result is not None

    def test_correct_pool_selected(self):
        """All 4 known pools are detected with correct denomination."""
        for d, _ in WHIRLPOOL_POOLS:
            result = _whirlpool_coinjoin_heuristic(self._make_coinjoin(2, pool=d))
            assert result is not None, f"Failed for pool denomination {d}"
            assert result.pool_denomination_sat == d

    # --- Surge cycle tests (6/7/8 inputs) ---

    def test_surge_6x6(self):
        """Surge cycle: 6 inputs, 6 outputs — 2 new entrants + 4 remixers."""
        result = _whirlpool_coinjoin_heuristic(self._make_coinjoin(2, size=6))
        assert result is not None
        assert result.n_new_entrants == 2
        assert result.n_remixers == 4

    def test_surge_7x7(self):
        """Surge cycle: 7 inputs, 7 outputs — 3 new entrants + 4 remixers."""
        result = _whirlpool_coinjoin_heuristic(self._make_coinjoin(3, size=7))
        assert result is not None
        assert result.n_new_entrants == 3
        assert result.n_remixers == 4

    def test_surge_8x8(self):
        """Surge cycle: 8 inputs, 8 outputs — 4 new entrants + 4 remixers."""
        result = _whirlpool_coinjoin_heuristic(self._make_coinjoin(4, size=8))
        assert result is not None
        assert result.n_new_entrants == 4
        assert result.n_remixers == 4

    def test_surge_8x8_one_new_entrant(self):
        """Surge cycle: 8 inputs — 1 new entrant + 7 remixers."""
        result = _whirlpool_coinjoin_heuristic(self._make_coinjoin(1, size=8))
        assert result is not None
        assert result.n_new_entrants == 1
        assert result.n_remixers == 7


# ---------------------------------------------------------------------------
# CoinJoin — rejection cases
# ---------------------------------------------------------------------------


class TestWhirlpoolCoinJoinRejection:
    D, F = WHIRLPOOL_POOLS[1]
    EPS = 5_000

    def _make_coinjoin(self, n_new_entrants=2):
        inputs = [
            make_input(self.D + self.EPS, f"new_{i}") for i in range(n_new_entrants)
        ] + [make_input(self.D, f"remix_{i}") for i in range(5 - n_new_entrants)]
        outputs = [make_output(self.D, f"out_{i}") for i in range(5)]
        return make_tx(inputs, outputs)

    def test_coinbase_rejected(self):
        tx = make_tx(inputs=[], outputs=[], coinbase=True)
        assert _whirlpool_coinjoin_heuristic(tx) is None

    def test_too_few_inputs_rejected(self):
        inputs = [make_input(self.D, f"remix_{i}") for i in range(4)]
        outputs = [make_output(self.D, f"out_{i}") for i in range(5)]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None

    def test_too_many_inputs_rejected(self):
        """9 inputs exceeds surge cycle maximum of 8."""
        inputs = [make_input(self.D, f"remix_{i}") for i in range(9)]
        outputs = [make_output(self.D, f"out_{i}") for i in range(9)]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None

    def test_too_few_outputs_rejected(self):
        inputs = [make_input(self.D + self.EPS, f"new_{i}") for i in range(2)] + [
            make_input(self.D, f"remix_{i}") for i in range(3)
        ]
        outputs = [make_output(self.D, f"out_{i}") for i in range(4)]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None

    def test_mismatched_input_output_count_rejected(self):
        """6 inputs but 5 outputs — must have equal counts."""
        inputs = [make_input(self.D + self.EPS, f"new_{i}") for i in range(2)] + [
            make_input(self.D, f"remix_{i}") for i in range(4)
        ]
        outputs = [make_output(self.D, f"out_{i}") for i in range(5)]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None

    def test_duplicate_input_script_rejected(self):
        """Two inputs sharing the same address — not distinct participants."""
        inputs = [
            make_input(self.D + self.EPS, "new_0"),
            make_input(self.D + self.EPS, "new_0"),  # duplicate
            make_input(self.D, "remix_0"),
            make_input(self.D, "remix_1"),
            make_input(self.D, "remix_2"),
        ]
        outputs = [make_output(self.D, f"out_{i}") for i in range(5)]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None

    def test_duplicate_output_script_rejected(self):
        """Two outputs sharing the same address."""
        inputs = [make_input(self.D + self.EPS, f"new_{i}") for i in range(2)] + [
            make_input(self.D, f"remix_{i}") for i in range(3)
        ]
        outputs = [
            make_output(self.D, "out_0"),
            make_output(self.D, "out_0"),  # duplicate
            make_output(self.D, "out_2"),
            make_output(self.D, "out_3"),
            make_output(self.D, "out_4"),
        ]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None

    def test_output_wrong_denomination_rejected(self):
        """One output not at pool denomination d."""
        inputs = [make_input(self.D + self.EPS, f"new_{i}") for i in range(2)] + [
            make_input(self.D, f"remix_{i}") for i in range(3)
        ]
        outputs = [make_output(self.D, f"out_{i}") for i in range(4)] + [
            make_output(self.D + 1, "out_4")  # off by 1
        ]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None

    def test_input_below_d_rejected(self):
        """Input value below pool denomination — neither remixer nor new entrant."""
        inputs = [make_input(self.D - 1, "low_0")] + [
            make_input(self.D, f"remix_{i}") for i in range(4)
        ]
        outputs = [make_output(self.D, f"out_{i}") for i in range(5)]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None

    def test_input_above_epsilon_max_rejected(self):
        """New entrant epsilon exceeds ε_max."""
        inputs = [make_input(self.D + WHIRLPOOL_EPSILON_MAX + 1, "new_0")] + [
            make_input(self.D, f"remix_{i}") for i in range(4)
        ]
        outputs = [make_output(self.D, f"out_{i}") for i in range(5)]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None

    def test_epsilon_below_minimum_rejected(self):
        """New entrant epsilon below ε_min."""
        inputs = [make_input(self.D + WHIRLPOOL_EPSILON_MIN - 1, "new_0")] + [
            make_input(self.D, f"remix_{i}") for i in range(4)
        ]
        outputs = [make_output(self.D, f"out_{i}") for i in range(5)]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None

    def test_all_remixers_rejected(self):
        """All 5 inputs are remixers — no surplus to pay miner fee."""
        inputs = [make_input(self.D, f"remix_{i}") for i in range(5)]
        outputs = [make_output(self.D, f"out_{i}") for i in range(5)]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None

    def test_all_new_entrants_rejected(self):
        """All 5 inputs are new entrants — protocol requires at least 1 remixer."""
        inputs = [make_input(self.D + self.EPS, f"new_{i}") for i in range(5)]
        outputs = [make_output(self.D, f"out_{i}") for i in range(5)]
        assert _whirlpool_coinjoin_heuristic(make_tx(inputs, outputs)) is None


# ---------------------------------------------------------------------------
# Wasabi 1.0 — happy path
# ---------------------------------------------------------------------------


class TestWasabi10HappyPath:
    D = WASABI_10_DENOM_SAT  # 10_000_000 sat
    FEE = 50_000  # coordinator fee (arbitrary unique value)

    def _make_wasabi10(self, n, n_change=None, fee=None, inputs_per_participant=1):
        """Build a valid Wasabi 1.0 tx with n participants."""
        if n_change is None:
            n_change = n  # all participants have change by default
        fee = fee or self.FEE
        outputs = (
            [make_output(self.D, f"postmix_{i}") for i in range(n)]
            + [
                make_output(self.D // 3 + i * 1001, f"change_{i}")
                for i in range(n_change)
            ]
            + [make_output(fee, "coordinator")]
        )
        inputs = [
            make_input(self.D * 2, f"inp_{i}_{j}")
            for i in range(n)
            for j in range(inputs_per_participant)
        ]
        return make_tx(inputs, outputs)

    def test_minimal_valid(self):
        """3 participants, all with change."""
        result = _wasabi_10_heuristic(self._make_wasabi10(3))
        assert result is not None
        assert result.detected is True
        assert result.version == "1.0"
        assert result.n_participants == 3
        assert result.denominations == [self.D]

    def test_typical_round(self):
        """10 participants, all with change."""
        result = _wasabi_10_heuristic(self._make_wasabi10(10))
        assert result is not None
        assert result.n_participants == 10

    def test_no_change_outputs(self):
        """Participants with no change — fewer total outputs."""
        result = _wasabi_10_heuristic(self._make_wasabi10(5, n_change=0))
        assert result is not None
        assert result.n_participants == 5

    def test_partial_change(self):
        """Only some participants have change outputs."""
        result = _wasabi_10_heuristic(self._make_wasabi10(6, n_change=3))
        assert result is not None
        assert result.n_participants == 6

    def test_denomination_at_lower_bound(self):
        """Post-mix value at d - epsilon."""
        d = WASABI_10_DENOM_SAT - WASABI_10_EPSILON_SAT
        outputs = (
            [make_output(d, f"postmix_{i}") for i in range(5)]
            + [make_output(d // 3 + i * 1001, f"change_{i}") for i in range(5)]
            + [make_output(self.FEE, "coordinator")]
        )
        inputs = [make_input(d * 2, f"inp_{i}") for i in range(5)]
        result = _wasabi_10_heuristic(make_tx(inputs, outputs))
        assert result is not None
        assert result.denominations == [d]

    def test_denomination_at_upper_bound(self):
        """Post-mix value at d + epsilon."""
        d = WASABI_10_DENOM_SAT + WASABI_10_EPSILON_SAT
        outputs = (
            [make_output(d, f"postmix_{i}") for i in range(5)]
            + [make_output(d // 3 + i * 1001, f"change_{i}") for i in range(5)]
            + [make_output(self.FEE, "coordinator")]
        )
        inputs = [make_input(d * 2, f"inp_{i}") for i in range(5)]
        result = _wasabi_10_heuristic(make_tx(inputs, outputs))
        assert result is not None
        assert result.denominations == [d]

    def test_multiple_inputs_per_participant(self):
        """Participants consolidating 2 UTXOs each — still within a_max."""
        result = _wasabi_10_heuristic(self._make_wasabi10(5, inputs_per_participant=2))
        assert result is not None
        assert result.n_participants == 5

    def test_op_return_ignored(self):
        """OP_RETURN in outputs must not break detection."""
        tx = self._make_wasabi10(5)
        tx["outputs"].append(make_op_return())
        result = _wasabi_10_heuristic(tx)
        assert result is not None
        assert result.n_participants == 5


# ---------------------------------------------------------------------------
# Wasabi 1.0 — rejection cases
# ---------------------------------------------------------------------------


class TestWasabi10Rejection:
    D = WASABI_10_DENOM_SAT
    FEE = 50_000

    def _make_wasabi10(self, n, n_change=None, fee=None):
        if n_change is None:
            n_change = n
        fee = fee or self.FEE
        outputs = (
            [make_output(self.D, f"postmix_{i}") for i in range(n)]
            + [
                make_output(self.D // 3 + i * 1001, f"change_{i}")
                for i in range(n_change)
            ]
            + [make_output(fee, "coordinator")]
        )
        inputs = [make_input(self.D * 2, f"inp_{i}") for i in range(n)]
        return make_tx(inputs, outputs)

    def test_coinbase_rejected(self):
        assert _wasabi_10_heuristic(make_tx([], [], coinbase=True)) is None

    def test_denomination_too_low(self):
        """Post-mix value below d - epsilon."""
        d = WASABI_10_DENOM_SAT - WASABI_10_EPSILON_SAT - 1
        outputs = (
            [make_output(d, f"postmix_{i}") for i in range(5)]
            + [make_output(100_000 + i * 1001, f"change_{i}") for i in range(5)]
            + [make_output(self.FEE, "coordinator")]
        )
        inputs = [make_input(d * 2, f"inp_{i}") for i in range(5)]
        assert _wasabi_10_heuristic(make_tx(inputs, outputs)) is None

    def test_denomination_too_high(self):
        """Post-mix value above d + epsilon."""
        d = WASABI_10_DENOM_SAT + WASABI_10_EPSILON_SAT + 1
        outputs = (
            [make_output(d, f"postmix_{i}") for i in range(5)]
            + [make_output(100_000 + i * 1001, f"change_{i}") for i in range(5)]
            + [make_output(self.FEE, "coordinator")]
        )
        inputs = [make_input(d * 2, f"inp_{i}") for i in range(5)]
        assert _wasabi_10_heuristic(make_tx(inputs, outputs)) is None

    def test_too_few_postmix_outputs(self):
        """n is too small relative to total outputs — fails participant lower bound."""
        # 2 post-mix but 10 other outputs → n < (|outputs|-1)/2
        outputs = (
            [make_output(self.D, f"postmix_{i}") for i in range(2)]
            + [make_output(100_000 + i * 1001, f"other_{i}") for i in range(10)]
            + [make_output(self.FEE, "coordinator")]
        )
        inputs = [make_input(self.D * 2, f"inp_{i}") for i in range(5)]
        assert _wasabi_10_heuristic(make_tx(inputs, outputs)) is None

    def test_too_few_inputs(self):
        """Fewer distinct input scripts than participants."""
        outputs = (
            [make_output(self.D, f"postmix_{i}") for i in range(5)]
            + [make_output(100_000 + i * 1001, f"change_{i}") for i in range(5)]
            + [make_output(self.FEE, "coordinator")]
        )
        # only 2 distinct input addresses for 5 participants
        inputs = [make_input(self.D * 2, f"inp_{i % 2}") for i in range(5)]
        assert _wasabi_10_heuristic(make_tx(inputs, outputs)) is None

    def test_too_many_inputs(self):
        """Input count exceeds a_max × n."""
        n = 3
        outputs = (
            [make_output(self.D, f"postmix_{i}") for i in range(n)]
            + [make_output(100_000 + i * 1001, f"change_{i}") for i in range(n)]
            + [make_output(self.FEE, "coordinator")]
        )
        # WASABI_10_A_MAX * n + 1 distinct inputs
        inputs = [
            make_input(self.D * 2, f"inp_{i}") for i in range(WASABI_10_A_MAX * n + 1)
        ]
        assert _wasabi_10_heuristic(make_tx(inputs, outputs)) is None

    def test_duplicate_output_script_rejected(self):
        """Reused output address violates distinct-scripts condition."""
        outputs = (
            [make_output(self.D, "postmix_0")]
            + [make_output(self.D, "postmix_0")]  # duplicate
            + [make_output(self.D, f"postmix_{i}") for i in range(2, 5)]
            + [make_output(100_000 + i * 1001, f"change_{i}") for i in range(5)]
            + [make_output(self.FEE, "coordinator")]
        )
        inputs = [make_input(self.D * 2, f"inp_{i}") for i in range(5)]
        assert _wasabi_10_heuristic(make_tx(inputs, outputs)) is None

    def test_no_outputs_in_denomination_window(self):
        """No outputs near 0.1 BTC at all."""
        outputs = [make_output(500_000 + i * 1001, f"out_{i}") for i in range(10)] + [
            make_output(self.FEE, "coordinator")
        ]
        inputs = [make_input(1_000_000, f"inp_{i}") for i in range(5)]
        assert _wasabi_10_heuristic(make_tx(inputs, outputs)) is None


# ---------------------------------------------------------------------------
# Wasabi 1.1 — happy path
# ---------------------------------------------------------------------------


class TestWasabi11HappyPath:
    D = WASABI_10_DENOM_SAT  # 10_000_000 sat base denomination
    FEE = 50_000

    def _make_wasabi11(self, level_counts: dict[int, int], n_change=None, fee=None):
        """
        Build a valid Wasabi 1.1 tx.
        level_counts: {level_index: n_outputs_at_that_level}
        """
        fee = fee or self.FEE
        outputs = []
        addr_idx = 0
        for level, count in level_counts.items():
            level_d = (2**level) * self.D
            for _ in range(count):
                outputs.append(make_output(level_d, f"postmix_{addr_idx}"))
                addr_idx += 1

        n_participants = max(level_counts.values())
        if n_change is None:
            n_change = n_participants
        for i in range(n_change):
            outputs.append(make_output(self.D // 3 + i * 1001, f"change_{i}"))

        outputs.append(make_output(fee, "coordinator"))
        inputs = [make_input(self.D * 4, f"inp_{i}") for i in range(n_participants)]
        return make_tx(inputs, outputs)

    def test_two_levels(self):
        """Basic 1.1: level 0 and level 1 both active."""
        result = _wasabi_11_heuristic(self._make_wasabi11({0: 3, 1: 3}))
        assert result is not None
        assert result.detected is True
        assert result.version == "1.1"
        assert result.n_participants == 3
        assert set(result.denominations) == {self.D, 2 * self.D}

    def test_three_levels(self):
        """Levels 0, 1, 2 all active."""
        result = _wasabi_11_heuristic(self._make_wasabi11({0: 2, 1: 3, 2: 4}))
        assert result is not None
        assert result.version == "1.1"
        assert result.n_participants == 4
        assert set(result.denominations) == {self.D, 2 * self.D, 4 * self.D}

    def test_level_1_only_classified_as_11(self):
        """Single active level at 2d — not level 0, so must be 1.1 not 1.0."""
        result = _wasabi_11_heuristic(self._make_wasabi11({1: 3}))
        assert result is not None
        assert result.version == "1.1"
        assert result.denominations == [2 * self.D]

    def test_level_0_only_classified_as_10(self):
        """Single active level at d — indistinguishable from 1.0, version must be 1.0."""
        result = _wasabi_11_heuristic(self._make_wasabi11({0: 4}))
        assert result is not None
        assert result.version == "1.0"

    def test_confidence_in_valid_range(self):
        """Confidence must be in [0, 100]."""
        result = _wasabi_11_heuristic(self._make_wasabi11({0: 3, 1: 3}))
        assert 0 <= result.confidence <= 100

    def test_confidence_floor_at_worst_case_inputs(self):
        """Worst case: every participant uses a_max inputs — confidence must still be ≥ 50."""
        n = 4
        outputs = (
            [make_output(self.D, f"postmix_{i}") for i in range(n)]
            + [make_output(2 * self.D, f"postmix2_{i}") for i in range(n)]
            + [make_output(self.D // 3 + i * 1001, f"change_{i}") for i in range(n)]
            + [make_output(self.FEE, "coordinator")]
        )
        inputs = [
            make_input(self.D * 10, f"inp_{i}") for i in range(WASABI_11_A_MAX * n)
        ]
        result = _wasabi_11_heuristic(make_tx(inputs, outputs))
        assert result is not None
        assert result.confidence >= 50

    def test_confidence_high_with_one_input_per_participant(self):
        """Best case: one input per participant — confidence must be 100."""
        result = _wasabi_11_heuristic(self._make_wasabi11({0: 4, 1: 4}))
        assert result.confidence == 100

    def test_no_change_outputs(self):
        """Valid 1.1 tx where no participants have change."""
        result = _wasabi_11_heuristic(self._make_wasabi11({0: 3, 1: 3}, n_change=0))
        assert result is not None
        assert result.version == "1.1"

    def test_op_return_ignored(self):
        """OP_RETURN must not affect level detection."""
        tx = self._make_wasabi11({0: 3, 1: 3})
        tx["outputs"].append(make_op_return())
        result = _wasabi_11_heuristic(tx)
        assert result is not None
        assert result.version == "1.1"


# ---------------------------------------------------------------------------
# Wasabi 1.1 — rejection cases
# ---------------------------------------------------------------------------


class TestWasabi11Rejection:
    D = WASABI_10_DENOM_SAT
    FEE = 50_000

    def _make_wasabi11(self, level_counts, n_change=None):
        fee = self.FEE
        outputs = []
        addr_idx = 0
        for level, count in level_counts.items():
            level_d = (2**level) * self.D
            for _ in range(count):
                outputs.append(make_output(level_d, f"postmix_{addr_idx}"))
                addr_idx += 1
        n_participants = max(level_counts.values())
        if n_change is None:
            n_change = n_participants
        for i in range(n_change):
            outputs.append(make_output(self.D // 3 + i * 1001, f"change_{i}"))
        outputs.append(make_output(fee, "coordinator"))
        inputs = [make_input(self.D * 4, f"inp_{i}") for i in range(n_participants)]
        return make_tx(inputs, outputs)

    def test_coinbase_rejected(self):
        assert _wasabi_11_heuristic(make_tx([], [], coinbase=True)) is None

    def test_no_outputs_in_denomination_window(self):
        outputs = [make_output(500_000 + i * 999, f"out_{i}") for i in range(10)]
        inputs = [make_input(1_000_000, f"inp_{i}") for i in range(5)]
        assert _wasabi_11_heuristic(make_tx(inputs, outputs)) is None

    def test_all_levels_have_only_one_output(self):
        """Every level has only 1 output — none qualify (need ≥2 per level)."""
        outputs = (
            [make_output(self.D, "postmix_0")]
            + [make_output(2 * self.D, "postmix_1")]
            + [make_output(4 * self.D, "postmix_2")]
            + [make_output(self.D // 3, f"change_{i}") for i in range(3)]
            + [make_output(self.FEE, "coordinator")]
        )
        inputs = [make_input(self.D * 4, f"inp_{i}") for i in range(3)]
        assert _wasabi_11_heuristic(make_tx(inputs, outputs)) is None

    def test_too_few_inputs(self):
        """Fewer distinct inputs than estimated n."""
        tx = self._make_wasabi11({0: 5, 1: 5})
        # collapse all inputs to 2 distinct addresses
        tx["inputs"] = [make_input(self.D * 4, f"inp_{i % 2}") for i in range(6)]
        assert _wasabi_11_heuristic(tx) is None

    def test_too_many_inputs(self):
        """Inputs exceed a_max × n."""
        n = 3
        outputs = (
            [make_output(self.D, f"postmix_{i}") for i in range(n)]
            + [make_output(2 * self.D, f"postmix2_{i}") for i in range(n)]
            + [make_output(self.D // 3 + i * 1001, f"change_{i}") for i in range(n)]
            + [make_output(self.FEE, "coordinator")]
        )
        inputs = [
            make_input(self.D * 4, f"inp_{i}") for i in range(WASABI_11_A_MAX * n + 1)
        ]
        assert _wasabi_11_heuristic(make_tx(inputs, outputs)) is None

    def test_duplicate_output_script_rejected(self):
        tx = self._make_wasabi11({0: 3, 1: 3})
        # duplicate one output address
        tx["outputs"][0] = make_output(
            tx["outputs"][0].value, tx["outputs"][1].address[0]
        )
        assert _wasabi_11_heuristic(tx) is None

    def test_sum_condition_fails(self):
        """Too many non-postmix outputs so total_postmix < |outputs| - n - 1."""
        outputs = (
            [make_output(self.D, f"postmix_{i}") for i in range(2)]
            + [make_output(2 * self.D, f"postmix2_{i}") for i in range(2)]
            + [make_output(self.D // 4 + i * 997, f"junk_{i}") for i in range(30)]
            + [make_output(self.FEE, "coordinator")]
        )
        inputs = [make_input(self.D * 4, f"inp_{i}") for i in range(4)]
        assert _wasabi_11_heuristic(make_tx(inputs, outputs)) is None


# ---------------------------------------------------------------------------
# JoinMarket — happy path
# ---------------------------------------------------------------------------


class TestJoinMarketHappyPath:
    D = 5_000_000  # arbitrary denomination — JoinMarket has no fixed denom

    def _make_joinmarket(self, n, n_change=None, denom=None):
        """n participants, each with one post-mix output, optionally one change."""
        d = denom or self.D
        if n_change is None:
            n_change = n
        outputs = [make_output(d, f"postmix_{i}") for i in range(n)] + [
            make_output(d // 3 + i * 1001, f"change_{i}") for i in range(n_change)
        ]
        inputs = [make_input(d * 2, f"inp_{i}") for i in range(n)]
        return make_tx(inputs, outputs)

    def test_minimal_valid(self):
        """Minimum 2 participants — detected with low confidence."""
        result = _joinmarket_heuristic(self._make_joinmarket(2))
        assert result is not None
        assert result.detected is True
        assert result.n_participants == 2
        assert result.pool_denomination == self.D
        assert result.confidence == 20

    def test_three_participants(self):
        """3 participants — detected with normal confidence."""
        result = _joinmarket_heuristic(self._make_joinmarket(3))
        assert result is not None
        assert result.detected is True
        assert result.n_participants == 3
        assert result.pool_denomination == self.D
        assert result.confidence == 49

    def test_typical_round(self):
        """Typical round with 8 participants."""
        result = _joinmarket_heuristic(self._make_joinmarket(8))
        assert result is not None
        assert result.n_participants == 8

    def test_no_change_outputs(self):
        """All participants without change — still valid."""
        result = _joinmarket_heuristic(self._make_joinmarket(5, n_change=0))
        assert result is not None
        assert result.n_participants == 5

    def test_arbitrary_denomination(self):
        """JoinMarket has no fixed denomination — any value should work."""
        for d in [100_000, 1_234_567, 50_000_000]:
            result = _joinmarket_heuristic(self._make_joinmarket(5, denom=d))
            assert result is not None, f"Failed for denomination {d}"
            assert result.pool_denomination == d

    def test_wasabi_coordinator_fee_breaks_joinmarket(self):
        """A Wasabi 1.0 tx with coordinator fee fails JoinMarket — the extra output
        pushes n below |outputs|/2. JoinMarket has no coordinator fee by design."""
        outputs = (
            [make_output(10_000_000, f"postmix_{i}") for i in range(5)]
            + [make_output(3_000_000 + i * 1001, f"change_{i}") for i in range(5)]
            + [
                make_output(50_000, "coordinator")
            ]  # this extra output breaks the condition
        )
        inputs = [make_input(20_000_000, f"inp_{i}") for i in range(5)]
        assert _joinmarket_heuristic(make_tx(inputs, outputs)) is None

    def test_confidence_below_wasabi(self):
        """JoinMarket confidence must be lower than Wasabi minimum (50)."""
        result = _joinmarket_heuristic(self._make_joinmarket(5))
        assert result.confidence < 50

    def test_op_return_ignored(self):
        """OP_RETURN in outputs must not affect detection."""
        tx = self._make_joinmarket(5)
        tx["outputs"].append(make_op_return())
        result = _joinmarket_heuristic(tx)
        assert result is not None


# ---------------------------------------------------------------------------
# JoinMarket — rejection cases
# ---------------------------------------------------------------------------


class TestJoinMarketRejection:
    D = 5_000_000

    def _make_joinmarket(self, n, n_change=None):
        if n_change is None:
            n_change = n
        outputs = [make_output(self.D, f"postmix_{i}") for i in range(n)] + [
            make_output(self.D // 3 + i * 1001, f"change_{i}") for i in range(n_change)
        ]
        inputs = [make_input(self.D * 2, f"inp_{i}") for i in range(n)]
        return make_tx(inputs, outputs)

    def test_coinbase_rejected(self):
        assert _joinmarket_heuristic(make_tx([], [], coinbase=True)) is None

    def test_fewer_than_2_participants_rejected(self):
        """n=1 fails the minimum participant check."""
        result = _joinmarket_heuristic(self._make_joinmarket(1))
        assert result is None

    def test_postmix_not_majority_rejected(self):
        """n < |outputs| / 2 — too many non-postmix outputs."""
        outputs = [make_output(self.D, f"postmix_{i}") for i in range(3)] + [
            make_output(self.D // 3 + i * 1001, f"change_{i}") for i in range(10)
        ]
        inputs = [make_input(self.D * 2, f"inp_{i}") for i in range(3)]
        assert _joinmarket_heuristic(make_tx(inputs, outputs)) is None

    def test_too_few_distinct_inputs_rejected(self):
        """n > n_scripts_in — not enough distinct input addresses."""
        outputs = [make_output(self.D, f"postmix_{i}") for i in range(5)] + [
            make_output(self.D // 3 + i * 1001, f"change_{i}") for i in range(5)
        ]
        inputs = [make_input(self.D * 2, f"inp_{i % 2}") for i in range(5)]
        assert _joinmarket_heuristic(make_tx(inputs, outputs)) is None

    def test_duplicate_output_script_rejected(self):
        tx = self._make_joinmarket(5)
        tx["outputs"][0] = make_output(
            tx["outputs"][0].value, tx["outputs"][1].address[0]
        )
        assert _joinmarket_heuristic(tx) is None

    def test_all_outputs_dust_rejected(self):
        """All outputs below dust threshold — no valid denomination candidate."""
        outputs = [make_output(500, f"out_{i}") for i in range(6)]
        inputs = [make_input(5000, f"inp_{i}") for i in range(3)]
        assert _joinmarket_heuristic(make_tx(inputs, outputs)) is None

    def test_dust_spam_not_misclassified(self):
        """A dust attack tx sending the same small amount to many addresses
        must not be classified as JoinMarket — dust is excluded as denomination."""
        outputs = [
            make_output(JOINMARKET_DUST_THRESHOLD, f"victim_{i}") for i in range(20)
        ]
        inputs = [make_input(100_000, "attacker")]
        assert _joinmarket_heuristic(make_tx(inputs, outputs)) is None


# ---------------------------------------------------------------------------
# Wasabi 2.0 — happy path
# ---------------------------------------------------------------------------


class TestWasabi20HappyPath:
    """Wasabi 2.0 (WabiSabi): large rounds with variable denomination sets."""

    def _make_wasabi20(self, denoms: dict[int, int], n_change=10, n_inputs=30):
        """
        Build a valid Wasabi 2.0 tx.
        denoms: {denomination_value: count} — the denomination set with frequencies
        n_change: number of non-denomination outputs (change)
        n_inputs: total input count (must be >= 20)
        """
        outputs = []
        addr_idx = 0
        for value, count in denoms.items():
            for _ in range(count):
                outputs.append(make_output(value, f"denom_{addr_idx}"))
                addr_idx += 1
        for i in range(n_change):
            outputs.append(make_output(50_000 + i * 1337, f"change_{i}"))
        outputs.append(make_output(100_000, "coordinator"))
        inputs = [make_input(1_000_000, f"inp_{i}") for i in range(n_inputs)]
        return make_tx(inputs, outputs)

    def test_minimal_valid(self):
        """20 inputs, three denomination tiers with enough outputs."""
        # 30 denom outputs + 10 change + 1 coordinator = 41 outputs
        # denom majority: 30 >= (41-1)/2 = 20 ✓
        # denom vs inputs: 30 >= 20/10 = 2 ✓
        denoms = {100_000: 10, 200_000: 10, 400_000: 10}
        result = _wasabi_20_heuristic(self._make_wasabi20(denoms, n_inputs=20))
        assert result is not None
        assert result.detected is True
        assert result.version == "2.0"
        assert set(result.denominations) == {100_000, 200_000, 400_000}

    def test_multiple_denominations(self):
        """Multiple denomination values in set D."""
        denoms = {100_000: 10, 200_000: 8, 500_000: 6}
        result = _wasabi_20_heuristic(self._make_wasabi20(denoms, n_inputs=60))
        assert result is not None
        assert result.version == "2.0"
        assert set(result.denominations) == {100_000, 200_000, 500_000}

    def test_large_round(self):
        """100 inputs, typical large WabiSabi round."""
        denoms = {100_000: 30, 200_000: 20, 500_000: 10}
        result = _wasabi_20_heuristic(
            self._make_wasabi20(denoms, n_inputs=100, n_change=15)
        )
        assert result is not None
        assert result.n_participants >= 1

    def test_exactly_20_inputs(self):
        """Boundary: exactly 20 inputs should pass."""
        denoms = {100_000: 10, 200_000: 10, 400_000: 10}
        result = _wasabi_20_heuristic(self._make_wasabi20(denoms, n_inputs=20))
        assert result is not None

    def test_no_change_outputs(self):
        """All outputs are denominations — no change (coordinator is the non-denom)."""
        denoms = {150_000: 15, 250_000: 15, 400_000: 10}
        result = _wasabi_20_heuristic(
            self._make_wasabi20(denoms, n_change=0, n_inputs=50)
        )
        assert result is not None

    def test_op_return_ignored(self):
        """OP_RETURN outputs should be filtered out, not cause crashes."""
        denoms = {100_000: 10, 200_000: 10, 400_000: 10}
        tx = self._make_wasabi20(denoms, n_inputs=50)
        tx["outputs"].append(make_op_return())
        result = _wasabi_20_heuristic(tx)
        assert result is not None

    def test_denominations_not_near_01_btc(self):
        """Wasabi 2.0 has no fixed denomination — values far from 0.1 BTC should work."""
        denoms = {50_000: 10, 75_000: 10, 100_000: 10}
        result = _wasabi_20_heuristic(
            self._make_wasabi20(denoms, n_change=5, n_inputs=55)
        )
        assert result is not None
        assert result.version == "2.0"

    def test_confidence_is_set(self):
        """Confidence should be a reasonable value."""
        denoms = {100_000: 10, 200_000: 10, 400_000: 10}
        result = _wasabi_20_heuristic(self._make_wasabi20(denoms, n_inputs=50))
        assert result.confidence > 0
        assert result.confidence <= 100


# ---------------------------------------------------------------------------
# Wasabi 2.0 — rejection cases
# ---------------------------------------------------------------------------


class TestWasabi20Rejection:
    def test_coinbase_rejected(self):
        assert _wasabi_20_heuristic(make_tx([], [], coinbase=True)) is None

    def test_fewer_than_20_inputs(self):
        """19 inputs — below minimum, must reject."""
        outputs = [make_output(200_000, f"denom_{i}") for i in range(30)] + [
            make_output(50_000 + i * 1001, f"change_{i}") for i in range(10)
        ]
        inputs = [make_input(1_000_000, f"inp_{i}") for i in range(19)]
        assert _wasabi_20_heuristic(make_tx(inputs, outputs)) is None

    def test_output_below_v_min(self):
        """Any output below 5000 sat should reject."""
        outputs = (
            [make_output(200_000, f"denom_{i}") for i in range(30)]
            + [make_output(4_999, "tiny_output")]  # below v_min
            + [make_output(50_000 + i * 1001, f"change_{i}") for i in range(10)]
        )
        inputs = [make_input(1_000_000, f"inp_{i}") for i in range(50)]
        assert _wasabi_20_heuristic(make_tx(inputs, outputs)) is None

    def test_no_repeated_output_values(self):
        """If every output has a unique value, no denomination set can be derived."""
        outputs = [make_output(100_000 + i * 1337, f"out_{i}") for i in range(60)]
        inputs = [make_input(1_000_000, f"inp_{i}") for i in range(50)]
        assert _wasabi_20_heuristic(make_tx(inputs, outputs)) is None

    def test_denom_outputs_not_majority(self):
        """Denomination outputs are less than half — too much change."""
        # 5 denom + 40 change + 1 coord = 46 outputs
        # denom majority: 5 >= (46-1)/2 = 22.5 → fails
        outputs = (
            [make_output(200_000, f"denom_{i}") for i in range(5)]
            + [make_output(50_000 + i * 1001, f"change_{i}") for i in range(40)]
            + [make_output(100_000, "coordinator")]
        )
        inputs = [make_input(1_000_000, f"inp_{i}") for i in range(50)]
        assert _wasabi_20_heuristic(make_tx(inputs, outputs)) is None

    def test_too_few_denom_outputs_vs_inputs(self):
        """Denomination count < inputs / a_max."""
        # 4 denom outputs, 50 inputs → 4 < 50/10 = 5 → fails
        outputs = (
            [make_output(200_000, f"denom_{i}") for i in range(4)]
            + [make_output(50_000 + i * 1001, f"change_{i}") for i in range(3)]
            + [make_output(100_000, "coordinator")]
        )
        inputs = [make_input(1_000_000, f"inp_{i}") for i in range(50)]
        assert _wasabi_20_heuristic(make_tx(inputs, outputs)) is None

    def test_duplicate_output_script_rejected(self):
        """Two outputs sharing the same address should reject."""
        outputs = (
            [make_output(200_000, f"denom_{i}") for i in range(30)]
            + [make_output(50_000 + i * 1001, f"change_{i}") for i in range(10)]
            + [make_output(100_000, "coordinator")]
        )
        # duplicate one address
        outputs[1] = make_output(200_000, outputs[0].address[0])
        inputs = [make_input(1_000_000, f"inp_{i}") for i in range(50)]
        assert _wasabi_20_heuristic(make_tx(inputs, outputs)) is None

    def test_empty_tx(self):
        assert _wasabi_20_heuristic(make_tx([], [])) is None


# ---------------------------------------------------------------------------
# False positive scenarios — cross-protocol
# ---------------------------------------------------------------------------


class TestFalsePositiveScenarios:
    """Transactions that superficially resemble CoinJoin but are not."""

    def test_batch_payout_detected_as_joinmarket(self):
        """An exchange paying 10 users exactly 0.5 BTC each with 10 distinct
        hot-wallet UTXOs as inputs. Structurally indistinguishable from
        JoinMarket — this is a known limitation. We document that it passes."""
        d = 50_000_000  # 0.5 BTC
        outputs = [make_output(d, f"user_{i}") for i in range(10)]
        inputs = [make_input(d * 2, f"hotwallet_utxo_{i}") for i in range(10)]
        result = _joinmarket_heuristic(make_tx(inputs, outputs))
        # known false positive — structurally identical to JoinMarket
        assert result is not None

    def test_batch_payout_with_change_not_joinmarket(self):
        """Same batch payout but with change outputs — now n < |outputs|/2
        so JoinMarket rejects it."""
        d = 50_000_000
        outputs = [make_output(d, f"user_{i}") for i in range(5)] + [
            make_output(d // 3 + i * 1001, f"change_{i}") for i in range(6)
        ]
        inputs = [make_input(d * 2, f"hotwallet_utxo_{i}") for i in range(5)]
        assert _joinmarket_heuristic(make_tx(inputs, outputs)) is None

    def test_omni_tx_not_whirlpool_tx0(self):
        """Omni Layer tx with OP_RETURN and outputs that happen to be near
        the 0.001 BTC pool. Should not be classified as Whirlpool Tx0."""
        _, _ = 100_000, 5_000  # smallest Whirlpool pool
        outputs = [
            make_output(
                80_000, "omni_recipient_1"
            ),  # near d but not in [d+emin, d+emax]
            make_output(80_000, "omni_recipient_2"),
            make_output(20_000, "omni_fee"),
            make_op_return(),  # Omni protocol data
        ]
        inputs = [make_input(200_000, "omni_sender")]
        assert _whirlpool_tx0_heuristic(make_tx(inputs, outputs)) is None

    def test_omni_tx_with_premix_like_values_not_tx0(self):
        """Omni tx where outputs are in the premix range but no valid
        coordinator fee output exists."""
        d, _ = 1_000_000, 50_000
        eps = 5_000
        outputs = [
            make_output(d + eps, "recipient_1"),
            make_output(d + eps, "recipient_2"),
            make_output(d + eps, "recipient_3"),
            make_op_return(),
        ]
        inputs = [make_input(5_000_000, "sender")]
        assert _whirlpool_tx0_heuristic(make_tx(inputs, outputs)) is None

    def test_exchange_withdrawal_not_wasabi_10(self):
        """Exchange sends multiple users ~0.1 BTC each. Without a coordinator
        fee output, the participant bound (|outputs|-1)/2 makes this harder
        to misclassify, but with enough outputs it could pass."""
        d = WASABI_10_DENOM_SAT
        # 4 identical outputs + 3 change-like outputs = 7 outputs
        # n=4, (7-1)/2=3, 4>=3 ✓ — but n_scripts_in must be ≥ n
        outputs = [make_output(d, f"user_{i}") for i in range(4)] + [
            make_output(d // 3 + i * 1001, f"change_{i}") for i in range(3)
        ]
        # only 2 distinct input addresses — fails n <= n_scripts_in
        inputs = [make_input(d * 5, f"exchange_{i % 2}") for i in range(4)]
        assert _wasabi_10_heuristic(make_tx(inputs, outputs)) is None

    def test_wasabi_11_many_inputs_few_level_outputs(self):
        """A tx with 20 distinct inputs where only 4 outputs land near level
        windows. Rejected by the input bounds check: n_scripts_in=20 > 7*n=14."""
        d = WASABI_10_DENOM_SAT
        outputs = (
            [make_output(d, f"level0_{i}") for i in range(2)]
            + [make_output(2 * d, f"level1_{i}") for i in range(2)]
            + [make_output(d // 4 + i * 997, f"junk_{i}") for i in range(21)]
        )
        inputs = [make_input(d * 10, f"inp_{i}") for i in range(20)]
        assert _wasabi_11_heuristic(make_tx(inputs, outputs)) is None
