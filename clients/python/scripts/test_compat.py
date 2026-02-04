#!/usr/bin/env python3
"""
Comprehensive backward compatibility tests for OpenAPI v7 generated client.

This script tests that existing user code patterns continue to work after
migrating to the v7 generated client with compatibility patches.

Key patterns tested:
1. ModelSimple .value patterns (tx.height.value, tx.inputs.value)
2. Union type transparency (address_tx.tx_hash direct access)
3. Arithmetic operations (tx.height + 1)
4. Iteration patterns (for item in tx.inputs.value)
5. Nested access (tx.inputs.value[0].address[0])
"""

import sys
from pathlib import Path

# Add the client directory to path for imports
client_dir = Path(__file__).parent.parent
sys.path.insert(0, str(client_dir))


def test_compat_int():
    """Test CompatInt backward compatibility."""
    print("Testing CompatInt...")

    from graphsense.compat import CompatInt

    # Basic creation and .value access
    h = CompatInt(12345)
    assert h.value == 12345, f"Expected 12345, got {h.value}"

    # Arithmetic operations
    assert h + 1 == 12346, f"Expected 12346, got {h + 1}"
    assert h - 1 == 12344, f"Expected 12344, got {h - 1}"
    assert h * 2 == 24690, f"Expected 24690, got {h * 2}"
    assert h // 2 == 6172, f"Expected 6172, got {h // 2}"

    # Type checking
    assert isinstance(h, int), "CompatInt should be instance of int"
    assert isinstance(h.value, int), "CompatInt.value should be instance of int"

    # Comparison
    assert h == 12345, "CompatInt should equal int"
    assert h > 12344, "CompatInt comparison should work"
    assert h < 12346, "CompatInt comparison should work"

    # String representation
    assert str(h) == "12345", f"Expected '12345', got '{str(h)}'"

    print("  CompatInt: PASSED")


def test_compat_list():
    """Test CompatList backward compatibility."""
    print("Testing CompatList...")

    from graphsense.compat import CompatList

    # Basic creation and .value access
    items = CompatList([1, 2, 3])
    assert items.value == [1, 2, 3], f"Expected [1, 2, 3], got {items.value}"

    # Indexing through .value
    assert items.value[0] == 1, f"Expected 1, got {items.value[0]}"
    assert items.value[-1] == 3, f"Expected 3, got {items.value[-1]}"

    # Length through .value
    assert len(items.value) == 3, f"Expected 3, got {len(items.value)}"

    # Direct length
    assert len(items) == 3, f"Expected 3, got {len(items)}"

    # Iteration through .value
    collected = []
    for item in items.value:
        collected.append(item)
    assert collected == [1, 2, 3], f"Expected [1, 2, 3], got {collected}"

    # Direct iteration
    collected = []
    for item in items:
        collected.append(item)
    assert collected == [1, 2, 3], f"Expected [1, 2, 3], got {collected}"

    # Type checking
    assert isinstance(items, list), "CompatList should be instance of list"
    assert isinstance(items.value, list), "CompatList.value should be instance of list"

    # Slicing
    assert items.value[1:] == [2, 3], f"Expected [2, 3], got {items.value[1:]}"

    print("  CompatList: PASSED")


def test_compat_list_with_objects():
    """Test CompatList with object items."""
    print("Testing CompatList with objects...")

    from graphsense.compat import CompatList

    class MockTxInput:
        def __init__(self, address, value):
            self.address = address
            self.value = value

    inputs = CompatList(
        [
            MockTxInput(address=["addr1"], value=100),
            MockTxInput(address=["addr2"], value=200),
        ]
    )

    # Access through .value
    assert len(inputs.value) == 2
    assert inputs.value[0].address[0] == "addr1"
    assert inputs.value[1].value == 200

    # Iteration through .value
    total = 0
    for inp in inputs.value:
        total += inp.value
    assert total == 300, f"Expected 300, got {total}"

    print("  CompatList with objects: PASSED")


def test_nested_value_patterns():
    """Test nested .value access patterns that users commonly use."""
    print("Testing nested .value patterns...")

    from graphsense.compat import CompatInt, CompatList

    class MockValues:
        """Mock for Values type"""

        def __init__(self, value):
            self._value = value

        @property
        def value(self):
            return self._value

    class MockTxInput:
        """Mock for TxInput type"""

        def __init__(self, address, value, index):
            self.address = CompatList(address)  # Was list
            self.value = MockValues(value)  # Has .value property
            self.index = index

    class MockTxOutput:
        """Mock for TxOutput type"""

        def __init__(self, address, value):
            self.address = CompatList(address)
            self.value = MockValues(value)

    class MockTx:
        """Mock for Tx type"""

        def __init__(self):
            self.height = CompatInt(12345)
            self.inputs = CompatList(
                [
                    MockTxInput(address=["addr1", "addr2"], value=100, index=0),
                    MockTxInput(address=["addr3"], value=200, index=1),
                ]
            )
            self.outputs = CompatList(
                [
                    MockTxOutput(address=["addr4"], value=300),
                ]
            )

    tx = MockTx()

    # Pattern: tx.height.value
    assert tx.height.value == 12345
    print("  tx.height.value: OK")

    # Pattern: tx.inputs.value
    assert len(tx.inputs.value) == 2
    print("  tx.inputs.value (len): OK")

    # Pattern: for x in tx.inputs.value
    count = 0
    for x in tx.inputs.value:
        count += 1
        assert hasattr(x, "address")
    assert count == 2
    print("  for x in tx.inputs.value: OK")

    # Pattern: tx.inputs.value[0].address[0]
    assert tx.inputs.value[0].address[0] == "addr1"
    print("  tx.inputs.value[0].address[0]: OK")

    # Pattern: tx.inputs.value[0].address.value[0]  (double .value)
    assert tx.inputs.value[0].address.value[0] == "addr1"
    print("  tx.inputs.value[0].address.value[0]: OK")

    # Pattern: tx.inputs.value[0].value.value (Values object)
    assert tx.inputs.value[0].value.value == 100
    print("  tx.inputs.value[0].value.value: OK")

    # Pattern: tx.outputs.value[0].value.value
    assert tx.outputs.value[0].value.value == 300
    print("  tx.outputs.value[0].value.value: OK")

    print("  Nested .value patterns: PASSED")


def test_user_code_simulation():
    """Simulate actual user code patterns from QuickLockUTXOGsApiDataProvider."""
    print("Testing user code simulation...")

    from graphsense.compat import CompatInt, CompatList
    from datetime import datetime

    class MockValues:
        def __init__(self, value, fiat_values=None):
            self._value = value
            self.fiat_values = fiat_values or []

        @property
        def value(self):
            return self._value

    class MockTxInput:
        def __init__(self, address, value, index):
            self.address = CompatList(address)
            self.value = MockValues(value)
            self.index = index

    class MockAddressTx:
        """Simulating AddressTx (union type)"""

        def __init__(self, tx_hash, height, timestamp, coinbase, value):
            self.tx_hash = tx_hash
            self.height = CompatInt(height)
            self.timestamp = timestamp
            self.coinbase = coinbase
            self.value = MockValues(value)
            self.inputs = CompatList(
                [
                    MockTxInput(address=["1ABC..."], value=1000, index=0),
                    MockTxInput(address=["1DEF..."], value=2000, index=1),
                ]
            )

    # Simulating user code pattern:
    # QuickLockAddressTx(
    #     tx_hash=tx.tx_hash,
    #     block=tx.height.value,  # <- This pattern!
    #     timestamp=datetime.fromtimestamp(tx.timestamp),
    #     coinbase=tx.coinbase,
    #     value=Values.from_gs(tx.value),
    # )

    tx = MockAddressTx(
        tx_hash="abc123",
        height=123456,
        timestamp=1609459200,
        coinbase=False,
        value=5000,
    )

    # User code pattern
    result = {
        "tx_hash": tx.tx_hash,
        "block": tx.height.value,  # <-- Key pattern
        "timestamp": datetime.fromtimestamp(tx.timestamp),
        "coinbase": tx.coinbase,
        "value": tx.value.value,
    }

    assert result["tx_hash"] == "abc123"
    assert result["block"] == 123456
    assert result["coinbase"] is False
    assert result["value"] == 5000
    print("  QuickLockAddressTx pattern: OK")

    # User code pattern for inputs:
    # for x in tx.inputs.value:
    #     QuickLockIOValue(
    #         index=x.index,
    #         address=x.address[0],
    #         value=Values.from_gs(x.value)
    #     )

    io_values = []
    for x in tx.inputs.value:  # <-- Key pattern
        io_values.append(
            {
                "index": x.index,
                "address": x.address[0],  # <-- Key pattern (or x.address.value[0])
                "value": x.value.value,
            }
        )

    assert len(io_values) == 2
    assert io_values[0]["index"] == 0
    assert io_values[0]["address"] == "1ABC..."
    assert io_values[0]["value"] == 1000
    assert io_values[1]["index"] == 1
    assert io_values[1]["address"] == "1DEF..."
    assert io_values[1]["value"] == 2000
    print("  QuickLockIOValue pattern: OK")

    print("  User code simulation: PASSED")


def test_arithmetic_with_height():
    """Test arithmetic operations with height values."""
    print("Testing arithmetic with height...")

    from graphsense.compat import CompatInt

    height = CompatInt(1000)

    # Common arithmetic
    assert height + 10 == 1010
    assert height - 10 == 990
    assert height * 2 == 2000
    assert height / 2 == 500.0
    assert height // 2 == 500
    assert height % 3 == 1

    # Using .value in arithmetic
    assert height.value + 10 == 1010
    assert height.value * 2 == 2000

    # Comparisons
    assert height > 999
    assert height >= 1000
    assert height < 1001
    assert height <= 1000
    assert height == 1000
    assert height != 999

    # In expressions
    max_height = max(height, 500)
    assert max_height == 1000

    min_height = min(height, 2000)
    assert min_height == 1000

    print("  Arithmetic with height: PASSED")


def run_all_tests():
    """Run all compatibility tests."""
    print("=" * 60)
    print("Running backward compatibility tests")
    print("=" * 60)

    try:
        test_compat_int()
        test_compat_list()
        test_compat_list_with_objects()
        test_nested_value_patterns()
        test_user_code_simulation()
        test_arithmetic_with_height()

        print("=" * 60)
        print("ALL TESTS PASSED!")
        print("=" * 60)
        return 0

    except AssertionError as e:
        print(f"\nTEST FAILED: {e}")
        return 1
    except ImportError as e:
        print(f"\nIMPORT ERROR: {e}")
        print("Make sure the graphsense package is properly generated and patched.")
        return 1
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())
