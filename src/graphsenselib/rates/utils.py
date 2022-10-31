from typing import List


def convert_to_fiat(value: int, rates: List[int]) -> List[int]:
    # col(valueColumn) * x / 1e6 + 0.5).cast(LongType) / 100.0
    return [int(value * r / 1e6 + 0.5) / 100 for r in rates]
