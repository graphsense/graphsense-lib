from graphsenselib.ingest.common import BlockRangeContent
from graphsenselib.ingest.source import split_blockrange


def test_split_blockrange():
    blockrange = (0, 10)
    size = 3
    result = list(split_blockrange(blockrange, size))
    assert result == [(0, 2), (3, 5), (6, 8), (9, 10)]


def test_split_blockrange_unclean():
    blockrange = (9998, 10_001)
    size = 10_000
    result = list(split_blockrange(blockrange, size))
    assert result == [(9998, 9999), (10_000, 10_001)]


def test_split_blockrange_single1():
    blockrange = (10_000, 10_000)
    size = 10_000
    result = list(split_blockrange(blockrange, size))
    assert result == [(10_000, 10_000)]


def test_split_blockrange_single2():
    blockrange = (9999, 9999)
    size = 10_000
    result = list(split_blockrange(blockrange, size))
    assert result == [(9999, 9999)]


def test_block_range():
    BlockRangeContent(table_contents={})
