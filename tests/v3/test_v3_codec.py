"""The Spark expressions and their Python twins must agree exactly.

Two producers disagreeing about a key is the failure mode this migration is most
exposed to, and it fails silently: a wrong bucket writes a row nobody can read.
These run against a real local SparkSession for that reason.
"""

import zlib

import pytest

from graphsense_v3.codec import bucket, decode_address, encode_address, search_prefix

pyspark = pytest.importorskip("pyspark")

ADDRESSES = {
    "btc": [
        "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
        "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy",
        "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq",
        "bc1p5cyxnuxmeuwuvkwfem96lqzszd02n6xdcjrs20cac6yqjjwudpxqkedrcr",
    ],
    "ltc": [
        "LNE5crMWZ1CzBHiF9wUmVQCjjXTuFsHzGP",
        "ltc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
    ],
}


@pytest.mark.parametrize("network", sorted(ADDRESSES))
def test_encoding_round_trips(network: str) -> None:
    for address in ADDRESSES[network]:
        assert decode_address(network, encode_address(network, address)) == address


def test_spark_crc32_matches_zlib(spark) -> None:
    """The bucket function rests on this identity: F.crc32 is java.util.zip.CRC32
    and zlib.crc32 is the same IEEE CRC-32. If it ever stops holding, every
    bucket silently moves."""
    from pyspark.sql import functions as F

    encoded = [(encode_address(net, a),) for net in ADDRESSES for a in ADDRESSES[net]]
    df = spark.createDataFrame(encoded, "address binary")
    rows = df.withColumn("crc", F.crc32(F.col("address"))).collect()
    for row in rows:
        assert row["crc"] == zlib.crc32(bytes(row["address"]))


@pytest.mark.parametrize("buckets", [1, 16, 100, 300_000])
def test_bucket_expr_matches_python(spark, buckets: int) -> None:
    from graphsense_v3.spark.udf import bucket_expr

    encoded = [(encode_address(net, a),) for net in ADDRESSES for a in ADDRESSES[net]]
    df = spark.createDataFrame(encoded, "address binary")
    rows = df.withColumn("b", bucket_expr(df["address"], buckets)).collect()
    for row in rows:
        assert row["b"] == bucket(bytes(row["address"]), buckets)


def test_encode_udf_matches_python(spark) -> None:
    from graphsense_v3.spark.udf import encode_address_udf

    df = spark.createDataFrame([(a,) for a in ADDRESSES["btc"]], "address string")
    rows = df.withColumn("b", encode_address_udf("btc")(df["address"])).collect()
    for row in rows:
        assert bytes(row["b"]) == encode_address("btc", row["address"])


def test_prefix_udf_matches_python(spark) -> None:
    from graphsense_v3.spark.udf import search_prefix_udf

    df = spark.createDataFrame([(a,) for a in ADDRESSES["btc"]], "address string")
    rows = df.withColumn("p", search_prefix_udf("btc")(df["address"])).collect()
    for row in rows:
        assert row["p"] == search_prefix("btc", row["address"])


def test_bech32_prefix_carries_four_varying_characters() -> None:
    """v2 strips only 'bc', leaving '1q...'/'1p...' -- two of four characters
    constant, so the whole segwit space lands in 32^2 = 1024 partitions."""
    v3 = {search_prefix("btc", a) for a in ADDRESSES["btc"] if a.startswith("bc1")}
    v2 = {a[len("bc") :][:4] for a in ADDRESSES["btc"] if a.startswith("bc1")}
    assert all(not p.startswith(("1q", "1p")) for p in v3)
    assert all(p.startswith(("1q", "1p")) for p in v2)


def test_bucket_rejects_zero() -> None:
    with pytest.raises(ValueError, match="positive"):
        bucket(b"x", 0)
