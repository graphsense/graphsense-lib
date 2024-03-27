import logging
import os

# todo  requirements delta-spark and pyspark
import time

import pyspark
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

# todo restore previous parquet functionality?


def write_delta(
    path: str,
    table_name: str,
    data: list,
    schema_table: dict,
    partition_cols: list = ["partition"],
    primary_keys: list = None,
) -> None:
    print("Writing ", table_name)
    if not data:
        return

    builder = (
        pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = spark.createDataFrame(data, schema=schema_table[table_name])

    table_path = os.path.join(path, table_name)
    # check if there is a deltatable existing already
    if not os.path.exists(table_path):
        print(f"Creating Delta table {table_name} at {table_path}")
        time_ = time.time()
        df.write.format("delta").mode("overwrite").partitionBy(partition_cols).save(
            table_path
        )
        print(f"Time to write {table_name} to Delta: {time.time() - time_}")
    else:
        rewrite_existing = False
        # upsert
        print(f"Merge Delta table {table_name} at {table_path} using pyspark")

        time_ = time.time()
        target = DeltaTable.forPath(spark, table_path)

        condition_str = " AND ".join(
            [f"target.{key} = source.{key}" for key in primary_keys + partition_cols]
        )
        merge_builder = (
            target.alias("target")
            .merge(df.alias("source"), condition_str)
            .whenNotMatchedInsertAll()
        )

        if rewrite_existing:
            merge_builder.whenMatchedUpdateAll().execute()
        else:
            merge_builder.execute()

        print(
            f"Time to merge {table_name} of length {len(data)} to Delta: "
            f"{time.time() - time_}"
        )
