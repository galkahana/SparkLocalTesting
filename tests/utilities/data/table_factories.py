from pyspark.sql.types import StringType, IntegerType, StructType, StructField

from typing import Iterable
from pyspark.sql import SparkSession

from tests.utilities.data.spark_tester import SparkTester


def create_dim_listings_df(spark: SparkSession, data: Iterable):
    schema = StructType(
        [
            StructField("listing_key", StringType(), True),
            StructField("quiz_id", IntegerType(), True),
            StructField("zr_org_id", StringType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_dim_listings_table(spark: SparkSession, data: Iterable):
    yield from SparkTester(spark).create_table_from_df(
        create_dim_listings_df(spark, data), "jobs", "vw_dim_listings"
    )


def create_dim_apply_df(spark: SparkSession, data: Iterable):
    schema = StructType(
        [
            StructField("apply_id", StringType(), True),
            StructField("listing_key", StringType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_dim_apply_table(spark: SparkSession, data: Iterable):
    yield from SparkTester(spark).create_table_from_df(
        create_dim_apply_df(spark, data), "interactions", "dim_apply"
    )
