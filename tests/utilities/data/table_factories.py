from pyspark.sql.types import StringType, IntegerType, StructType, StructField

from typing import Iterable
from pyspark.sql import SparkSession

from tests.utilities.data.spark_tester import SparkTester


def create_dim_courses_df(spark: SparkSession, data: Iterable):
    schema = StructType(
        [
            StructField("course_key", StringType(), True),
            StructField("subject_id", IntegerType(), True),
            StructField("faculty_id", StringType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_dim_courses_table(spark: SparkSession, data: Iterable):
    yield from SparkTester(spark).create_table_from_df(
        create_dim_courses_df(spark, data), "subjects", "dim_courses"
    )


def create_dim_apply_df(spark: SparkSession, data: Iterable):
    schema = StructType(
        [
            StructField("apply_id", StringType(), True),
            StructField("course_key", StringType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_dim_apply_table(spark: SparkSession, data: Iterable):
    yield from SparkTester(spark).create_table_from_df(
        create_dim_apply_df(spark, data), "actions", "dim_apply"
    )
