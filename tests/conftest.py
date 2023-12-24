from pyspark.sql import SparkSession
from typing import cast
from pytest import fixture
from tests.utilities.data.spark_tester import SparkTester


@fixture
def spark():
    spark = cast(
        SparkSession,
        SparkSession.builder.appName("Pyspark Testing")
        .enableHiveSupport()
        .config("spark.log.level", "ERROR")
        .config("spark.ui.showConsoleProgress", False)
        .getOrCreate(),
    )
    yield spark
    spark.stop()


@fixture
def subjects_db(spark: SparkSession):
    yield from SparkTester(spark).create_db("subjects")


@fixture
def actions_db(spark: SparkSession):
    yield from SparkTester(spark).create_db("actions")
