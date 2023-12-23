from typing import Optional
from pyspark.sql import SparkSession, DataFrame


class SparkTester:
    spark: SparkSession

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @staticmethod
    def _location_stmt(location: Optional[str] = ""):
        return f"LOCATION '{location}'" if location else ""

    def sql(self, sql: str):
        return self.spark.sql(sql)

    def create_db(self, db_name: str, location: Optional[str] = ""):
        self.sql(
            f"CREATE DATABASE IF NOT EXISTS {db_name} {self._location_stmt(location)}"
        )
        yield
        self.sql(f"DROP DATABASE IF EXISTS {db_name}")

    def create_table_from_temp_view(
        self, view_name: str, db_name: str, table_name: str
    ):
        qualified_name = f"{db_name}.{table_name}"

        self.sql(
            f"CREATE TABLE IF NOT EXISTS {qualified_name} AS SELECT * FROM {view_name}"
        )
        yield
        self.sql(f"DROP TABLE IF EXISTS {qualified_name}")

    def create_table_from_df(self, df: DataFrame, db_name: str, table_name: str):
        view_name = f"{db_name}__{table_name}"  # perhaps just a random string?
        df.createOrReplaceTempView(view_name)
        yield from self.create_table_from_temp_view(view_name, db_name, table_name)
        # actually....why keep the view after creating it...can destroy straight after creation
        self.spark.catalog.dropTempView(view_name)
