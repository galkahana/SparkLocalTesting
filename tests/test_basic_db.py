from pytest import fixture
from pyspark.sql import SparkSession

from tests.utilities.data.table_factories import (
    create_dim_listings_table,
    create_dim_apply_table,
)


@fixture
def simple_jobs_table(spark: SparkSession, jobs_db):
    yield from create_dim_listings_table(
        spark,
        [
            ("listing_1", 1, "org_1"),
            ("listing_2", 2, "org_2"),
            ("listing_3", 1, "org_1"),
            ("listing_4", 1, "org_3"),
        ],
    )


@fixture
def simple_applies_table(spark: SparkSession, interactions_db):
    yield from create_dim_apply_table(
        spark,
        [
            ("apply_1", "listing_1"),
            ("apply_2", "listing_1"),
            ("apply_3", "listing_3"),
            ("apply_4", "listing_2"),
            ("apply_5", "listing_1"),
        ],
    )


class TestBasicDB:
    def test_correctly_counts_applies_per_org(
        self, spark: SparkSession, simple_jobs_table, simple_applies_table
    ):
        expected_result = [("org_1", 4), ("org_2", 1), ("org_3", 0)]

        df = spark.sql(
            """  
            select 
                zr_org_id,
                count(apply_id) as count
            from
                jobs.vw_dim_listings
                left join interactions.dim_apply on vw_dim_listings.listing_key = dim_apply.listing_key
            group by 1
            order by 1
            """
        )

        result = [(x["zr_org_id"], x["count"]) for x in df.collect()]

        assert result == expected_result
