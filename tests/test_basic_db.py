from pytest import fixture
from pyspark.sql import SparkSession

from tests.utilities.data.table_factories import (
    create_dim_courses_table,
    create_dim_apply_table,
)


@fixture
def simple_courses_table(spark: SparkSession, subjects_db):
    yield from create_dim_courses_table(
        spark,
        [
            ("course_1", 1, "faculty_1"),
            ("course_2", 2, "faculty_2"),
            ("course_3", 1, "faculty_1"),
            ("course_4", 1, "faculty_3"),
        ],
    )


@fixture
def simple_applies_table(spark: SparkSession, actions_db):
    yield from create_dim_apply_table(
        spark,
        [
            ("apply_1", "course_1"),
            ("apply_2", "course_1"),
            ("apply_3", "course_3"),
            ("apply_4", "course_2"),
            ("apply_5", "course_1"),
        ],
    )


class TestBasicDB:
    def test_correctly_counts_applies_per_org(
        self, spark: SparkSession, simple_courses_table, simple_applies_table
    ):
        expected_result = [("faculty_1", 4), ("faculty_2", 1), ("faculty_3", 0)]

        df = spark.sql(
            """  
            select 
                faculty_id,
                count(apply_id) as count
            from
                subjects.dim_courses
                left join actions.dim_apply on dim_courses.course_key = dim_apply.course_key
            group by 1
            order by 1
            """
        )

        result = [(x["faculty_id"], x["count"]) for x in df.collect()]

        assert result == expected_result
