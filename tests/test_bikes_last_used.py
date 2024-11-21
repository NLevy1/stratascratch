from datetime import datetime

import pytest
from chispa import assert_df_equality
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.bikes_last_used import get_bikes_last_used


@pytest.fixture()
def input_df(spark_session):
    data = [
        ("25-3-2012 10:40", "W00576"),
        ("28-3-2012 19:11", "W00576"),
        ("12-3-2012 22:37", "W01215"),
        ("30-3-2012 20:00", "W01215"),
        ("12-3-2012 20:15", "W00455"),
    ]
    schema = StructType(
        [
            StructField("end_time", StringType(), True),
            StructField("bike_number", StringType(), True),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture()
def expected_df(spark_session):
    data = [
        (datetime(2012, 3, 30, 20, 00), "W01215"),
        (datetime(2012, 3, 28, 19, 11), "W00576"),
        (datetime(2012, 3, 12, 20, 15), "W00455"),
    ]
    schema = StructType(
        [
            StructField("end_time", TimestampType(), True),
            StructField("bike_number", StringType(), True),
        ]
    )
    return spark_session.createDataFrame(data, schema)


def test_groupby_bikes(input_df, expected_df):
    actual_df = get_bikes_last_used(input_df)
    assert_df_equality(actual_df, expected_df)
