import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session", name="spark_session")
def spark_session():
    spark = SparkSession.builder.master("local").getOrCreate()
    yield spark

    spark.stop()
