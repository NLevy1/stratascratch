import pytest


@pytest.fixture
def sample_df(spark_session):
    data = []
    schema = []
    return spark_session.createDataFrame(data, schema)


def test_dummy():
    assert True
