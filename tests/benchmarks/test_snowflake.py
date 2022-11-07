import os
import uuid

import dask
import pytest

dask_snowflake = pytest.importorskip("dask_snowflake", minversion="0.1")

from dask_snowflake import read_snowflake, to_snowflake  # noqa: E402
from snowflake.sqlalchemy import URL  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402


@pytest.fixture
def table(connection_kwargs):
    name = f"test_table_{uuid.uuid4().hex}".upper()

    yield name

    engine = create_engine(URL(**connection_kwargs))
    engine.execute(f"DROP TABLE IF EXISTS {name}")


@pytest.fixture
def perma_table():
    return "TEST_TABLE_PERMADATA"


@pytest.fixture(scope="module")
def connection_kwargs():
    return dict(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        database="TESTDB",
        schema="TESTSCHEMA",
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        application="dask",
    )


@pytest.mark.skipif(
    "SNOWFLAKE_USER" not in os.environ.keys(), reason="no snowflake credentials"
)
def test_write_to_snowflake(table, connection_kwargs, small_client):

    ddf = dask.datasets.timeseries(
        start="2000-01-01", end="2000-03-31", freq="1T", partition_freq="1W"
    )
    to_snowflake(ddf, name=table, connection_kwargs=connection_kwargs)


@pytest.mark.skipif(
    "SNOWFLAKE_USER" not in os.environ.keys(), reason="no snowflake credentials"
)
def test_read_from_snowflake(perma_table, connection_kwargs, small_client):
    query = f"SELECT * FROM {perma_table}"
    reader = read_snowflake(query, connection_kwargs=connection_kwargs)
    reader.compute()


@pytest.mark.skipif(
    "SNOWFLAKE_USER" not in os.environ.keys(), reason="no snowflake credentials"
)
def test_dask_means_from_snowfloake(perma_table, connection_kwargs, small_client):
    query = f"SELECT * FROM {perma_table}"
    reader = (
        read_snowflake(query, connection_kwargs=connection_kwargs)
        .groupby("NAME")["X"]
        .median()
    )
    reader.compute()


@pytest.mark.skipif(
    "SNOWFLAKE_USER" not in os.environ.keys(), reason="no snowflake credentials"
)
def test_snowflake_means_inside_snowflake(perma_table, connection_kwargs, small_client):
    median_query = f"SELECT NAME, MEDIAN(X) as MEAN_X FROM {perma_table} GROUP BY NAME"
    reader = read_snowflake(median_query, connection_kwargs=connection_kwargs)
    reader.compute()
