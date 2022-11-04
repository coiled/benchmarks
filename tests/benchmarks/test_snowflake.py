import os
import uuid

import dask
import pytest
from dask_snowflake import read_snowflake, to_snowflake
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


@pytest.fixture
def table(connection_kwargs):
    name = f"test_table_{uuid.uuid4().hex}".upper()

    yield name

    engine = create_engine(URL(**connection_kwargs))
    engine.execute(f"DROP TABLE IF EXISTS {name}")


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

    if False:
        query = f"SELECT * FROM {table}"
        (
            read_snowflake(query, connection_kwargs=connection_kwargs)
            .rename(columns=str.upper)  # snowflake's column casing is weird
            .sort_values(by="X")  # sort needed to re-sync the partitions
            .reset_index(drop=True)
            .compute()
        )
