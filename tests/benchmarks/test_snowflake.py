import os
import uuid

import dask
import pytest
from dask_snowflake import read_snowflake, to_snowflake
from distributed import Client
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
def test_write_to_snowflake(
    table, connection_kwargs, small_cluster, upload_cluster_dump, benchmark_all
):

    ddf = dask.datasets.timeseries(
        start="2000-01-01", end="2000-03-31", freq="1T", partition_freq="1W"
    )

    with Client(small_cluster) as client:
        small_cluster.scale(10)
        client.wait_for_workers(10)
        with upload_cluster_dump(client), benchmark_all(client):
            to_snowflake(ddf, name=table, connection_kwargs=connection_kwargs)

    with Client(small_cluster) as client:
        small_cluster.scale(10)
        client.wait_for_workers(10)

        raw_query = f"SELECT * FROM {table}"

        with upload_cluster_dump(client), benchmark_all(client):
            reader = read_snowflake(raw_query, connection_kwargs=connection_kwargs)
            reader.compute()

        with upload_cluster_dump(client), benchmark_all(client):
            reader = (
                read_snowflake(raw_query, connection_kwargs=connection_kwargs)
                .groupby("NAME")["X"]
                .median()
            )
            reader.compute()

        with upload_cluster_dump(client), benchmark_all(client):
            median_query = f"SELECT NAME, MEDIAN(X) FROM {table} GROUP BY NAME"
            reader = read_snowflake(median_query, connection_kwargs=connection_kwargs)
            reader.compute()
