import os
import uuid

import dask.dataframe as dd
import pandas as pd
import pytest
from dask.distributed import wait
from sqlalchemy import create_engine  # noqa: E402

pytest.importorskip("snowflake.sqlalchemy", reason="snowflake.sqlalchemy not available")
pytest.importorskip("dask_snowflake", reason="dask_snowflake not available")

from dask_snowflake import read_snowflake, to_snowflake  # noqa: E402
from snowflake.sqlalchemy import URL  # noqa: E402


@pytest.fixture(scope="module")
def connection_kwargs():
    return {
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ.get("SNOWFLAKE_ROLE", "public"),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "testdb"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "public"),
    }


@pytest.fixture
def table(connection_kwargs):
    """Connect to snowflake and create table"""
    name = f"citibike_tripdata_{uuid.uuid4().hex}"
    engine = create_engine(URL(**connection_kwargs))
    engine.execute(f"DROP TABLE IF EXISTS {name}")
    engine.execute(
        f"""create table if not exists {name} (
                ride_id varchar not null unique,
                rideable_type varchar not null,
                started_at timestamp not null,
                ended_at timestamp not null,
                start_station_name varchar not null,
                start_station_id smallint not null,
                end_station_name varchar not null,
                end_station_id smallint not null,
                start_lat number,
                start_lng number,
                end_lat number,
                end_lng number,
                is_member boolean not null
            )"""
    )
    yield name
    # after the data is written, delete table
    engine.execute(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.client("snowflake")
def test_write(client, connection_kwargs, table):
    csv_paths = [
        f"tripdata/{ts.year}{ts.month:02}-*.csv.zip"
        for ts in pd.date_range(start="2022-01-01", end="2023-03-01", freq="MS")
    ]

    # preprocess data
    def safe_int(x):
        """Some station IDs are not correct integers"""
        try:
            return int(float(x))
        except Exception:
            # if station ID is not an int, return -1
            return -1

    ddf = dd.read_csv(
        csv_paths,
        compression="zip",
        blocksize=None,
        converters={"start_station_id": safe_int, "end_station_id": safe_int},
    )

    # filter out incorrect station IDs
    ddf = ddf[(ddf.start_station_id != -1) & (ddf.end_station_id != -1)].reset_index(
        drop=True
    )

    # create boolean is_member and drop member_casual
    ddf["is_member"] = ddf.member_casual == "member"

    ddf = ddf.drop(columns="member_casual")

    # repartition to ensure even chunks
    ddf = ddf.repartition(partition_size="100Mb")

    wait(ddf)

    # save data to Snowflake
    to_snowflake(ddf, name=table, connection_kwargs=connection_kwargs)


@pytest.mark.client("snowflake")
def test_read(client, connection_kwargs):
    """Read and explore NYC bike dataset from Snowflake"""
    table = "citibike_tripdata"  # persistent table

    read_snowflake(
        f"SELECT * FROM {table}",
        connection_kwargs=connection_kwargs,
        npartitions=200,
    ).rename(columns=str.lower).compute()
