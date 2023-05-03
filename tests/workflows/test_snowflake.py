import os
import urllib
import zipfile

import dask.dataframe as dd
import pandas as pd
import pytest
from dask.distributed import wait

snowflake = pytest.importorskip("snowflake")
sqlalchemy = pytest.importorskip("sqlalchemy")
dask_snowflake = pytest.importorskip("dask_snowflake")

from dask_snowflake import read_snowflake, to_snowflake  # noqa: E402
from snowflake_sqlalchemy import URL  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402


@pytest.mark.client("snowflake")
def test_exploratory_analysis(client):
    """Upload NYC bike dataset to Snowflake. Then read
    and explore the data."""

    csv_path = "nyc-bike-data"

    def unzip(filename: str):
        """Unzip csv file to csv_path"""
        if not os.path.exists(f"{csv_path}/{filename}"):
            url = f"https://s3.amazonaws.com/tripdata/{filename}.zip"
            zip_path, _ = urllib.request.urlretrieve(url)
            with zipfile.ZipFile(zip_path, "r") as f:
                f.extractall(csv_path)
        return filename

    filenames = [
        f"{ts.year}{ts.month:02}-citibike-tripdata.csv"
        for ts in pd.date_range(start="2022-01-01", end="2023-03-01", freq="MS")
    ]

    # download and unzip files
    futures = client.map(unzip, filenames)
    wait(futures)

    # preprocess data
    def safe_int(x):
        """Some station IDs are not correct integers"""
        try:
            return int(float(x))
        except Exception:
            # if station ID is not an int, return -1
            return -1

    ddf = dd.read_csv(
        f"{csv_path}/*.csv",
        converters={"start_station_id": safe_int, "end_station_id": safe_int},
    )

    # filter out incorrect station IDs
    ddf = ddf[(ddf.start_station_id != -1) & (ddf.end_station_id != -1)].reset_index(
        drop=True
    )

    # create boolean is_member and drop member_casual
    ddf["is_member"] = ddf.member_casual.apply(
        lambda x: x == "member", meta=("is_member", bool)
    )
    ddf = ddf.drop(columns="member_casual")

    # repartition to ensure even chunks
    ddf = ddf.repartition(partition_size="100Mb")

    input_npartitions = ddf.npartitions

    wait(ddf)

    # connect to snowflake and create table
    conn_kwargs = {
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ.get("SNOWFLAKE_ROLE", "public"),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "testdb"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "public"),
    }
    table_name = "citibike_tripdata"
    engine = create_engine(URL(**conn_kwargs))
    engine.execute(f"drop table if exists {table_name}")
    engine.execute(
        f"""create table if not exists {table_name} (
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

    # save data to Snowflake
    to_snowflake(ddf, name=table_name, connection_kwargs=conn_kwargs)

    query = f"SELECT * FROM {table_name}"

    _ = read_snowflake(
        query, connection_kwargs=conn_kwargs, npartitions=input_npartitions
    )

    # TODO
