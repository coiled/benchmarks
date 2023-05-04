import os
import urllib
import uuid
import zipfile

import dask.dataframe as dd
import pandas as pd
import pytest
from dask.distributed import wait

snowflake = pytest.importorskip("snowflake")
sqlalchemy = pytest.importorskip("sqlalchemy")
dask_snowflake = pytest.importorskip("dask_snowflake")

from dask_snowflake import read_snowflake, to_snowflake  # noqa: E402
from snowflake.sqlalchemy import URL  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402


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
    csv_path = "nyc-bike-data"

    def unzip(filename: str):
        """Unzip csv file to csv_path"""
        if not os.path.exists(f"{csv_path}/{filename}"):
            url = f"https://s3.amazonaws.com/tripdata/{filename}.zip"
            try:
                zip_path, _ = urllib.request.urlretrieve(url)
            except urllib.error.HTTPError:
                # some of the filenames have "citbike" vs "citibike"
                zip_path, _ = urllib.request.urlretrieve(
                    url.replace("-citibike-", "-citbike-")
                )
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
    ddf["is_member"] = ddf.member_casual == "member"

    ddf = ddf.drop(columns="member_casual")

    # repartition to ensure even chunks
    ddf = ddf.repartition(partition_size="100Mb")

    wait(ddf)

    # save data to Snowflake
    to_snowflake(ddf, name=table, connection_kwargs=connection_kwargs)


@pytest.mark.client("snowflake")
def test_read_explore(client, connection_kwargs, table):
    """Read and explore NYC bike dataset from Snowflake"""
    table = "citibike_tripdata"  # persistent table

    ddf = read_snowflake(
        f"SELECT * FROM {table}",
        connection_kwargs=connection_kwargs,
        npartitions=200,
    ).rename(
        columns=str.lower
    )  # snowflake has all uppercase

    ddf["is_roundtrip"] = ddf["start_station_id"] == ddf["end_station_id"]

    # compute counts of roundtrips vs one-way
    roundtrips = (  # noqa: F841
        ddf.is_roundtrip.value_counts().compute().reset_index(name="count")
    )

    """
    # maybe plot roundtrips vs one-way
    import seaborn as sns
    sns.barplot(roundtrips, x="is_roundtrip", y="count")
    """

    # explore trip duration
    ddf["trip_duration"] = ddf["ended_at"] - ddf["started_at"]
    ddf["trip_minutes"] = ddf["trip_duration"].dt.total_seconds() // 60

    # find quantiles
    duration_quantile = ddf["trip_minutes"].quantile([0.05, 0.95]).compute()
    min_duration, max_duration = duration_quantile[0.05], duration_quantile[0.95]

    # filter out outliers
    ddf = ddf[
        (ddf.trip_duration_minutes >= min_duration)
        & (ddf.trip_duration_minutes <= max_duration)
    ]

    durations = (  # noqa: F841
        ddf.trip_duration_minutes.value_counts()
        .compute()
        .reset_index(name="count")
        .rename(columns={"index": "trip_minutes"})
    )

    """
    # maybe plot the durations / counts
    sns.barplot(durations, x="trip_minutes", y="count")
    """
