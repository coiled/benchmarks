import uuid

import dask.dataframe as dd
import pandas as pd
from coiled import Cluster


def test_quickstart_csv(small_client):
    ddf = dd.read_csv(
        "s3://coiled-runtime-ci/nyc-tlc/yellow_tripdata_2019_csv/yellow_tripdata_2019-*.csv",
        dtype={
            "payment_type": "UInt8",
            "VendorID": "UInt8",
            "passenger_count": "UInt8",
            "RatecodeID": "UInt8",
        },
        blocksize="16 MiB",
    ).persist()

    result = ddf.groupby("passenger_count").tip_amount.mean().compute()

    assert isinstance(result, pd.Series)
    assert not result.empty


def test_quickstart_parquet(small_client):
    ddf = dd.read_parquet(
        "s3://coiled-runtime-ci/nyc-tlc/yellow_tripdata_2019_parquet/yellow_tripdata_2019-*.parquet",
        columns=["passenger_count", "tip_amount"],
    ).persist()

    result = ddf.groupby("passenger_count").tip_amount.mean().compute()

    assert isinstance(result, pd.Series)
    assert not result.empty


def test_default_cluster_spinup_time(request, auto_benchmark_time):

    with Cluster(
        name=f"{request.node.originalname}-{uuid.uuid4().hex[:8]}", package_sync=True
    ):
        pass
