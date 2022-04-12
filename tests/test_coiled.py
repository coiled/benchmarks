import sys
import uuid

import dask.dataframe as dd
import pandas as pd
from coiled.v2 import Cluster
from dask.distributed import Client

SOFTWARE = f"dask-engineering/coiled_dist-py{sys.version_info[0]}{sys.version_info[1]}"


def test_quickstart():
    with Cluster(
        software=SOFTWARE,
        name="nyc-quickstart_" + str(uuid.uuid4()),
        account="dask-engineering",
        n_workers=10,
    ) as cluster:

        with Client(cluster) as client:  # noqa F841
            ddf = dd.read_csv(
                "s3://nyc-tlc/trip data/yellow_tripdata_2019-*.csv",
                dtype={
                    "payment_type": "UInt8",
                    "VendorID": "UInt8",
                    "passenger_count": "UInt8",
                    "RatecodeID": "UInt8",
                },
                storage_options={"anon": True},
                blocksize="16 MiB",
            ).persist()

            result = ddf.groupby("passenger_count").tip_amount.mean().compute()

        assert isinstance(result, pd.Series)
        assert not result.empty
