import os
import uuid

import coiled
import dask.dataframe as dd
import pandas
from dask.distributed import Client

SOFTWARE = os.environ["SOFTWARE_ENV"]


def test_quickstart():
    cluster = coiled.Cluster(
        software=SOFTWARE,
        name="nyc-quickstart_" + str(uuid.uuid4()),
        account="dask-engineering",
        n_workers=10,
        backend_options={"spot": False},
    )

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

        assert isinstance(result, pandas.core.series.Series)
        assert not result.empty
