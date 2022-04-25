import dask.dataframe as dd
import pandas as pd


def test_quickstart(small_client):
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
    assert False
