import dask.dataframe as dd
import pandas as pd
import pytest

from ..utils_test import run_up_to_nthreads


@run_up_to_nthreads("small", 50, reason="fixed dataset")
@pytest.mark.client("small")
def test_csv_basic(client):
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
