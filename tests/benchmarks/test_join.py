import dask.dataframe as dd
import pytest

from ..utils_test import wait


@pytest.mark.client("uber_lyft_large")
def test_set_index_on_uber_lyft(client, configure_shuffling):
    df = dd.read_parquet(
        "s3://coiled-datasets/uber-lyft-tlc/", storage_options={"anon": True}
    )
    result = df.set_index("PULocationID")
    wait(result, client, 20 * 60)
