import dask.dataframe as dd
import dask_deltatable as ddt
import pytest


@pytest.fixture(params=["read_deltalake", "read_parquet"])
def ddf(request, small_client):
    uri = "s3://coiled-datasets/delta/ds20f_100M/"
    if request.param == "read_deltalake":
        yield ddt.read_deltalake(uri, delta_storage_options={"AWS_REGION": "us-east-2"})
    else:
        yield dd.read_parquet(f"{uri}*.parquet", engine="pyarrow")


def test_column_agg(ddf):
    ddf["float1"].agg(["sum", "mean"]).compute()


def test_group_agg(ddf):
    ddf = ddf[["int1", "int2", "int3"]]
    (
        ddf.groupby(["int2", "int3"], dropna=False, observed=True)
        .agg({"int1": ["sum", "mean"]})
        .compute()
    )


def test_group_median(ddf, shuffle_method):
    ddf = ddf[["int1", "int2", "int3"]]
    (
        ddf.groupby(["int2", "int3"], dropna=False, observed=True)
        .agg({"int1": ["median", "std"]}, shuffle=shuffle_method)
        .compute()
    )
