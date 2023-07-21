import os

import dask.dataframe as dd
import dask_deltatable as ddt
import pandas as pd
import pytest

DATASETS = {
    "0.5 GB": "s3://coiled-datasets/h2o-delta/N_1e7_K_1e2/",
    "5 GB": "s3://coiled-datasets/h2o-delta/N_1e8_K_1e2/",
}

enabled_datasets = os.getenv("H2O_DELTA_DATASETS")
if enabled_datasets is not None:
    enabled_datasets = {k.strip() for k in enabled_datasets.split(",")}
    if unknown_datasets := enabled_datasets - DATASETS.keys():
        raise ValueError("Unknown h2o-delta dataset(s): ", unknown_datasets)
else:
    enabled_datasets = {
        "0.5 GB",
        "5 GB",
    }


@pytest.fixture(params=list(DATASETS))
def uri(request):
    if request.param not in enabled_datasets:
        raise pytest.skip(
            "Disabled by default config or H2O_DELTA_DATASETS env variable"
        )
    return DATASETS[request.param]


@pytest.fixture(params=["read_deltalake", "read_parquet"])
def ddf(request, small_client, uri):
    if request.param == "read_deltalake":
        yield ddt.read_deltalake(
            uri, delta_storage_options={"AWS_REGION": "us-east-2", "anon": "true"}
        )
    else:
        yield dd.read_parquet(
            f"{uri}*.parquet", engine="pyarrow", storage_options={"anon": "true"}
        )


def test_q1(ddf):
    ddf = ddf[["id1", "v1"]]
    ddf.groupby("id1", dropna=False, observed=True).agg({"v1": "sum"}).compute()


def test_q2(ddf):
    ddf = ddf[["id1", "id2", "v1"]]
    (
        ddf.groupby(["id1", "id2"], dropna=False, observed=True)
        .agg({"v1": "sum"})
        .compute()
    )


def test_q3(ddf):
    ddf = ddf[["id3", "v1", "v3"]]
    (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "sum", "v3": "mean"})
        .compute()
    )


def test_q4(ddf):
    ddf = ddf[["id4", "v1", "v2", "v3"]]
    (
        ddf.groupby("id4", dropna=False, observed=True)
        .agg({"v1": "mean", "v2": "mean", "v3": "mean"})
        .compute()
    )


def test_q5(ddf):
    ddf = ddf[["id6", "v1", "v2", "v3"]]
    (
        ddf.groupby("id6", dropna=False, observed=True)
        .agg(
            {"v1": "sum", "v2": "sum", "v3": "sum"},
        )
        .compute()
    )


def test_q6(ddf, shuffle_method):
    # Median aggregation uses an explicitly-set shuffle
    ddf = ddf[["id4", "id5", "v3"]]
    (
        ddf.groupby(["id4", "id5"], dropna=False, observed=True)
        .agg({"v3": ["median", "std"]}, shuffle=shuffle_method)
        .compute()  # requires shuffle arg to be set explicitly
    )


def test_q7(ddf):
    ddf = ddf[["id3", "v1", "v2"]]
    (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "max", "v2": "min"})
        .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["range_v1_v2"]]
        .compute()
    )


def test_q8(ddf, configure_shuffling):
    # .groupby(...).apply(...) uses a shuffle to transfer data before applying the function
    ddf = ddf[["id6", "v1", "v2", "v3"]]
    (
        ddf[~ddf["v3"].isna()][["id6", "v3"]]
        .groupby("id6", dropna=False, observed=True)
        .apply(
            lambda x: x.nlargest(2, columns="v3"),
            meta={"id6": "Int64", "v3": "float64"},
        )[["v3"]]
        .compute()
    )


def test_q9(ddf, configure_shuffling):
    # .groupby(...).apply(...) uses a shuffle to transfer data before applying the function
    ddf = ddf[["id2", "id4", "v1", "v2"]]
    (
        ddf[["id2", "id4", "v1", "v2"]]
        .groupby(["id2", "id4"], dropna=False, observed=True)
        .apply(
            lambda x: pd.Series({"r2": x.corr(numeric_only=True)["v1"]["v2"] ** 2}),
            meta={"r2": "float64"},
        )
        .compute()
    )
