"""
h2o-ai benchmark groupby part running on coiled.

Note: Only holistic aggregations (median and groupby-apply) use a shuffle with the
default split_out=1.
"""
import os

import dask_expr as dd
import pandas as pd
import pytest

from ..utils_test import run_up_to_nthreads

DATASETS = {
    "0.5 GB (csv)": "s3://coiled-datasets/h2o-benchmark/N_1e7_K_1e2/*.csv",
    "5 GB (csv)": "s3://coiled-datasets/h2o-benchmark/N_1e8_K_1e2/*.csv",
    "50 GB (csv)": "s3://coiled-datasets/h2o-benchmark/N_1e9_K_1e2/*.csv",
    "0.5 GB (parquet)": "s3://coiled-datasets/h2o-benchmark/N_1e7_K_1e2_parquet/*.parquet",
    "5 GB (parquet)": "s3://coiled-datasets/h2o-benchmark/N_1e8_K_1e2_parquet/*.parquet",
    "50 GB (parquet)": "s3://coiled-datasets/h2o-benchmark/N_1e9_K_1e2_parquet/*.parquet",
    "5 GB (parquet+pyarrow)": "s3://coiled-datasets/h2o-benchmark/pyarrow_strings/N_1e8_K_1e2/*.parquet",
    "50 GB (parquet+pyarrow)": "s3://coiled-datasets/h2o-benchmark/pyarrow_strings/N_1e9_K_1e2/*.parquet",
    "500 GB (parquet+pyarrow)": "s3://coiled-datasets/h2o-benchmark/pyarrow_strings/N_1e10_K_1e2/*.parquet",
}

enabled_datasets = os.getenv("H2O_DATASETS")
if enabled_datasets is not None:
    enabled_datasets = {k.strip() for k in enabled_datasets.split(",")}
    if unknown_datasets := enabled_datasets - DATASETS.keys():
        raise ValueError("Unknown h2o dataset(s): ", unknown_datasets)
else:
    enabled_datasets = {
        "0.5 GB (csv)",
        "0.5 GB (parquet)",
        "5 GB (parquet)",
    }


@pytest.fixture(params=list(DATASETS))
def ddf(request, small_client):
    if request.param not in enabled_datasets:
        raise pytest.skip("Disabled by default config or H2O_DATASETS env variable")

    n_gib = float(request.param.split(" GB ")[0])
    # 0.5 GB datasets are broken in 5~10 files
    # 5 GB -> 100 files
    # 50 GB -> 1000 files
    # 500 GB -> 10,000 files
    max_threads = max(20, int(n_gib * 20))
    run_up_to_nthreads(
        "small_cluster", max_threads, reason="fixed data size", as_decorator=False
    )

    uri = DATASETS[request.param]

    if uri.endswith("csv"):
        yield dd.read_csv(
            uri,
            dtype={
                "id1": "category",
                "id2": "category",
                "id3": "category",
                "id4": "Int32",
                "id5": "Int32",
                "id6": "Int32",
                "v1": "Int32",
                "v2": "Int32",
                "v3": "float64",
            },
            storage_options={"anon": True},
        )
    else:
        yield dd.read_parquet(uri, engine="pyarrow", storage_options={"anon": True})


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
