"""
h2o-ai benchmark groupby part running on coiled.
"""

import dask
import dask.dataframe as dd
import pandas as pd
import pytest
from packaging.version import Version

from ...utils_test import run_up_to_nthreads


@pytest.fixture(
    scope="module",
    params=[
        # "s3://coiled-datasets/h2o-benchmark/N_1e7_K_1e2_single.csv",
        # # "s3://coiled-datasets/h2o-benchmark/N_1e8_K_1e2_single.csv",
        # # "s3://coiled-datasets/h2o-benchmark/N_1e9_K_1e2_single.csv",
        # "s3://coiled-datasets/h2o-benchmark/N_1e7_K_1e2_parquet/*.parquet",
        # "s3://coiled-datasets/h2o-benchmark/N_1e8_K_1e2_parquet/*.parquet",
        # # "s3://coiled-datasets/h2o-benchmark/N_1e9_K_1e2_parquet/*.parquet",
        "s3://coiled-datasets/h2o-benchmark/pyarrow_strings/N_1e9_K_1e2/*.parquet",
    ],
    ids=[
        # "0.5 GB (csv)",
        # # "5 GB (csv)",
        # # "50 GB (csv)",
        # "0.5 GB (parquet)",
        # "5 GB (parquet)",
        "50 GB (parquet)",
    ],
)
def ddf(request):

    if request.param.endswith("csv"):
        yield dd.read_csv(
            request.param,
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
        yield dd.read_parquet(
            request.param,
            engine="pyarrow",
            storage_options={"anon": True},
            use_nullable_dtypes=True,
        )


@run_up_to_nthreads("small_cluster", 100, reason="fixed size data")
def test_q1(ddf, small_client):
    ddf = ddf[["id1", "v1"]]
    ddf.groupby("id1", dropna=False, observed=True).agg({"v1": "sum"}).compute()


@run_up_to_nthreads("small_cluster", 100, reason="fixed size data")
def test_q2(ddf, small_client):
    ddf = ddf[["id1", "id2", "v1"]]
    (
        ddf.groupby(["id1", "id2"], dropna=False, observed=True)
        .agg({"v1": "sum"})
        .compute()
    )


@run_up_to_nthreads("small_cluster", 100, reason="fixed size data")
def test_q3(ddf, small_client):
    ddf = ddf[["id3", "v1", "v3"]]
    (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "sum", "v3": "mean"})
        .compute()
    )


@run_up_to_nthreads("small_cluster", 100, reason="fixed size data")
def test_q4(ddf, small_client):
    ddf = ddf[["id4", "v1", "v2", "v3"]]
    (
        ddf.groupby("id4", dropna=False, observed=True)
        .agg({"v1": "mean", "v2": "mean", "v3": "mean"})
        .compute()
    )


@run_up_to_nthreads("small_cluster", 100, reason="fixed size data")
def test_q5(ddf, small_client):
    ddf = ddf[["id6", "v1", "v2", "v3"]]
    (
        ddf.groupby("id6", dropna=False, observed=True)
        .agg({"v1": "sum", "v2": "sum", "v3": "sum"})
        .compute()
    )


@run_up_to_nthreads("small_cluster", 100, reason="fixed size data")
@pytest.mark.skipif(
    Version(dask.__version__) < Version("2022.10.0"),
    reason="No support for median in dask < 2022.10.0",
)
def test_q6(ddf, small_client):
    ddf = ddf[["id4", "id5", "v3"]]
    (
        ddf.groupby(["id4", "id5"], dropna=False, observed=True)
        .agg({"v3": ["median", "std"]}, shuffle="tasks")
        .compute()  # requires shuffle="tasks"
    )


@run_up_to_nthreads("small_cluster", 100, reason="fixed size data")
def test_q7(ddf, small_client):
    ddf = ddf[["id3", "v1", "v2"]]
    (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "max", "v2": "min"})
        .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["range_v1_v2"]]
        .compute()
    )


@run_up_to_nthreads("small_cluster", 100, reason="fixed size data")
def test_q8(ddf, small_client):
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


@run_up_to_nthreads("small_cluster", 100, reason="fixed size data")
def test_q9(ddf, small_client):
    ddf = ddf[["id2", "id4", "v1", "v2"]]
    (
        ddf[["id2", "id4", "v1", "v2"]]
        .groupby(["id2", "id4"], dropna=False, observed=True)
        .apply(
            lambda x: pd.Series({"r2": x.corr()["v1"]["v2"] ** 2}),
            meta={"r2": "float64"},
        )
        .compute()
    )
