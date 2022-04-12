"""
h2o-ai benchmark groupby part running on coiled.
"""

import uuid

import coiled
import dask.dataframe as dd
import pandas as pd
import pytest
from dask.distributed import Client

from .utils import runtime_software_env


@pytest.fixture(scope="module")
def cluster():
    with coiled.Cluster(
        software=runtime_software_env,
        name="h2o-groupby-benchmark_" + str(uuid.uuid4()),
        account="dask-engineering",
        n_workers=4,
        backend_options={"spot": False},
    ) as cluster:
        yield cluster


@pytest.fixture(autouse=True)
def client(cluster):
    with Client(cluster) as client:
        yield client
        client.restart()


@pytest.fixture(
    scope="module",
    params=[
        "s3://coiled-datasets/h2o-benchmark/N_1e7_K_1e2_single.csv",
        "s3://coiled-datasets/h2o-benchmark/N_1e8_K_1e2_single.csv",
        "s3://coiled-datasets/h2o-benchmark/N_1e9_K_1e2_single.csv",
    ],
    ids=["0.5 GB", "5 GB", "50 GB"],
)
def ddf(request):
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


def test_q1(ddf):
    ddf.groupby("id1", dropna=False, observed=True).agg({"v1": "sum"}).compute()


def test_q2(ddf):
    (
        ddf.groupby(["id1", "id2"], dropna=False, observed=True)
        .agg({"v1": "sum"})
        .compute()
    )


def test_q3(ddf):
    (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "sum", "v3": "mean"})
        .compute()
    )


def test_q4(ddf):
    (
        ddf.groupby("id4", dropna=False, observed=True)
        .agg({"v1": "mean", "v2": "mean", "v3": "mean"})
        .compute()
    )


def test_q5(ddf):
    (
        ddf.groupby("id6", dropna=False, observed=True)
        .agg({"v1": "sum", "v2": "sum", "v3": "sum"})
        .compute()
    )


def test_q7(ddf):
    (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "max", "v2": "min"})
        .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["range_v1_v2"]]
        .compute()
    )


def test_q8(ddf):
    (
        ddf[~ddf["v3"].isna()][["id6", "v3"]]
        .groupby("id6", dropna=False, observed=True)
        .apply(
            lambda x: x.nlargest(2, columns="v3"),
            meta={"id6": "Int64", "v3": "float64"},
        )[["v3"]]
        .compute()
    )


def test_q9(ddf):
    (
        ddf[["id2", "id4", "v1", "v2"]]
        .groupby(["id2", "id4"], dropna=False, observed=True)
        .apply(
            lambda x: pd.Series({"r2": x.corr()["v1"]["v2"] ** 2}),
            meta={"r2": "float64"},
        )
        .compute()
    )
