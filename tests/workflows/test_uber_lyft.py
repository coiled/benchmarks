import uuid

import coiled
import dask.dataframe as dd
import pytest
from dask.distributed import Client


@pytest.fixture(scope="module")
def uber_lyft_cluster(
    dask_env_variables,
    cluster_kwargs,
    github_cluster_tags,
):
    with coiled.Cluster(
        f"uber-lyft-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **cluster_kwargs["uber_lyft_cluster"],
    ) as cluster:
        yield cluster


@pytest.fixture
def uber_lyft_client(
    uber_lyft_cluster,
    cluster_kwargs,
    upload_cluster_dump,
    benchmark_all,
):
    n_workers = cluster_kwargs["uber_lyft_cluster"]["n_workers"]
    with Client(uber_lyft_cluster) as client:
        uber_lyft_cluster.scale(n_workers)
        client.wait_for_workers(n_workers)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


@pytest.fixture
def ddf(uber_lyft_client):
    """NYC taxi Uber/Lyft dataset"""
    paths = [
        f"s3://coiled-datasets/mrocklin/nyc-taxi-fhv/{year}-*.parquet"
        for year in range(2019, 2023)
    ]
    return dd.read_parquet(paths, engine="pyarrow", storage_options={"anon": True})


def test_mean_tips(ddf):
    """How many passengers tip"""
    (ddf.tips != 0).mean().compute()


def test_max_pay(ddf):
    """Max driver pay"""
    ddf.driver_pay.max().compute()


def test_max_distance(ddf):
    """Ride count per rideshare company"""
    ddf.groupby("hvfhs_license_num").count().compute()


def test_longer_trips(ddf):
    """How many passengers ride over 5 miles"""
    (ddf.trip_miles > 5).mean().compute()
