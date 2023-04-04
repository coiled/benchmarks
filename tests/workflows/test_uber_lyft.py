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
    return dd.read_parquet("s3://coiled-datasets/uber-lyft-tlc/")


def test_explore_dataset(ddf):
    """Run some exploratory aggs on the dataset"""

    # how many riders tip, in general
    (ddf.tips != 0).mean().compute()

    taxi_companies = {
        "HV0002": "Juno",
        "HV0003": "Uber",
        "HV0004": "Via",
        "HV0005": "Lyft",
    }

    # add a column to indicate company, instead of license number
    ddf["company"] = ddf.hvfhs_license_num.replace(taxi_companies)

    # how many riders tip, grouped by company
    def over_zero(x):
        return x[x > 0].count() / x.count()

    ddf.groupby("company").tips.apply(over_zero, meta=("tips", float)).compute()

    # what's the largest pay that driver got, by company
    ddf.groupby("company").driver_pay.max().compute()

    # ride count per company
    ddf.groupby("company").count().compute()

    # how many passengers ride over 5 miles, per company
    def over_five(x):
        return x[x > 5].count() / x.count()

    ddf.groupby("company").trip_miles.apply(
        over_five, meta=("trip_miles", float)
    ).compute()
