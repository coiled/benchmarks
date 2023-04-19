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


def test_exploratory_analysis(uber_lyft_client):
    """Run some exploratory aggs on the dataset"""

    # NYC taxi Uber/Lyft dataset
    df = dd.read_parquet(
        "s3://coiled-datasets/uber-lyft-tlc/", storage_options={"anon": True}
    )

    # Preprocessing:
    #   - Add a column to indicate company, instead of license number
    #   - Add a column to indicate if a tip was given
    taxi_companies = {
        "HV0002": "Juno",
        "HV0003": "Uber",
        "HV0004": "Via",
        "HV0005": "Lyft",
    }
    df["company"] = df.hvfhs_license_num.replace(taxi_companies)
    df["tipped"] = df.tips > 0

    # Persist so we only read once
    df = df.persist()

    # How many riders tip?
    df.tipped.mean().compute()
    # How many riders tip for each company?
    df.groupby("company").tipped.value_counts().compute()
    # What are those as percentages?
    df.groupby("company").tipped.mean().compute()
