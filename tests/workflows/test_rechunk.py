import uuid

import coiled
import dask.array as da
import pytest
from dask.distributed import Client


@pytest.fixture(scope="module")
def rechunk_cluster(
    dask_env_variables,
    cluster_kwargs,
    github_cluster_tags,
):
    with coiled.Cluster(
        f"test-rechunk-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **cluster_kwargs["rechunk_cluster"],
    ) as cluster:
        yield cluster


@pytest.fixture
def rechunk_client(
    rechunk_cluster,
    cluster_kwargs,
    upload_cluster_dump,
    benchmark_all,
):
    n_workers = cluster_kwargs["rechunk_cluster"]["n_workers"]
    with Client(rechunk_cluster) as client:
        rechunk_cluster.scale(n_workers)
        client.wait_for_workers(n_workers)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


def test_rechunk(rechunk_client, s3_url):
    # Dataset is 3.80 TiB (https://registry.opendata.aws/mur)
    x = da.from_zarr(
        "s3://mur-sst/zarr",
        component="sea_ice_fraction",
        storage_options={"anon": True, "client_kwargs": {"region_name": "us-west-2"}},
    )
    assert x.chunksize == (6443, 100, 100)  # 61.45 MiB (dtype = int8)
    y = x.rechunk((6443, 100, 200))  # 122.89 MiB
    y.to_zarr(s3_url)


def test_rechunk_full_shuffle(rechunk_client, s3_url):
    # Test a "full shuffle" rechunk where every input chunk goes into every output chunk
    # Dataset is 196.72 GiB
    x = da.from_zarr(
        "s3://yuvipanda-test1/cmr/gpm3imergdl.zarr/HQprecipitation",
        storage_options={"anon": True},
    )
    y = x.rechunk((-1, 36, 180))
    y.to_zarr(s3_url)
