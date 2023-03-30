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
        **cluster_kwargs["embarrassingly_parallel_cluster"],
    ) as cluster:
        yield cluster


@pytest.fixture
def rechunk_client(
    rechunk_cluster,
    cluster_kwargs,
    upload_cluster_dump,
    benchmark_all,
):
    n_workers = cluster_kwargs["embarrassingly_parallel_cluster"]["n_workers"]
    with Client(rechunk_cluster) as client:
        rechunk_cluster.scale(n_workers)
        client.wait_for_workers(n_workers)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


def test_rechunk(rechunk_client, s3_url):
    x = da.random.random((5_000, 5_000, 5_000, 2), chunks="50 MB")  # 1.82 TiB
    y = x.rechunk("200 MB")
    y.to_zarr(s3_url)
