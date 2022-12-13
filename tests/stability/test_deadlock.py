import uuid

import dask
import pytest
from coiled import Cluster
from distributed import Client, wait


def test_repeated_merge_spill(
    upload_cluster_dump,
    benchmark_all,
    cluster_kwargs,
    dask_env_variables,
    gitlab_cluster_tags,
):
    with Cluster(
        name=f"test_repeated_merge_spill-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=gitlab_cluster_tags,
        **cluster_kwargs["test_repeated_merge_spill"],
    ) as cluster:
        with Client(cluster) as client:
            with upload_cluster_dump(client), benchmark_all(client):
                ddf = dask.datasets.timeseries(
                    "2020",
                    "2025",
                    partition_freq="2w",
                )
                ddf2 = dask.datasets.timeseries(
                    "2020",
                    "2023",
                    partition_freq="2w",
                )

                for _ in range(10):
                    client.restart()
                    fs = client.compute((ddf.x + ddf.y).mean())

                    wait(fs, timeout=2 * 60)
                    del fs

                    ddf3 = ddf.merge(ddf2)
                    fs = client.compute((ddf3.x + ddf3.y).mean())

                    wait(fs, timeout=2 * 60)
                    del fs
