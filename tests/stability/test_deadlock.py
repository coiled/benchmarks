import uuid

import coiled.v2
import dask
import pytest
from distributed import Client, wait


# @pytest.mark.skipif(
#     Version(distributed.__version__) < Version("2022.4.2"),
#     reason="https://github.com/dask/distributed/issues/6110",
# )
@pytest.mark.skip(
    reason="Skip until https://github.com/dask/distributed/pull/6637 is merged"
)
def test_repeated_merge_spill(upload_cluster_dump, benchmark_time):
    with coiled.v2.Cluster(
        name=f"test_deadlock-{uuid.uuid4().hex}",
        n_workers=20,
        worker_vm_types=["t3.large"],
        scheduler_vm_types=["t3.large"],
        package_sync=True,
    ) as cluster:
        with Client(cluster) as client:
            with upload_cluster_dump(client, cluster), benchmark_time:
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
