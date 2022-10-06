import uuid

import dask
import pytest
from coiled import Cluster
from distributed import Client, wait


# @pytest.mark.skipif(
#     Version(distributed.__version__) < Version("2022.4.2"),
#     reason="https://github.com/dask/distributed/issues/6110",
# )
@pytest.mark.skip(
    reason="Skip until https://github.com/dask/distributed/pull/6637 is merged"
)
def test_repeated_merge_spill(
    upload_cluster_dump, benchmark_all, get_cluster_info, dask_env_variables
):
    with Cluster(
        name=f"test_deadlock-{uuid.uuid4().hex}",
        n_workers=20,
        worker_vm_types=["t3.large"],
        scheduler_vm_types=["t3.xlarge"],
        wait_for_workers=True,
        package_sync=True,
        environ=dask_env_variables,
        backend_options={"send_prometheus_metrics": True},
    ) as cluster:
        with Client(cluster) as client:
            with upload_cluster_dump(client, cluster), benchmark_all(
                client
            ), get_cluster_info(client):
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
