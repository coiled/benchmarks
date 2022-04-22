import uuid

import coiled.v2
import dask
from distributed import Client


def test_deadlock(cluster_kwargs):
    # See https://github.com/dask/distributed/issues/6110

    with coiled.v2.Cluster(
        name=f"test_deadlock-{uuid.uuid4().hex}",
        n_workers=20,
        worker_vm_types=["t3.medium"],
        **cluster_kwargs,
    ) as cluster:
        with Client(cluster) as client:
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

            i = 1
            while True:
                print(f"Iteration {i}")
                client.restart()
                (ddf.x + ddf.y).mean().compute()

                ddf3 = ddf.merge(ddf2)
                (ddf3.x + ddf3.y).mean().compute()
                i += 1
