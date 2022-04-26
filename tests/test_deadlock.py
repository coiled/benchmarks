import uuid
from time import sleep

import coiled.v2
import dask
from distributed import Client, wait
from distributed.metrics import time


def test_repeated_merge_spill():
    # See https://github.com/dask/distributed/issues/6110

    with coiled.v2.Cluster(
        name=f"test_deadlock-{uuid.uuid4().hex}",
        n_workers=20,
        worker_vm_types=["t3.medium"],
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
            INTERVAL = 1

            for i in range(10):
                client.restart()
                fs = client.compute((ddf.x + ddf.y).mean())

                print(f"Iteration {i} Section 1")
                start = time()

                while fs.status == "pending" and time() - start < 2 * 60:
                    # We'd like to have seen all workers in the last INTERVAL seconds
                    workers = list(client.scheduler_info()["workers"].values())
                    assert all(w["last_seen"] - time() < INTERVAL for w in workers)
                    sleep(INTERVAL)

                wait(fs, timeout=0.1)
                del fs

                ddf3 = ddf.merge(ddf2)
                fs = client.compute((ddf3.x + ddf3.y).mean())

                print(f"Iteration {i} Section 2")
                start = time()

                while fs.status == "pending" and time() - start < 2 * 60:
                    # We'd like to have seen all workers in the last INTERVAL seconds
                    workers = list(client.scheduler_info()["workers"].values())
                    assert all(w["last_seen"] - time() < INTERVAL for w in workers)
                    sleep(INTERVAL)

                wait(fs, timeout=0.1)
                del fs

                i += 1
