import uuid

import coiled.v2
import dask
from distributed import Client


def test_deadlock(software):
    # See https://github.com/dask/distributed/issues/6110

    with coiled.v2.Cluster(
        name=f"test_deadlock-{uuid.uuid4().hex}",
        n_workers=20,
        account="dask-engineering",
        software=software,
        # software="dask-engineering/florian-deadlock",
        # environ=dict(
        #     DASK_LOGGING__DISTRIBUTED="debug",
        #     DASK_DISTRIBUTED__ADMIN__LOG_LENGTH="1000000"
        # )
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

            def slowident(df):
                import random
                import time

                time.sleep(random.randint(1, 5))
                return df

            i = 1
            while True:
                print(f"Iteration {i}")
                client.restart()
                demo1 = ddf.map_partitions(slowident)
                (demo1.x + demo1.y).mean().compute()

                demo2 = ddf.merge(ddf2)
                demo2 = demo2.map_partitions(slowident)
                (demo2.x + demo2.y).mean().compute()
                i += 1
