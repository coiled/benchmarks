import dask


def test_deadlock(small_client):
    # See https://github.com/dask/distributed/issues/6110

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
        small_client.restart()
        demo1 = ddf.map_partitions(slowident)
        (demo1.x + demo1.y).mean().compute()

        demo2 = ddf.merge(ddf2)
        demo2 = demo2.map_partitions(slowident)
        (demo2.x + demo2.y).mean().compute()
        i += 1
