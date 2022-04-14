import dask


def test_shuffle_simple(small_client):
    df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-12-31", freq="1s", partition_freq="1D"
    )

    sdf = df.shuffle(on="x")

    sdf_size = sdf.memory_usage(index=True).sum() / (1024.0**3)
    assert sdf_size.compute() > 1.0  # 1.0 GB
