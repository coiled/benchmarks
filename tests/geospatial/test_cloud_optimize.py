from tests.geospatial.workloads.cloud_optimize import cloud_optimize


def test_cloud_optimize(
    scale,
    s3,
    s3_url,
    setup_benchmark,
    cluster_kwargs={
        "workspace": "dask-benchmarks",
        "region": "us-west-2",
    },
    scale_kwargs={
        "small": {"n_workers": 50},
        "medium": {"n_workers": 100},
        "large": {"n_workers": 200},
    },
):
    with setup_benchmark(
        **scale_kwargs[scale], **cluster_kwargs
    ) as benchmark:  # noqa: F841
        benchmark(cloud_optimize, scale, fs=s3, storage_url=s3_url)
