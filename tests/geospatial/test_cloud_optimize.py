from tests.geospatial.workloads.cloud_optimize import cloud_optimize


def test_cloud_optimize(
    scale,
    s3,
    s3_url,
    client_factory,
    cluster_kwargs={
        "workspace": "dask-benchmarks",
        "region": "us-west-2",
    },
    scale_kwargs={
        "small": {"n_workers": 10},
        "medium": {"n_workers": 100},
        "large": {"n_workers": 200},
    },
):
    with client_factory(
        **scale_kwargs[scale], **cluster_kwargs
    ) as client:  # noqa: F841
        result = cloud_optimize(scale, s3fs=s3, storage_url=s3_url)
        fut = client.compute(result, snyc=False)
        fut.result()
