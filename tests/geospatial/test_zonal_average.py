"""
This example was adapted from https://github.com/dcherian/dask-demo/blob/main/nwm-aws.ipynb
"""

from tests.geospatial.workloads.zonal_average import nwm


def test_nwm(
    s3,
    scale,
    client_factory,
    cluster_kwargs={
        "workspace": "dask-benchmarks",
        "region": "us-east-1",
    },
    scale_kwargs={
        "small": {"n_workers": 10},
        "large": {"n_workers": 200, "scheduler_memory": "32 GiB"},
    },
):
    with client_factory(
        **scale_kwargs[scale], **cluster_kwargs
    ) as client:  # noqa: F841
        result = nwm(scale=scale, s3fs=s3)
        result.compute()
