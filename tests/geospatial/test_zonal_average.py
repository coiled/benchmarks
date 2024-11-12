"""
This example was adapted from https://github.com/dcherian/dask-demo/blob/main/nwm-aws.ipynb
"""

import pytest

from tests.geospatial.workloads.zonal_average import nwm


def test_nwm(
    scale,
    benchmark_type,
    setup_benchmark,
    cluster_kwargs={
        "workspace": "dask-benchmarks",
        "region": "us-east-1",
    },
    scale_kwargs={
        "small": {"n_workers": 10},
        "large": {"n_workers": 200, "scheduler_memory": "32 GiB"},
    },
):
    if benchmark_type == "submission":
        pytest.skip(
            reason="FIXME: Submission requires pre-computations, but no workers were requested."
        )
    with setup_benchmark(
        **scale_kwargs[scale], **cluster_kwargs
    ) as benchmark:  # noqa: F841
        benchmark(nwm, scale=scale)
