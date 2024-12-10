import os

import pytest

from tests.geospatial.workloads.satellite_filtering import satellite_filtering


def test_satellite_filtering(
    az_url,
    scale,
    setup_benchmark,
    cluster_kwargs={
        "workspace": "dask-benchmarks-azure",
        "region": "westeurope",
    },
    scale_kwargs={
        "small": {"n_workers": 50},
        "large": {"n_workers": 100},
    },
):
    if scale not in scale_kwargs.keys():
        pytest.skip(reason=f"{scale=} not implemented")
    with setup_benchmark(
        **scale_kwargs[scale],
        env={
            "AZURE_STORAGE_ACCOUNT_NAME": os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
            "AZURE_STORAGE_SAS_TOKEN": os.environ["AZURE_STORAGE_SAS_TOKEN"],
        },
        **cluster_kwargs,
    ) as benchmark:  # noqa: F841
        benchmark(satellite_filtering, scale=scale, storage_url=az_url)
