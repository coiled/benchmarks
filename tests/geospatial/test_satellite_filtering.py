import os

from tests.geospatial.workloads.satellite_filtering import satellite_filtering


def test_satellite_filtering(
    az_url,
    scale,
    client_factory,
    cluster_kwargs={
        "workspace": "dask-benchmarks-azure",
        "region": "westeurope",
    },
    scale_kwargs={
        "small": {"n_workers": 10},
        "large": {"n_workers": 100},
    },
):
    with client_factory(
        **scale_kwargs[scale],
        env={
            "AZURE_STORAGE_ACCOUNT_NAME": os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
            "AZURE_STORAGE_SAS_TOKEN": os.environ["AZURE_STORAGE_SAS_TOKEN"],
        },
        **cluster_kwargs,
    ) as client:  # noqa: F841
        result = satellite_filtering(scale=scale, storage_url=az_url)
        result.compute()
