import pytest
from coiled.credentials.google import CoiledShippedCredentials

from tests.geospatial.workloads.regridding import xesmf


@pytest.mark.parametrize("output_resolution", [1.5, 0.1])
def test_xesmf(
    gcs_url,
    scale,
    client_factory,
    output_resolution,
    cluster_kwargs={
        "workspace": "dask-benchmarks-gcp",
        "region": "us-central1",
        "wait_for_workers": True,
    },
    scale_kwargs={
        "small": {"n_workers": 10},
        "medium": {"n_workers": 100},
        "large": {"n_workers": 100},
    },
):
    with client_factory(
        **scale_kwargs[scale], **cluster_kwargs
    ) as client:  # noqa: F841
        result = xesmf(
            scale=scale,
            output_resolution=output_resolution,
            storage_url=gcs_url,
            storage_options={"token": CoiledShippedCredentials()},
        )
        result.compute()
