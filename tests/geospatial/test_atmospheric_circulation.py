from coiled.credentials.google import CoiledShippedCredentials

from tests.geospatial.workloads.atmospheric_circulation import atmospheric_circulation


def test_atmospheric_circulation(
    gcs_url,
    scale,
    client_factory,
    cluster_kwargs={
        "workspace": "dask-benchmarks-gcp",
        "region": "us-central1",
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
        result = atmospheric_circulation(
            scale=scale,
            storage_url=gcs_url,
            storage_options={"token": CoiledShippedCredentials()},
        )
        result.compute()
