from coiled.credentials.google import CoiledShippedCredentials

from tests.geospatial.workloads.rechunking import era5_rechunking


def test_era5_rechunking(
    gcs_url,
    scale,
    setup_benchmark,
    cluster_kwargs={
        "workspace": "dask-benchmarks-gcp",
        "region": "us-central1",
    },
    scale_kwargs={
        "small": {"n_workers": 50},
        "medium": {"n_workers": 100},
        "large": {"n_workers": 100},
    },
):
    with setup_benchmark(
        **scale_kwargs[scale], **cluster_kwargs
    ) as benchmark:  # noqa: F841
        benchmark(
            era5_rechunking,
            scale=scale,
            storage_url=gcs_url,
            storage_options={"token": CoiledShippedCredentials()},
        )
