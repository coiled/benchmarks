from coiled.credentials.google import CoiledShippedCredentials

from tests.geospatial.workloads.regridding import xesmf


def test_xesmf(
    gcs_url,
    scale,
    setup_benchmark,
    cluster_kwargs={
        "workspace": "dask-benchmarks-gcp",
        "region": "us-central1",
        "wait_for_workers": True,
    },
    scale_kwargs={
        "small": {"n_workers": 10},
        "medium": {"n_workers": 10},
        "large": {"n_workers": 10},
    },
):
    with setup_benchmark(
        **scale_kwargs[scale], **cluster_kwargs
    ) as benchmark:  # noqa: F841
        benchmark(
            xesmf,
            scale=scale,
            storage_url=gcs_url,
            storage_options={"token": CoiledShippedCredentials()},
        )
