"""This benchmark is a port of the climatology computation implemented in
https://github.com/google-research/weatherbench2/blob/47d72575cf5e99383a09bed19ba989b718d5fe30/scripts/compute_climatology.py
with the parameters

FREQUENCY = "hourly"
HOUR_INTERVAL = 6
WINDOW_SIZE = 61
STATISTICS = ["mean"]
METHOD = "explicit"
"""

from coiled.credentials.google import CoiledShippedCredentials

from tests.geospatial.workloads.climatology import highlevel_api, rechunk_map_blocks


def test_rechunk_map_blocks(
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
        result = rechunk_map_blocks(
            scale=scale,
            storage_url=gcs_url,
            storage_options={"token": CoiledShippedCredentials()},
        )
        result.compute()


def test_highlevel_api(
    gcs_url,
    scale,
    client_factory,
    cluster_kwargs={
        "workspace": "dask-benchmarks-gcp",
        "region": "us-central1",
        "idle_timeout": "1h",
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
        result = highlevel_api(
            scale=scale,
            storage_url=gcs_url,
            storage_options={"token": CoiledShippedCredentials()},
        )
        result.compute()
