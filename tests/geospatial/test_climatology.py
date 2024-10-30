"""This benchmark is a port of the climatology computation implemented in
https://github.com/google-research/weatherbench2/blob/47d72575cf5e99383a09bed19ba989b718d5fe30/scripts/compute_climatology.py
with the parameters

FREQUENCY = "hourly"
HOUR_INTERVAL = 6
WINDOW_SIZE = 61
STATISTICS = ["mean"]
METHOD = "explicit"
"""

import numpy as np
import xarray as xr
from coiled.credentials.google import CoiledShippedCredentials

from tests.geospatial.workloads.climatology import highlevel_api, rechunk_map_blocks


def compute_hourly_climatology(
    ds: xr.Dataset,
) -> xr.Dataset:
    hours = xr.DataArray(range(0, 24, 6), dims=["hour"])
    window_weights = create_window_weights(61)
    return xr.concat(
        [compute_rolling_mean(select_hour(ds, hour), window_weights) for hour in hours],
        dim=hours,
    )


def compute_rolling_mean(ds: xr.Dataset, window_weights: xr.DataArray) -> xr.Dataset:
    window_size = len(window_weights)
    half_window_size = window_size // 2  # For padding
    ds = xr.concat(
        [
            replace_time_with_doy(ds.sel(time=str(y)))
            for y in np.unique(ds.time.dt.year)
        ],
        dim="year",
    )
    ds = ds.fillna(ds.sel(dayofyear=365))
    ds = ds.pad(pad_width={"dayofyear": half_window_size}, mode="wrap")
    ds = ds.rolling(dayofyear=window_size, center=True).construct("window")
    ds = ds.weighted(window_weights).mean(dim=("window", "year"))
    return ds.isel(dayofyear=slice(half_window_size, -half_window_size))


def create_window_weights(window_size: int) -> xr.DataArray:
    """Create linearly decaying window weights."""
    assert window_size % 2 == 1, "Window size must be odd."
    half_window_size = window_size // 2
    window_weights = np.concatenate(
        [
            np.linspace(0, 1, half_window_size + 1),
            np.linspace(1, 0, half_window_size + 1)[1:],
        ]
    )
    window_weights = window_weights / window_weights.mean()
    window_weights = xr.DataArray(window_weights, dims=["window"])
    return window_weights


def replace_time_with_doy(ds: xr.Dataset) -> xr.Dataset:
    """Replace time coordinate with days of year."""
    return ds.assign_coords({"time": ds.time.dt.dayofyear}).rename(
        {"time": "dayofyear"}
    )


def select_hour(ds: xr.Dataset, hour: int) -> xr.Dataset:
    """Select given hour of day from dataset."""
    # Select hour
    ds = ds.isel(time=ds.time.dt.hour == hour)
    # Adjust time dimension
    ds = ds.assign_coords({"time": ds.time.astype("datetime64[D]")})
    return ds


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
