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
        # Load dataset
        ds = xr.open_zarr(
            "gs://weatherbench2/datasets/era5/1959-2023_01_10-wb13-6h-1440x721.zarr",
        )

        if scale == "small":
            # 101.83 GiB (small)
            time_range = slice("2020-01-01", "2022-12-31")
            variables = ["sea_surface_temperature"]
        elif scale == "medium":
            # 2.12 TiB (medium)
            time_range = slice("1959-01-01", "2022-12-31")
            variables = ["sea_surface_temperature"]
        else:
            # 4.24 TiB (large)
            # This currently doesn't complete successfully.
            time_range = slice("1959-01-01", "2022-12-31")
            variables = ["sea_surface_temperature", "snow_depth"]
        ds = ds[variables].sel(time=time_range)
        original_chunks = ds.chunks

        ds = ds.drop_vars([k for k, v in ds.items() if "time" not in v.dims])
        pencil_chunks = {"time": -1, "longitude": "auto", "latitude": "auto"}

        working = ds.chunk(pencil_chunks)
        hours = xr.DataArray(range(0, 24, 6), dims=["hour"])
        daysofyear = xr.DataArray(range(1, 367), dims=["dayofyear"])
        template = (
            working.isel(time=0)
            .drop_vars("time")
            .expand_dims(hour=hours, dayofyear=daysofyear)
            .assign_coords(hour=hours, dayofyear=daysofyear)
        )
        working = working.map_blocks(compute_hourly_climatology, template=template)

        pancake_chunks = {
            "hour": 1,
            "dayofyear": 1,
            "latitude": original_chunks["latitude"],
            "longitude": original_chunks["longitude"],
        }
        result = working.chunk(pancake_chunks)
        result.to_zarr(gcs_url, storage_options={"token": CoiledShippedCredentials()})


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
        # Load dataset
        ds = xr.open_zarr(
            "gs://weatherbench2/datasets/era5/1959-2023_01_10-wb13-6h-1440x721.zarr",
        )

        if scale == "small":
            # 101.83 GiB (small)
            time_range = slice("2020-01-01", "2022-12-31")
            variables = ["sea_surface_temperature"]
        elif scale == "medium":
            # 2.12 TiB (medium)
            time_range = slice("1959-01-01", "2022-12-31")
            variables = ["sea_surface_temperature"]
        else:
            # 4.24 TiB (large)
            # This currently doesn't complete successfully.
            time_range = slice("1959-01-01", "2022-12-31")
            variables = ["sea_surface_temperature", "snow_depth"]
        ds = ds[variables].sel(time=time_range)
        original_chunks = ds.chunks

        # Drop all static variables
        ds = ds.drop_vars([k for k, v in ds.items() if "time" not in v.dims])

        # Split time dimension into three dimensions
        ds["dayofyear"] = ds.time.dt.dayofyear
        ds["hour"] = ds.time.dt.hour
        ds["year"] = ds.time.dt.year
        ds = ds.set_index(time=["year", "dayofyear", "hour"]).unstack()

        # Fill empty values for non-leap years
        ds = ds.ffill(dim="dayofyear", limit=1)

        # Calculate climatology
        window_size = 61
        window_weights = create_window_weights(window_size)
        half_window_size = window_size // 2
        ds = ds.pad(pad_width={"dayofyear": half_window_size}, mode="wrap")
        # FIXME: https://github.com/pydata/xarray/issues/9550
        ds = ds.chunk(latitude=128, longitude=128)
        ds = ds.rolling(dayofyear=window_size, center=True).construct("window")
        ds = ds.weighted(window_weights).mean(dim=("window", "year"))
        ds = ds.isel(dayofyear=slice(half_window_size, -half_window_size))

        pancake_chunks = {
            "hour": 1,
            "dayofyear": 1,
            "latitude": original_chunks["latitude"],
            "longitude": original_chunks["longitude"],
        }
        result = ds.chunk(pancake_chunks)
        result.to_zarr(gcs_url, storage_options={"token": CoiledShippedCredentials()})
