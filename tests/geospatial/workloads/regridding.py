from typing import Any, Literal

import numpy as np
import xarray as xr
import xesmf as xe
from dask.delayed import Delayed


def xesmf(
    scale: Literal["small", "medium", "large"],
    storage_url: str,
    storage_options: dict[str, Any],
) -> Delayed:
    ds = xr.open_zarr(
        "gs://weatherbench2/datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr",
        chunks={"time": "auto"},
    )
    # Fixed time range and variable as the interesting part of this benchmark scales with the
    # regridding matrix
    ds = ds[["sea_surface_temperature"]].sel(time=slice("2020-01-01", "2021-12-31"))
    if scale == "small":
        # Regridding from a resolution of 0.25 degress to 1 degrees
        # results in 4 MiB weight matrix
        output_resolution = 1
    elif scale == "medium":
        # Regridding from a resolution of 0.25 degrees to 0.2 degrees
        # results in 100 MiB weight matrix
        output_resolution = 0.2
    else:
        # Regridding from a resolution of 0.25 degrees to 0.05 degrees
        # results in 1.55 GiB weight matrix
        output_resolution = 0.05

    out_grid = xr.Dataset(
        {
            "latitude": (
                ["latitude"],
                np.arange(90, -90 - output_resolution, -output_resolution),
                {"units": "degrees_north"},
            ),
            "longitude": (
                ["longitude"],
                np.arange(0, 360, output_resolution),
                {"units": "degrees_east"},
            ),
        }
    )
    regridder = xe.Regridder(ds, out_grid, "bilinear", periodic=True)
    regridded = regridder(ds, keep_attrs=True)

    result = regridded.chunk(time="auto")
    return result.to_zarr(storage_url, storage_options=storage_options, compute=False)
