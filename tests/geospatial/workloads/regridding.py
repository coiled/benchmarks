from typing import Any, Literal

import numpy as np
import xarray as xr
import xesmf as xe
from dask.delayed import Delayed


def xesmf(
    scale: Literal["small", "medium", "large"],
    output_resolution: float,
    storage_url: str,
    storage_options: dict[str, Any],
) -> Delayed:
    ds = xr.open_zarr(
        "gs://weatherbench2/datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr",
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

    # 240x121
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
