from typing import Literal

import flox.xarray
import fsspec
import numpy as np
import rioxarray
import xarray as xr


def nwm(
    scale: Literal["small", "medium", "large"],
) -> xr.DataArray:
    ds = xr.open_zarr(
        "s3://noaa-nwm-retrospective-2-1-zarr-pds/rtout.zarr", consolidated=True
    )

    if scale == "small":
        # 6.03 TiB
        time_range = slice("2020-01-01", "2020-12-31")
    else:
        # 252.30 TiB
        time_range = slice("1979-02-01", "2020-12-31")
    subset = ds.zwattablrt.sel(time=time_range)

    counties = rioxarray.open_rasterio(
        "s3://nwm-250m-us-counties/Counties_on_250m_grid.tif",
        chunks="auto",
        opener=fsspec.open,
    ).squeeze()

    # Remove any small floating point error in coordinate locations
    _, counties_aligned = xr.align(subset, counties, join="override")
    counties_aligned = counties_aligned.persist()

    county_id = np.unique(counties_aligned.data).compute()
    county_id = county_id[county_id != 0]
    county_mean = flox.xarray.xarray_reduce(
        subset,
        counties_aligned.rename("county"),
        func="mean",
        expected_groups=(county_id,),
    )
    return county_mean
