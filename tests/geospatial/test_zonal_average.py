"""
This example was adapted from https://github.com/dcherian/dask-demo/blob/main/nwm-aws.ipynb
"""

import flox.xarray
import numpy as np
import rioxarray
import xarray as xr


def test_nwm(
    s3,
    scale,
    client_factory,
    cluster_kwargs={
        "workspace": "dask-benchmarks",
        "region": "us-east-1",
    },
    scale_kwargs={
        "small": {"n_workers": 10},
        "large": {"n_workers": 200, "scheduler_memory": "32 GiB"},
    },
):
    with client_factory(
        **scale_kwargs[scale], **cluster_kwargs
    ) as client:  # noqa: F841
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
            opener=s3.open,
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

        county_mean.compute()
