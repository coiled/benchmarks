"""
This example was adapted from https://github.com/dcherian/dask-demo/blob/main/nwm-aws.ipynb
"""

import dask
import flox.xarray
import numpy as np
import rioxarray
import xarray as xr


def test_nwm(
    s3_url,
    scale,
    client_factory,
    cluster_kwargs={
        "workspace": "dask-engineering",
        "region": "us-east-1",
        "wait_for_workers": True,
    },
    scale_n_workers={
        "small": 10,
        "large": 200,
    },
):
    with client_factory(
        n_workers=scale_n_workers[scale], **cluster_kwargs
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

        with dask.annotate(retries=3):
            counties = rioxarray.open_rasterio(
                "s3://nwm-250m-us-counties/Counties_on_250m_grid.tif",
                chunks="auto",
            ).squeeze()

        # Remove any small floating point error in coordinate locations
        _, counties_aligned = xr.align(subset, counties, join="override")

        counties_aligned = counties_aligned.persist()

        county_id = np.unique(counties_aligned.data).compute()
        county_id = county_id[county_id != 0]
        print(f"There are {len(county_id)} counties!")

        county_mean = flox.xarray.xarray_reduce(
            subset,
            counties_aligned.rename("county"),
            func="mean",
            expected_groups=(county_id,),
        )

        county_mean.load()
        yearly_mean = county_mean.mean("time")
        # Save dataset for further analysis
        yearly_mean.to_netcdf(s3_url)
