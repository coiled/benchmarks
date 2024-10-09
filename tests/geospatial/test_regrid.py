import numpy as np
import xarray as xr
import xesmf as xe
from coiled.credentials.google import CoiledShippedCredentials


def test_xesmf(
    gcs_url,
    scale,
    client_factory,
    cluster_kwargs={
        "workspace": "dask-engineering-gcp",
        "region": "us-central1",
        "wait_for_workers": True,
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

        # 240x121
        out_grid = xr.Dataset(
            {
                "latitude": (
                    ["latitude"],
                    np.arange(90, -91.5, -1.5),
                    {"units": "degrees_north"},
                ),
                "longitude": (
                    ["longitude"],
                    np.arange(0, 360, 1.5),
                    {"units": "degrees_east"},
                ),
            }
        )
        regridder = xe.Regridder(ds, out_grid, "bilinear", periodic=True)
        regridded = regridder(ds, keep_attrs=True)

        # 144 MiB chunks for variables with level dimension since there is a single level chunk
        result = regridded.chunk(time=100)
        result.to_zarr(gcs_url, storage_options={"token": CoiledShippedCredentials()})
