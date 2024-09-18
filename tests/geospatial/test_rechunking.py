import xarray as xr
from coiled.credentials.google import CoiledShippedCredentials


def test_era5_rechunking(
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
            "gs://weatherbench2/datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr",
        ).drop_encoding()

        if scale == "small":
            # 101.83 GiB (small)
            time_range = slice("2020-01-01", "2023-01-01")
            variables = ["sea_surface_temperature"]
        elif scale == "medium":
            # 2.12 TiB (medium)
            time_range = slice(None)
            variables = ["sea_surface_temperature"]
        else:
            # 4.24 TiB (large)
            # This currently doesn't complete successfully.
            time_range = slice(None)
            variables = ["sea_surface_temperature", "snow_depth"]
        subset = ds[variables].sel(time=time_range)

        # Rechunk
        result = subset.chunk({"time": -1, "longitude": "auto", "latitude": "auto"})

        # Write result to cloud storage
        result.to_zarr(gcs_url, storage_options={"token": CoiledShippedCredentials()})
