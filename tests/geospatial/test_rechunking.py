import pytest
import xarray as xr
from coiled.credentials.google import CoiledShippedCredentials


@pytest.mark.client("era5_rechunking")
def test_era5_rechunking(client, gcs_url):
    # Load dataset
    ds = xr.open_zarr(
        "gs://weatherbench2/datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr",
    ).drop_encoding()

    time_range = slice("2020-01-01", "2023-01-01")  # 101.83 GiB (small)
    # time_range = slice("2013-01-01", "2023-01-01")      # 339.09 GiB
    # time_range = slice("2003-01-01", "2023-01-01")      # 678.19 GiB (medium?)
    # time_range = slice("1990-01-01", "2023-01-01")      # 1.09 TiB
    # time_range = slice(None)                            # 2.12 TiB (large?)
    subset = ds.sea_surface_temperature.sel(time=time_range)

    # Rechunk
    result = subset.chunk({"time": -1, "longitude": "auto", "latitude": "auto"})

    # Write result to cloud storage
    result.to_zarr(gcs_url, storage_options={"token": CoiledShippedCredentials()})
