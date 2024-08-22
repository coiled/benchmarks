import pytest
import coiled
import dask
import xarray as xr
from coiled.credentials.google import CoiledShippedCredentials


@pytest.mark.client("era5_rechunking")
def test_era5_rechunking(client):
    # # Create cluster
    # cluster = coiled.Cluster(
    #     n_workers=100,
    #     workspace="dask-engineering-gcp",
    #     region="us-central1",
    # )
    # client = cluster.get_client()
    # with benchmark_all(client):

    # Load dataset
    ds = xr.open_zarr(
        "gs://weatherbench2/datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr",
    ).drop_encoding()

    time_range = slice("2020-01-01", "2023-01-01")  # 101.83 GiB
    subset = ds.sea_surface_temperature.sel(time=time_range)

    # Rechunk
    with dask.config.set({"array.rechunk.method": "p2p"}): # Use new algorithm
        result = subset.chunk({"time": -1, "longitude": "auto", "latitude": "auto"})

    # Write result to cloud storage
    result.to_zarr(
        "gs://coiled-scratch-space/benchmarks/era5.zarr/",
        storage_options={"token": CoiledShippedCredentials()},
    )