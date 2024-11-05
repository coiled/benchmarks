import xarray as xr


def load_era5() -> xr.Dataset:
    return xr.open_zarr(
        "gs://weatherbench2/datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr",
        chunks={
            "longitude": "auto",
            "latitude": "auto",
            "levels": "auto",
            "time": "auto",
        },
    )
