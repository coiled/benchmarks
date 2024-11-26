from typing import Literal

import xarray as xr
from s3fs import S3FileSystem


def cloud_optimize(
    scale: Literal["small", "medium", "large"], fs: S3FileSystem, storage_url: str
):
    models = [
        "ACCESS-CM2",
        "ACCESS-ESM1-5",
        "CMCC-ESM2",
        "CNRM-CM6-1",
        "CNRM-ESM2-1",
        "CanESM5",
        "EC-Earth3",
        "EC-Earth3-Veg-LR",
        "FGOALS-g3",
        "GFDL-ESM4",
        "GISS-E2-1-G",
        "INM-CM4-8",
        "INM-CM5-0",
        "KACE-1-0-G",
        "MIROC-ES2L",
        "MPI-ESM1-2-HR",
        "MPI-ESM1-2-LR",
        "MRI-ESM2-0",
        "NorESM2-LM",
        "NorESM2-MM",
        "TaiESM1",
        "UKESM1-0-LL",
    ]
    variables = [
        "hurs",
        "huss",
        "pr",
        "rlds",
        "rsds",
        "sfcWind",
        "tas",
        "tasmax",
        "tasmin",
    ]

    if scale == "small":
        # 130 files (152.83 GiB). One model and one variable.
        models = models[:1]
        variables = variables[:1]
    elif scale == "medium":
        # 390 files. Two models and two variables.
        # Currently fails after hitting 20 minute idle timeout
        # sending large graph to the scheduler.
        models = models[:2]
        variables = variables[:2]
    else:
        # 11635 files. All models and variables.
        pass

    # Get netCDF data files -- see https://registry.opendata.aws/nex-gddp-cmip6
    # for dataset details.
    files = []
    for model in models:
        for variable in variables:
            data_dir = f"s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/{model}/historical/r1i1p1f1/{variable}/*.nc"
            files += [f"s3://{path}" for path in fs.glob(data_dir)]
    print(f"Processing {len(files)} NetCDF files")

    # Load input NetCDF data files
    # TODO: Reduce explicit settings once https://github.com/pydata/xarray/issues/8778 is completed.
    ds = xr.open_mfdataset(
        files,
        engine="h5netcdf",
        combine="nested",
        concat_dim="time",
        data_vars="minimal",
        coords="minimal",
        compat="override",
        parallel=True,
    )

    # Rechunk from "pancake" to "pencil" format
    ds = ds.chunk({"time": -1, "lon": "auto", "lat": "auto"})

    # Write out to a Zar dataset
    return ds.to_zarr(storage_url, compute=False)
