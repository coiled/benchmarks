import xarray as xr


def test_cloud_optimize(
    scale,
    s3,
    s3_url,
    client_factory,
    cluster_kwargs={
        "workspace": "dask-engineering",
        "region": "us-west-2",
        "wait_for_workers": True,
    },
    scale_kwargs={
        "small": {"n_workers": 10},
        "medium": {"n_workers": 100},
        "large": {"n_workers": 200},
    },
):
    with client_factory(
        **scale_kwargs[scale], **cluster_kwargs
    ) as client:  # noqa: F841
        # Define models and variables of interest
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
            # 715 files. One model and all variables.
            # Currently fails after hitting 20 minute idle timeout
            # sending `to_zarr` graph to the scheduler.
            models = models[:1]
        else:
            # 11635 files. All models and variables.
            pass

        # Get netCDF data files -- see https://registry.opendata.aws/nex-gddp-cmip6
        # for dataset details.
        file_list = []
        for model in models:
            for variable in variables:
                data_dir = f"s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/{model}/historical/r1i1p1f1/{variable}/*.nc"
                file_list += [f"s3://{path}" for path in s3.glob(data_dir)]
        files = [s3.open(f) for f in file_list]
        print(f"Processing {len(files)} NetCDF files")

        # Load input NetCDF data files
        ds = xr.open_mfdataset(
            files,
            engine="h5netcdf",
            combine="nested",
            concat_dim="time",
            parallel=True,
        )

        # Rechunk from "pancake" to "pencil" format
        ds = ds.chunk({"time": -1, "lon": "auto", "lat": "auto"})

        # Write out to a Zar dataset
        ds.to_zarr(s3_url)
