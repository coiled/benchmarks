import xarray as xr


def test_atmospheric_circulation(
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
        ds = xr.open_zarr(
            "gs://weatherbench2/datasets/era5/1959-2023_01_10-full_37-1h-0p25deg-chunk-1.zarr",
            chunks={},
        )
        if scale == "small":
            # 852.56 GiB (small)
            time_range = slice("2020-01-01", "2020-02-01")
        elif scale == "medium":
            # 28.54 TiB (medium)
            time_range = slice("2020-01-01", "2023-01-01")
        else:
            # 608.42 TiB (large)
            time_range = slice(None)
        ds = ds.sel(time=time_range)

        ds = ds[
            [
                "u_component_of_wind",
                "v_component_of_wind",
                "temperature",
                "vertical_velocity",
            ]
        ].rename(
            {
                "u_component_of_wind": "U",
                "v_component_of_wind": "V",
                "temperature": "T",
                "vertical_velocity": "W",
            }
        )

        zonal_means = ds.mean("longitude")
        anomaly = ds - zonal_means

        anomaly["uv"] = anomaly.U * anomaly.V
        anomaly["vt"] = anomaly.V * anomaly.T
        anomaly["uw"] = anomaly.U * anomaly.W

        temdiags = zonal_means.merge(anomaly[["uv", "vt", "uw"]].mean("longitude"))

        # This is incredibly slow, takes a while for flox to construct the graph
        # daily = temdiags.resample(time="D").mean()

        # Option 2: rechunk to make it a blockwise problem
        # we should do this automatically
        daily = (
            temdiags.chunk(time=xr.groupers.TimeResampler("D"))
            .resample(time="D")
            .mean()
        )

        daily.to_zarr(gcs_url)
