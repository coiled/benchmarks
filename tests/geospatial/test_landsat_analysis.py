import collections
import datetime
import os

import numpy as np
import odc.stac
import planetary_computer
import pystac_client
import xarray as xr


def test_landsat_data_preparation(
    az_url,
    scale,
    client_factory,
    cluster_kwargs={
        "workspace": "dask-benchmarks-azure",
        "region": "westeurope",
        "wait_for_workers": True,
        "worker_cpu": 2,
        "worker_memory": "16GiB",
    },
    scale_kwargs={
        "small": {"n_workers": 20},
        "large": {"n_workers": 150},
    },
):
    with client_factory(
        **scale_kwargs[scale],
        env={
            "AZURE_STORAGE_ACCOUNT_NAME": os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
            "AZURE_STORAGE_SAS_TOKEN": os.environ["AZURE_STORAGE_SAS_TOKEN"],
        },
        **cluster_kwargs,
    ) as client:  # noqa: F841
        # Define our STAC endpoint (Microsoft Planetary Computer Catalogue)
        stac_url = "https://planetarycomputer.microsoft.com/api/stac/v1/"
        catalog = pystac_client.Client.open(
            stac_url,
            modifier=planetary_computer.sign_inplace,
        )

        if scale == "small":
            bands = ["cloud_qa", "red", "green", "blue"]
            box = (
                10.305842676154782,
                3.12634712296393502,
                14.497958439404897,
                5.052677334304235,
            )
        elif scale == "large":
            bands = [
                "cloud_qa",
                "red",
                "green",
                "blue",
                "swir16",
                "swir22",
                "nir08",
                "lwir11",
            ]
            box = (
                7.305842676154782,
                0.12634712296393502,
                14.497958439404897,
                5.052677334304235,
            )

        chunk_size = 2250
        geobox = odc.geo.geobox.GeoBox.from_bbox(
            box, crs="WGS 84", resolution=0.0003
        ).to_crs("EPSG:6933")
        start_time = datetime.datetime(year=2012, month=1, day=1)
        end_time = datetime.datetime(year=2013, month=10, day=30)

        search = catalog.search(
            collections=["landsat-c2-l2"], bbox=box, datetime=[start_time, end_time]
        )
        items = search.item_collection()

        grouped_items = collections.defaultdict(list)
        for item in items:
            grouped_items[item.id.split("_")[2]].append(item)

        tile_composites = []
        for i, (tile, items) in enumerate(grouped_items.items()):
            if scale == "small" and i >= 10:
                continue

            ds = odc.stac.load(
                items,
                chunks={},
                bands=bands,
                crs="EPSG:6933",
                resolution=30,
                fail_on_error=False,
            )

            fmask = ds["cloud_qa"].astype("uint16")
            ds = ds.drop_vars("cloud_qa")
            bitmask = 28
            bad = fmask & bitmask
            ds = ds.where(bad == 0)

            count = ds["red"].count(dim="time").astype("float32")
            count = count.where(count != 0, np.nan)

            composite = (
                ds.chunk({"time": -1, "x": "auto", "y": "auto"}).median(dim="time")
                # Use quantile to make things fall apart
                # .quantile(.75, dim="time")
                .odc.assign_crs(ds.odc.crs)
            )
            composite["count"] = count

            ds = odc.geo.xr.xr_reproject(
                composite, how=geobox, dst_nodata=np.nan
            ).chunk({"x": chunk_size, "y": chunk_size})
            tile_composites.append(ds)

        ds = xr.concat(tile_composites, dim="tile").mean(dim="tile")

        for band in ["red", "green", "blue"]:
            ds[f"{band}_texture"] = (
                ds[band].rolling(x=5, y=5, center=True, min_periods=1).std()
            )
        result = ds.chunk({"x": chunk_size, "y": chunk_size})
        result.to_zarr(az_url)
