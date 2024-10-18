import datetime
import os

import fsspec
import geojson
import odc.stac
import planetary_computer
import pystac_client
import xarray as xr


def harmonize_to_old(data: xr.Dataset) -> xr.Dataset:
    """
    Harmonize new Sentinel-2 data to the old baseline.

    Parameters
    ----------
    data:
        A Dataset with various bands as data variables and three dimensions: time, y, x

    Returns
    -------
    harmonized: xarray.Dataset
        A Dataset with all values harmonized to the old
        processing baseline.
    """
    cutoff = datetime.datetime(2022, 1, 25)
    offset = 1000
    bands = [
        "B01",
        "B02",
        "B03",
        "B04",
        "B05",
        "B06",
        "B07",
        "B08",
        "B8A",
        "B09",
        "B10",
        "B11",
        "B12",
    ]

    to_process = list(set(bands) & set(list(data.data_vars)))
    old = data.sel(time=slice(cutoff))[to_process]

    new = data.sel(time=slice(cutoff, None)).drop_vars(to_process)

    new_harmonized = data.sel(time=slice(cutoff, None))[to_process].clip(offset)
    new_harmonized -= offset

    new = xr.merge([new, new_harmonized])
    return xr.concat([old, new], dim="time")


def test_satellite_filtering(
    az_url,
    scale,
    client_factory,
    cluster_kwargs={
        "workspace": "dask-benchmarks-azure",
        "region": "westeurope",
        "wait_for_workers": True,
    },
    scale_kwargs={
        "small": {"n_workers": 10},
        "large": {"n_workers": 100},
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
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )

        # GeoJSON for region of interest is from https://github.com/isellsoap/deutschlandGeoJSON/tree/main/1_deutschland
        with fsspec.open(
            "https://raw.githubusercontent.com/isellsoap/deutschlandGeoJSON/main/1_deutschland/3_mittel.geo.json"
        ) as f:
            gj = geojson.load(f)

        # Flatten MultiPolygon to single Polygon
        coordinates = []
        for x in gj.features[0]["geometry"]["coordinates"]:
            coordinates.extend(x)
        area_of_interest = {
            "type": "Polygon",
            "coordinates": coordinates,
        }

        # Get stack items
        if scale == "small":
            time_of_interest = "2024-01-01/2024-09-01"
        else:
            time_of_interest = "2015-01-01/2024-09-01"

        search = catalog.search(
            collections=["sentinel-2-l2a"],
            intersects=area_of_interest,
            datetime=time_of_interest,
        )
        items = search.item_collection()

        # Construct Xarray Dataset from stack items
        ds = odc.stac.load(
            items,
            chunks={},
            patch_url=planetary_computer.sign,
            resolution=40,
            crs="EPSG:3857",
            groupby="solar_day",
        )
        # See https://planetarycomputer.microsoft.com/dataset/sentinel-2-l2a#Baseline-Change
        ds = harmonize_to_old(ds)

        # Compute humidity index
        humidity = (ds.B08 - ds.B11) / (ds.B08 + ds.B11)

        result = humidity.groupby("time.month").mean()
        result.to_zarr(az_url)
