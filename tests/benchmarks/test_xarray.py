import uuid

import dask.array as da
import fsspec
import numpy as np
import pytest
from coiled import Cluster
from dask.utils import parse_bytes
from distributed import Client

from tests.conftest import dump_cluster_kwargs

from ..utils_test import (
    cluster_memory,
    print_size_info,
    run_up_to_nthreads,
    scaled_array_shape,
    wait,
)

xr = pytest.importorskip("xarray")
pytest.importorskip("flox")


@pytest.fixture(scope="module")
def group_reduction_cluster(dask_env_variables, cluster_kwargs, github_cluster_tags):
    kwargs = dict(
        name=f"xarray-group-reduction-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **cluster_kwargs["group_reduction_cluster"],
    )
    dump_cluster_kwargs(kwargs, "group_reduction_cluster")
    with Cluster(**kwargs) as cluster:
        yield cluster


@pytest.fixture
def group_reduction_client(
    group_reduction_cluster, cluster_kwargs, benchmark_all, wait_for_workers
):
    n_workers = cluster_kwargs["group_reduction_cluster"]["n_workers"]
    with Client(group_reduction_cluster) as client:
        group_reduction_cluster.scale(n_workers)
        wait_for_workers(client, n_workers, timeout=600)
        client.restart()
        with benchmark_all(client):
            yield client


@pytest.mark.parametrize(
    "func",
    [
        pytest.param(
            lambda x: x.groupby("time.month").mean(method="cohorts"), id="cohorts"
        ),
        pytest.param(
            lambda x: x.groupby("time.month").mean(method="map-reduce"), id="map-reduce"
        ),
        pytest.param(
            lambda x: x.chunk(time=xr.groupers.TimeResampler("ME"))
            .groupby("time.month")
            .mean(method="cohorts"),
            id="chunked-cohorts",
        ),
    ],
)
def test_xarray_groupby_reduction(group_reduction_client, func):
    ds = xr.open_zarr(
        fsspec.get_mapper(
            "s3://noaa-nwm-retrospective-2-1-zarr-pds/rtout.zarr", anon=True
        ),
        consolidated=True,
    )
    # slice dataset properly to keep runtime in check
    subset = ds.zwattablrt.sel(time=slice("2001", "2002"))
    subset = subset.isel(x=slice(0, 350 * 8), y=slice(0, 350 * 8))
    result = func(subset)
    wait(result, group_reduction_client, 10 * 60)


@run_up_to_nthreads("small_cluster", 50, reason="fixed dataset")
@pytest.mark.parametrize("backend", ["dataframe", "array"])
def test_quadratic_mean(small_client, backend):
    # https://github.com/pangeo-data/distributed-array-examples/issues/2
    # See https://github.com/dask/dask/issues/10384
    size = 5000
    ds = xr.Dataset(
        dict(
            anom_u=(
                ["time", "face", "j", "i"],
                da.random.random((size, 1, 987, 1920), chunks=(10, 1, -1, -1)),
            ),
            anom_v=(
                ["time", "face", "j", "i"],
                da.random.random((size, 1, 987, 1920), chunks=(10, 1, -1, -1)),
            ),
        )
    )

    quad = ds**2
    quad["uv"] = ds.anom_u * ds.anom_v
    mean = quad.mean("time")
    if backend == "dataframe":
        mean = mean.to_dask_dataframe()

    wait(mean, small_client, 10 * 60)


def test_anom_mean(small_client, new_array):
    """From https://github.com/dask/distributed/issues/2602#issuecomment-498718651"""

    memory = cluster_memory(small_client)  # 76.66 GiB
    target_nbytes = memory // 2
    data = new_array(
        scaled_array_shape(target_nbytes, ("x", "10MiB")),
        chunks=(1, parse_bytes("10MiB") // 8),
    )
    print_size_info(memory, target_nbytes, data)
    # 38.32 GiB - 3925 10.00 MiB chunks

    ngroups = data.shape[0] // 100
    arr = xr.DataArray(
        data,
        dims=["time", "x"],
        coords={"day": ("time", np.arange(data.shape[0]) % ngroups)},
    )
    with xr.set_options(use_flox=False):
        clim = arr.groupby("day").mean(dim="time")
        anom = arr.groupby("day") - clim
        anom_mean = anom.mean(dim="time")

    wait(anom_mean, small_client, 10 * 60)


@pytest.mark.skip(
    "fails in actual CI; see https://github.com/coiled/benchmarks/issues/253"
)
def test_climatic_mean(small_client, new_array):
    """From https://github.com/dask/distributed/issues/2602#issuecomment-535009454"""

    memory = cluster_memory(small_client)  # 76.66 GiB
    target_nbytes = memory * 2
    chunks = (1, 1, 96, 21, 90, 144)
    shape = (28, "x", 96, 21, 90, 144)
    data = new_array(scaled_array_shape(target_nbytes, shape), chunks=chunks)
    print_size_info(memory, target_nbytes, data)
    # 152.62 GiB - 784 199.34 MiB chunks

    array = xr.DataArray(
        data,
        dims=["ensemble", "init_date", "lat", "lead_time", "level", "lon"],
        # coords={"init_date": pd.date_range(start="1960", periods=arr.shape[1])},
        coords={"init_date": np.arange(data.shape[1]) % 10},
    )
    # arr_clim = array.groupby("init_date.month").mean(dim="init_date")
    with xr.set_options(use_flox=False):
        arr_clim = array.groupby("init_date").mean(dim="init_date")

    wait(arr_clim, small_client, 15 * 60)
