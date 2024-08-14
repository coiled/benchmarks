import uuid

import fsspec
import pytest
from coiled import Cluster
from distributed import Client
from xarray.groupers import TimeResampler

from tests.conftest import dump_cluster_kwargs
from tests.utils_test import wait


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
    group_reduction_cluster, cluster_kwargs, upload_cluster_dump, benchmark_all
):
    n_workers = cluster_kwargs["group_reduction_cluster"]["n_workers"]
    with Client(group_reduction_cluster) as client:
        group_reduction_cluster.scale(n_workers)
        client.wait_for_workers(n_workers, timeout=600)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
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
            lambda x: x.chunk(time=TimeResampler("ME"))
            .groupby("time.month")
            .mean(method="cohorts"),
            id="chunked-cohorts",
        ),
    ],
)
def test_xarray_groupby_reduction(group_reduction_client, func):
    pytest.importorskip("xarray")
    pytest.importorskip("flox")

    import xarray as xr

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
