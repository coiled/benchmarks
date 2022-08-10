import xarray as xr
import dask.array as da
from dask.utils import format_bytes
import numpy as np
import distributed
import coiled


def wait(thing, client, timeout):
    p = thing.persist()
    try:
        distributed.wait(p, timeout=timeout)
        for f in client.futures_of(p):
            if f.status in ("error", "cancelled"):
                raise f.exception()
    finally:
        client.cancel(p)


def test_anom_mean(test_name_uuid, upload_cluster_dump, sample_memory, benchmark_time):
    # From https://github.com/dask/distributed/issues/2602#issuecomment-498718651
    data = da.random.random((10000, 1000000), chunks=(1, 1000000))
    arr = xr.DataArray(
        data, dims=["time", "x"], coords={"day": ("time", np.arange(10000) % 100)}
    )
    print(
        f"{format_bytes(data.nbytes)} - {data.npartitions} {format_bytes(data.blocks[0, 0].nbytes)} chunks"
    )
    # 74.51 GiB - 10000 7.63 MiB chunks

    clim = arr.groupby("day").mean(dim="time")
    anom = arr.groupby("day") - clim
    anom_mean = anom.mean(dim="time")

    with coiled.Cluster(
        name=test_name_uuid,
        n_workers=20,
        worker_vm_types=["r5a.large"],  # 2 CPU, 16GiB
        environ={"MALLOC_TRIM_THRESHOLD_": 0},
        wait_for_workers=True,
        package_sync=True,
    ) as cluster:
        with distributed.Client(cluster) as client, upload_cluster_dump(
            client, cluster
        ), sample_memory(client):
            wait(anom_mean, client, 10 * 60)


def test_basic_sum(test_name_uuid, upload_cluster_dump, sample_memory, benchmark_time):
    # From https://github.com/dask/distributed/pull/4864

    data = da.zeros((12500000, 5000), chunks=(12500000, 1))
    print(
        f"{format_bytes(data.nbytes)} - {data.npartitions} {format_bytes(data.blocks[0, 0].nbytes)} chunks"
    )
    # 465.66 GiB - 5000 95.37 MiB chunks

    result = da.sum(data, axis=1)

    with coiled.Cluster(
        name=test_name_uuid,
        n_workers=4,
        worker_vm_types=["t3.xlarge"],  # 4 CPU, 16GiB
        environ={"MALLOC_TRIM_THRESHOLD_": 0},
        wait_for_workers=True,
        package_sync=True,
    ) as cluster:
        with distributed.Client(cluster) as client, upload_cluster_dump(
            client, cluster
        ), sample_memory(client):
            wait(result, client, 10 * 60)
