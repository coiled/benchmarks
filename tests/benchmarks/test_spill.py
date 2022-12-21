import uuid

import dask.array as da
import numpy as np
import pytest
from coiled import Cluster
from dask.distributed import Client
from toolz import merge

from ..utils_test import (
    cluster_memory,
    print_size_info,
    scaled_array_shape,
    wait,
)


@pytest.fixture(scope="module")
def spill_cluster(dask_env_variables, cluster_kwargs, gitlab_cluster_tags):
    with Cluster(
        name=f"spill-{uuid.uuid4().hex[:8]}",
        environ=merge(
            dask_env_variables,
            {
                # Ensure that no tasks are not retried on worker ungraceful termination
                # caused by out-of-memory issues
                "DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": "0",
            },
        ),
        tags=gitlab_cluster_tags,
        **cluster_kwargs["spill_cluster"],
    ) as cluster:
        yield cluster


@pytest.fixture
def spill_client(spill_cluster, cluster_kwargs, upload_cluster_dump, benchmark_all):
    n_workers = cluster_kwargs["spill_cluster"]["n_workers"]
    with Client(spill_cluster) as client:
        spill_cluster.scale(n_workers)
        client.wait_for_workers(n_workers)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


@pytest.mark.parametrize(
    "keep_around", [pytest.param(False, id="release"), pytest.param(True, id="keep")]
)
@pytest.mark.parametrize(
    "compressible",
    [pytest.param(False, id="uncompressible"), pytest.param(True, id="compressible")],
)
def test_spilling(spill_client, compressible, keep_around):
    memory = cluster_memory(spill_client)  # 38.33 GiB
    shape = scaled_array_shape(memory * 1.67, ("x", "x"))  # 64 GiB
    a = da.random.random(shape)
    if compressible:
        # Note: this is not the same as da.zeros, which is smart and uses broadcasting
        # to actually store in memory just a single scalar
        a = a.map_blocks(np.zeros_like)
    print_size_info(memory, memory * 1.67, a)

    wait(a, spill_client, 600)
    b = a.sum()
    if not keep_around:
        del a
    wait(b, spill_client, 600)


@pytest.mark.parametrize(
    "compressible",
    [pytest.param(False, id="uncompressible"), pytest.param(True, id="compressible")],
)
def test_dot_product_spill(spill_client, compressible):
    """See also test_array.py::test_dot_product
    for variant that doesn't hit the spill threshold
    """
    memory = cluster_memory(spill_client)  # 38.33 GiB
    shape = scaled_array_shape(memory * 0.3, ("x", "x"))  # 11.5 GiB
    a = da.random.random(shape)
    if compressible:
        # Note: this is not the same as da.zeros, which is smart and uses broadcasting
        # to actually store in memory just a single scalar
        a = a.map_blocks(np.zeros_like)

    print_size_info(memory, memory * 0.3, a)
    b = (a @ a.T).sum().round(3)
    wait(b, spill_client, 3000)
