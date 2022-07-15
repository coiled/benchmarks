import uuid

import dask.array as da
import distributed
import pytest
from coiled import Cluster


@pytest.fixture(autouse=True)
def parquet_benchmark_fixture(benchmark_time):
    yield


def test_default_cluster_spinup_time(request):

    with Cluster(name=f"{request.node.originalname}-{uuid.uuid4().hex[:8]}"):
        pass


@pytest.mark.parametrize("offset", [1, 2, 3, 4, 5])
def test_stuff(offset):
    import random
    import time

    time.sleep(offset + random.uniform(-0.2, 0.2))


@pytest.fixture
def local_client(sample_memory):
    with distributed.Client() as client:
        with sample_memory(client):
            yield client


@pytest.mark.parametrize("n", [100, 1000, 10_000])
def test_sample_memory(n, local_client):
    x = da.random.uniform(size=(n, n))
    (x + x.T).mean(axis=1).compute()
