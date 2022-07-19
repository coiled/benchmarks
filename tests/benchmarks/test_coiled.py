import uuid

import pytest
from coiled import Cluster


@pytest.fixture(autouse=True)
def parquet_benchmark_fixture(benchmark_time):
    yield


def test_default_cluster_spinup_time(request):

    with Cluster(name=f"{request.node.originalname}-{uuid.uuid4().hex[:8]}"):
        pass
