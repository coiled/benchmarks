import pytest
from distributed import LocalCluster

from . import dask_queries

pytestmark = pytest.mark.tpch_dask


@pytest.fixture(scope="module")
def cluster():
    with LocalCluster() as cluster:
        yield cluster


@pytest.fixture
def client(cluster, restart, benchmark_time):
    with cluster.get_client() as client:
        if restart:
            client.restart()
        client.run(lambda: None)

        with benchmark_time:
            yield client


@pytest.mark.parametrize(
    "query",
    [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
    ],
)
def test_optimization(query, dataset_path, fs, client):
    func = getattr(dask_queries, f"query_{query}")
    result = func(dataset_path, fs)
    result.optimize()
