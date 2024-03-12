import pytest

from . import dask_queries

pytestmark = pytest.mark.tpch_dask


@pytest.fixture(
    params=[
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
def query(request):
    return request.param


def test_optimization(query, dataset_path, fs, client):
    func = getattr(dask_queries, f"query_{query}")
    result = func(dataset_path, fs)
    # We need to inject .repartition(npartitions=1) which .compute() does under the hood
    result.repartition(npartitions=1).optimize()


def test_delay_computation_start(query, dataset_path, fs, client, scale):
    func = getattr(dask_queries, f"query_{query}")
    result = func(dataset_path, fs, scale).optimize()
    # Client.compute unblocks as soon as update_graph finishes, i.e. graph is
    # submitted and parsed. This is the time until the dashboard kicks off
    client.compute(result)
