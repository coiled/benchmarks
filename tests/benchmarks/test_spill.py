# import uuid

# import pytest
# from coiled import Cluster
# from dask.distributed import Client, wait
# from toolz import merge

# from ..conftest import dump_cluster_kwargs
# from ..utils_test import (
#     cluster_memory,
#     print_size_info,
#     scaled_array_shape,
#     scaled_array_shape_quadratic,
# )


# @pytest.fixture(scope="module")
# def spill_cluster(dask_env_variables, cluster_kwargs, github_cluster_tags):
#     kwargs = dict(
#         name=f"spill-{uuid.uuid4().hex[:8]}",
#         environ=merge(
#             dask_env_variables,
#             {
#                 # Ensure that no tasks are not retried on worker ungraceful termination
#                 # caused by out-of-memory issues
#                 "DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": "0",
#             },
#         ),
#         tags=github_cluster_tags,
#         **cluster_kwargs["spill_cluster"],
#     )
#     dump_cluster_kwargs(kwargs, "spill")
#     with Cluster(**kwargs) as cluster:
#         yield cluster


# @pytest.fixture
# def spill_client(spill_cluster, cluster_kwargs, upload_cluster_dump, benchmark_all):
#     n_workers = cluster_kwargs["spill_cluster"]["n_workers"]
#     with Client(spill_cluster) as client:
#         spill_cluster.scale(n_workers)
#         client.wait_for_workers(n_workers)
#         client.restart()
#         with upload_cluster_dump(client), benchmark_all(client):
#             yield client


# @pytest.mark.parametrize(
#     "keep_around", [pytest.param(False, id="release"), pytest.param(True, id="keep")]
# )
# def test_spilling(spill_client, new_array, keep_around):
#     memory = cluster_memory(spill_client)  # 36 GiB
#     shape = scaled_array_shape(memory * 1.79, ("x", "x"))  # 64 GiB
#     a = new_array(shape)
#     print_size_info(memory, memory * 1.79, a)

#     a = a.persist()
#     wait(a)
#     b = a.sum().persist()
#     if not keep_around:
#         del a
#     assert b.compute()


# def test_dot_product_spill(spill_client, new_array):
#     """See also test_array.py::test_dot_product
#     for variant that doesn't hit the spill threshold
#     """
#     memory = cluster_memory(spill_client)  # 38.33 GiB
#     shape = scaled_array_shape_quadratic(memory * 0.3, "11.5 GiB", ("x", "x"))
#     a = new_array(shape)
#     print_size_info(memory, memory * 0.3, a)
#     b = (a @ a.T).sum()
#     assert b.compute()
