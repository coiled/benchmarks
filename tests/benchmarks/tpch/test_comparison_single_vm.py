import dask
import pytest
from distributed import LocalCluster

from tests.benchmarks import tpch

"""
At time of writing,.tpch.dask/pyspark will run on clusters,
and.tpch.duckdb/polars can only run on single VMs.

This module runs all on a single VM using coiled.function
"""


ENGINES = ("dask", "duckdb", "pyspark", "polars")


@pytest.mark.parametrize("engine", ENGINES)
def test_query_1(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        tpch.test_dask.test_query_1,
        tpch.test_duckdb.test_query_1,
        tpch.test_pyspark.test_query_1,
        tpch.test_polars.test_query_1,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_2(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        tpch.test_dask.test_query_2,
        tpch.test_duckdb.test_query_2,
        tpch.test_pyspark.test_query_2,
        tpch.test_polars.test_query_2,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_3(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        tpch.test_dask.test_query_3,
        tpch.test_duckdb.test_query_3,
        tpch.test_pyspark.test_query_3,
        tpch.test_polars.test_query_3,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_4(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        tpch.test_dask.test_query_4,
        tpch.test_duckdb.test_query_4,
        tpch.test_pyspark.test_query_4,
        tpch.test_polars.test_query_4,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_5(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        tpch.test_dask.test_query_5,
        tpch.test_duckdb.test_query_5,
        tpch.test_pyspark.test_query_5,
        tpch.test_polars.test_query_5,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_6(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        tpch.test_dask.test_query_6,
        tpch.test_duckdb.test_query_6,
        tpch.test_pyspark.test_query_6,
        tpch.test_polars.test_query_6,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_7(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        tpch.test_dask.test_query_7,
        tpch.test_duckdb.test_query_7,
        tpch.test_pyspark.test_query_7,
        tpch.test_polars.test_query_7,
    )


def _run_test(
    coiled_function, engine, dask_test, duckdb_test, pyspark_test, polars_test
):
    if engine == "dask":
        # Will get KeyError 'tests.benchmarks.tpch.comparison_single_vm' with coiled.function
        # and voodoo w/ run_on_scheduler to launch new local cluster to run a given function
        # also needs setting distributed.worker.daemon=False when launching single node cluster, then
        # on again when launching LocalCluster, otherwise AssertionError daemonic processes cannot have children
        pytest.skip(
            "Running dask on single node w/ coiled.Cluster / coiled.function not ready yet."
        )

        # Dask will automatically start making use of adaptive, since
        # Scheduler will start receiving tasks, DuckDB and PySpark won't
        def _():
            with dask.config.set({"distributed.worker.daemon": True}):
                with LocalCluster(
                    scheduler_kwargs={"port": 8785}, dashboard=False
                ) as cluster:
                    client = cluster.get_client()
                    return dask_test(client)

        f = coiled_function(_)
        f.cluster.adapt(minimum=1, maximum=1)
        try:
            f()
        finally:
            f.cluster.get_client().restart()

    elif engine == "duckdb":
        duckdb_test(coiled_function)

    elif engine == "pyspark":
        f = coiled_function(pyspark_test)
        try:
            f(None)
        finally:
            f.cluster.get_client().restart()

    elif engine == "polars":
        polars_test(coiled_function)
