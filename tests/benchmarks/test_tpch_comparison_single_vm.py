import dask
import pytest
from distributed import LocalCluster
from tests.benchmarks import (
    test_tpch as test_tpch_dask,
    test_tpch_duckdb,
    test_tpch_pyspark,
)

"""
At time of writing, test_tpch_dask/pyspark will run on clusters,
and test_tpch_duckdb can only run on single VMs. 

This module runs all on a single VM using coiled.function
"""


@pytest.mark.parametrize("engine", ("dask", "duckdb", "pyspark"))
def test_query_1(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        test_tpch_dask.test_query_1,
        test_tpch_duckdb.test_query_1,
        test_tpch_pyspark.test_query_1,
    )


@pytest.mark.parametrize("engine", ("dask", "duckdb", "pyspark"))
def test_query_2(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        test_tpch_dask.test_query_2,
        test_tpch_duckdb.test_query_2,
        test_tpch_pyspark.test_query_2,
    )


@pytest.mark.parametrize("engine", ("dask", "duckdb", "pyspark"))
def test_query_3(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        test_tpch_dask.test_query_3,
        test_tpch_duckdb.test_query_3,
        test_tpch_pyspark.test_query_3,
    )


@pytest.mark.parametrize("engine", ("dask", "duckdb", "pyspark"))
def test_query_4(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        test_tpch_dask.test_query_4,
        test_tpch_duckdb.test_query_4,
        test_tpch_pyspark.test_query_4,
    )


@pytest.mark.parametrize("engine", ("dask", "duckdb", "pyspark"))
def test_query_5(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        test_tpch_dask.test_query_5,
        test_tpch_duckdb.test_query_5,
        test_tpch_pyspark.test_query_5,
    )


@pytest.mark.parametrize("engine", ("dask", "duckdb", "pyspark"))
def test_query_6(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        test_tpch_dask.test_query_6,
        test_tpch_duckdb.test_query_6,
        test_tpch_pyspark.test_query_6,
    )


@pytest.mark.parametrize("engine", ("dask", "duckdb", "pyspark"))
def test_query_7(coiled_function, engine):
    _run_test(
        coiled_function,
        engine,
        test_tpch_dask.test_query_7,
        test_tpch_duckdb.test_query_7,
        test_tpch_pyspark.test_query_7,
    )


def _run_test(coiled_function, engine, dask_test, duckdb_test, pyspark_test):
    if engine == "dask":
        raise NotImplementedError(
            "Can't use LocalCluster(processes=True) in coiled.function ctx"
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
        f()

    elif engine == "duckdb":
        duckdb_test(coiled_function)

    elif engine == "pyspark":
        coiled_function(pyspark_test)(None)
