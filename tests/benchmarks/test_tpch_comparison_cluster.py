import json
import pathlib
import timeit

import pytest

from tests.benchmarks import test_tpch as test_tpch_dask
from tests.benchmarks import test_tpch_pyspark

ENGINES = ("pyspark", "dask")


@pytest.mark.parametrize("engine", ENGINES)
def test_query_1(tpch_pyspark_client, engine):
    return _run_test(
        tpch_pyspark_client,
        test_tpch_pyspark.test_query_1,
        test_tpch_dask.test_query_1,
        engine,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_2(tpch_pyspark_client, engine):
    return _run_test(
        tpch_pyspark_client,
        test_tpch_pyspark.test_query_2,
        test_tpch_dask.test_query_2,
        engine,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_3(tpch_pyspark_client, engine):
    return _run_test(
        tpch_pyspark_client,
        test_tpch_pyspark.test_query_3,
        test_tpch_dask.test_query_3,
        engine,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_4(tpch_pyspark_client, engine):
    return _run_test(
        tpch_pyspark_client,
        test_tpch_pyspark.test_query_4,
        test_tpch_dask.test_query_4,
        engine,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_5(tpch_pyspark_client, engine):
    return _run_test(
        tpch_pyspark_client,
        test_tpch_pyspark.test_query_5,
        test_tpch_dask.test_query_5,
        engine,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_6(tpch_pyspark_client, engine):
    return _run_test(
        tpch_pyspark_client,
        test_tpch_pyspark.test_query_6,
        test_tpch_dask.test_query_6,
        engine,
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_query_7(tpch_pyspark_client, engine):
    return _run_test(
        tpch_pyspark_client,
        test_tpch_pyspark.test_query_7,
        test_tpch_dask.test_query_7,
        engine,
    )


def _run_test(client, pyspark_test, dask_test, engine):
    if engine == "dask":
        start = timeit.default_timer()
        dask_test(client)
        results = dict(time_query=timeit.default_timer() - start)
    elif engine == "pyspark":
        results = pyspark_test(client)

    p = pathlib.Path("./results.json")
    results["engine"] = engine
    results["query_number"] = dask_test.__name__.split("_")[-1]
    with p.open("a") as f:
        f.write(f"{json.dumps(results)}\n")
