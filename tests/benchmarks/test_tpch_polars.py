import timeit
from tests.benchmarks import tpch_polars_queries as queries


def test_query_1(coiled_function):
    return run_tpch_query(queries.q1, coiled_function)


def test_query_2(coiled_function):
    return run_tpch_query(queries.q2, coiled_function)


def test_query_3(coiled_function):
    return run_tpch_query(queries.q3, coiled_function)


def test_query_4(coiled_function):
    return run_tpch_query(queries.q4, coiled_function)


def test_query_5(coiled_function):
    return run_tpch_query(queries.q5, coiled_function)


def test_query_6(coiled_function):
    return run_tpch_query(queries.q6, coiled_function)


def test_query_7(coiled_function):
    return run_tpch_query(queries.q7, coiled_function)


def run_tpch_query(module, coiled_function):
    @coiled_function
    def _():
        start = timeit.default_timer()
        module.query().collect(streaming=True)
        end = timeit.default_timer() - start
        return dict(time_total=end, time_query=end)

    try:
        return _()
    finally:
        _.cluster.get_client().restart()
