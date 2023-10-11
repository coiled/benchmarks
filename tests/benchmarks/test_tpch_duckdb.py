import timeit

import botocore
import duckdb

from tests.benchmarks import tpch_duckdb_queries as queries
from tests.benchmarks.conftest import ENABLED_DATASET


def test_query_1(coiled_function):
    return run_tpch_duckdb(queries.q1, coiled_function)


def test_query_2(coiled_function):
    return run_tpch_duckdb(queries.q2, coiled_function)


def test_query_3(coiled_function):
    return run_tpch_duckdb(queries.q3, coiled_function)


def test_query_4(coiled_function):
    return run_tpch_duckdb(queries.q4, coiled_function)


def test_query_5(coiled_function):
    return run_tpch_duckdb(queries.q5, coiled_function)


def test_query_6(coiled_function):
    return run_tpch_duckdb(queries.q6, coiled_function)


def test_query_7(coiled_function):
    return run_tpch_duckdb(queries.q7, coiled_function)


def run_tpch_duckdb(module, coiled_function):
    @coiled_function
    def _():
        start = timeit.default_timer()

        con = duckdb.connect()

        if ENABLED_DATASET != "local":  # Setup s3 credentials
            creds = botocore.session.get_session().get_credentials()
            con.install_extension("httpfs")
            con.load_extension("httpfs")
            con.sql(
                f"""
                SET s3_region='us-east-2';
                SET s3_access_key_id='{creds.access_key}';
                SET s3_secret_access_key='{creds.secret_key}';
                """
            )
        start_query = timeit.default_timer()
        if hasattr(module, "ddl"):
            con.sql(module.ddl)
        _ = con.execute(module.query).fetch_arrow_table()
        time_query = timeit.default_timer() - start_query

        con.close()

        time_total = timeit.default_timer() - start
        return dict(time_query=time_query, time_total=time_total)

    try:
        return _()
    finally:
        _.cluster.get_client().restart()
