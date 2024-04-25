import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import botocore.session
import pytest

pytestmark = pytest.mark.tpch_nondask

duckdb = pytest.importorskip("duckdb")

from . import duckdb_queries  # noqa: E402


@contextmanager
def tempdir(base_path: str) -> Iterator[str]:
    path = Path(base_path) / str(uuid.uuid4())
    path.mkdir()
    try:
        yield path.name
    finally:
        path.rmdir()


@pytest.fixture
def connection(local, restart):
    @contextmanager
    def _():
        if local:
            from tempfile import TemporaryDirectory

            tempdir_ctx = TemporaryDirectory()
        else:
            from distributed import get_worker

            tempdir_ctx = tempdir(get_worker().local_directory)

        with tempdir_ctx as dir, duckdb.connect() as con:
            con.sql(f"SET temp_directory='{dir}';")

            if not local:
                # Setup s3 credentials
                session = botocore.session.Session()
                creds = session.get_credentials()
                con.install_extension("httpfs")
                con.load_extension("httpfs")
                con.sql(
                    f"""
                    SET s3_region='us-east-2';
                    SET s3_access_key_id='{creds.access_key}';
                    SET s3_secret_access_key='{creds.secret_key}';
                    SET s3_session_token='{creds.token}';
                    """
                )
            yield con

    return _


def test_query_1(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_1(con, dataset_path, scale)

    run(_)


def test_query_2(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_2(con, dataset_path, scale)

    run(_)


def test_query_3(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_3(con, dataset_path, scale)

    run(_)


def test_query_4(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_4(con, dataset_path, scale)

    run(_)


def test_query_5(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_5(con, dataset_path, scale)

    run(_)


def test_query_6(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_6(con, dataset_path, scale)

    run(_)


def test_query_7(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_7(con, dataset_path, scale)

    run(_)


def test_query_8(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_8(con, dataset_path, scale)

    run(_)


def test_query_9(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_9(con, dataset_path, scale)

    run(_)


def test_query_10(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_10(con, dataset_path, scale)

    run(_)


def test_query_11(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_11(con, dataset_path, scale)

    run(_)


def test_query_12(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_12(con, dataset_path, scale)

    run(_)


def test_query_13(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_13(con, dataset_path, scale)

    run(_)


def test_query_14(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_14(con, dataset_path, scale)

    run(_)


def test_query_15(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_15(con, dataset_path, scale)

    run(_)


def test_query_16(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_16(con, dataset_path, scale)

    run(_)


def test_query_17(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_17(con, dataset_path, scale)

    run(_)


def test_query_18(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_18(con, dataset_path, scale)

    run(_)


def test_query_19(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_19(con, dataset_path, scale)

    run(_)


def test_query_20(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_20(con, dataset_path, scale)

    run(_)


def test_query_21(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_21(con, dataset_path, scale)

    run(_)


def test_query_22(run, connection, dataset_path, scale):
    def _():
        with connection() as con:
            duckdb_queries.query_22(con, dataset_path, scale)

    run(_)
