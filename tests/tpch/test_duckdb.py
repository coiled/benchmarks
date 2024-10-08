import botocore.session
import pytest

pytestmark = pytest.mark.tpch_nondask

duckdb = pytest.importorskip("duckdb")

from . import duckdb_queries  # noqa: E402


@pytest.fixture(autouse=True)
def add_duckdb_version(tpch_database_table_schema):
    tpch_database_table_schema.duckdb_version = duckdb.__version__


@pytest.fixture(autouse=True)
def add_cluster_spec_to_db(tpch_database_table_schema, machine_spec, local):
    if not local:
        tpch_database_table_schema.n_workers = 1
        tpch_database_table_schema.worker_vm_type = machine_spec["vm_type"]
        tpch_database_table_schema.cluster_disk_size = machine_spec.get(
            "worker_disk_size"
        )


@pytest.fixture
def connection(local, restart):
    def _():
        con = duckdb.connect()

        if not local:  # Setup s3 credentials
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
        return con

    return _


def test_query_01(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_01(connection(), dataset_path, scale)

    run(_)


def test_query_02(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_02(connection(), dataset_path, scale)

    run(_)


def test_query_03(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_03(connection(), dataset_path, scale)

    run(_)


def test_query_04(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_04(connection(), dataset_path, scale)

    run(_)


def test_query_05(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_05(connection(), dataset_path, scale)

    run(_)


def test_query_06(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_06(connection(), dataset_path, scale)

    run(_)


def test_query_07(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_07(connection(), dataset_path, scale)

    run(_)


def test_query_08(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_08(connection(), dataset_path, scale)

    run(_)


def test_query_09(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_09(connection(), dataset_path, scale)

    run(_)


def test_query_10(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_10(connection(), dataset_path, scale)

    run(_)


def test_query_11(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_11(connection(), dataset_path, scale)

    run(_)


def test_query_12(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_12(connection(), dataset_path, scale)

    run(_)


def test_query_13(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_13(connection(), dataset_path, scale)

    run(_)


def test_query_14(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_14(connection(), dataset_path, scale)

    run(_)


def test_query_15(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_15(connection(), dataset_path, scale)

    run(_)


def test_query_16(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_16(connection(), dataset_path, scale)

    run(_)


def test_query_17(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_17(connection(), dataset_path, scale)

    run(_)


def test_query_18(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_18(connection(), dataset_path, scale)

    run(_)


def test_query_19(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_19(connection(), dataset_path, scale)

    run(_)


def test_query_20(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_20(connection(), dataset_path, scale)

    run(_)


def test_query_21(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_21(connection(), dataset_path, scale)

    run(_)


def test_query_22(run, connection, dataset_path, scale):
    def _():
        duckdb_queries.query_22(connection(), dataset_path, scale)

    run(_)
