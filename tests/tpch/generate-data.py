import functools
import pathlib
import tempfile
import warnings

import botocore.session
import click
import coiled
import dask
import duckdb
import psutil
from dask.distributed import wait


def generate(
    scale: int = 10,
    partition_size: str = "50 MiB",
    path: str = "./tpch-data",
):
    if str(path).startswith("s3"):
        path += "/" if not path.endswith("/") else ""
        path += f"scale-{scale}/"
        use_coiled = True
    else:
        path = pathlib.Path(path) / f"scale-{scale}/"
        path.mkdir(parents=True, exist_ok=True)
        use_coiled = False

    print(f"Scale: {scale}, Path: {path}, Partition Size: {partition_size}")

    if use_coiled:
        with coiled.Cluster(
            n_workers=10, worker_memory="4 GiB", worker_options={"nthreads": 1}
        ) as cluster:
            cluster.adapt(minimum=1, maximum=250)
            client = cluster.get_client()

            jobs = []
            for step in range(0, scale):
                job = client.submit(
                    _dbgen_to_path,
                    scale=scale,
                    step=step,
                    path=path,
                    partition_size=partition_size,
                )
                jobs.append(job)
            wait(jobs)
    else:
        return _dbgen_to_path(scale, path, partition_size)


def retry(f):
    @functools.wraps(f)
    def _(*args, **kwargs):
        for _ in range(5):
            try:
                return f(*args, **kwargs)
            except Exception as exc:
                warnings.warn(f"Failed w/ {exc}, retrying...")
                continue
        return f(*args, **kwargs)

    return _


@retry
def _dbgen_to_path(scale, path, partition_size, step=None):
    con = duckdb.connect()
    con.install_extension("tpch")
    con.load_extension("tpch")

    session = botocore.session.Session()
    creds = session.get_credentials()
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    con.sql(
        f"""
        SET memory_limit='{psutil.virtual_memory().available // 2**30 }G';
        SET s3_region='us-east-2';
        SET s3_access_key_id='{creds.access_key}';
        SET s3_secret_access_key='{creds.secret_key}';
        SET s3_session_token='{creds.token}';
        SET preserve_insertion_order=false;
        """
    )

    print("Generating TPC-H data")
    if step is None:
        query = f"call dbgen(sf={scale})"
    else:
        query = f"call dbgen(sf={scale}, children={scale}, step={step})"
    con.sql(query)

    print("Finished generating data, exporting...")

    tables = (
        con.sql("select * from information_schema.tables").arrow().column("table_name")
    )
    for table in map(str, tables):
        print(f"Exporting table: {table}")
        if str(path).startswith("s3://"):
            out = path + table
        else:
            out = path / table

        # TODO: duckdb doesn't (yet) support writing parquet files by limited file size
        #       so we estimate the page size required for each table to get files of about a target size
        n_rows_total = con.sql(f"select count(*) from {table}").fetchone()[0]
        n_rows_per_page = rows_approx_mb(con, table, partition_size=partition_size)
        if n_rows_total == 0:
            continue  # In case of step based production, some tables may already be fully generated

        for offset in range(0, n_rows_total, n_rows_per_page):
            print(
                f"Start Exporting Page from {table} - Page {offset} - {offset + n_rows_per_page}"
            )
            con.sql(
                f"""
                copy
                    (select * from {table} offset {offset} limit {n_rows_per_page} )
                to '{out}'
                (format parquet, per_thread_output true, filename_pattern "{table}_{{uuid}}", overwrite_or_ignore)
                """
            )
        print(f"Finished exporting table {table}!")
    print("Finished exporting all data!")
    con.close()


def rows_approx_mb(con, table_name, partition_size: str):
    partition_size = dask.utils.parse_bytes(partition_size)
    sample_size = 10_000
    table = con.sql(f"select * from {table_name} limit {sample_size}").arrow()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = pathlib.Path(tmpdir) / "out.parquet"
        con.sql(
            f"copy (select * from {table_name} limit {sample_size}) to '{tmp}' (format parquet)"
        )
        mb = tmp.stat().st_size
    return int(
        (len(table) * ((len(table) / sample_size) * partition_size)) / mb
    ) or len(table)


@click.command()
@click.option(
    "--scale", default=10, help="Scale factor to use, roughly equal to number of GB"
)
@click.option(
    "--partition-size", default="50 MiB", help="Target output parquet file size "
)
@click.option(
    "--path",
    default="./tpch-data",
    help="Local or S3 base path, will affix 'scale-<scale>' subdirectory to this path",
)
def main(scale: int, partition_size: str, path: str):
    generate(scale, partition_size, path)


if __name__ == "__main__":
    main()
