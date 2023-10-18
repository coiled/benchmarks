import pathlib
import tempfile

import botocore.session
import click
import dask
import duckdb
import psutil


def generate(
    scale: int = 10, partition_size: str = "50 MiB", path: str = "./tpch-data"
):
    if str(path).startswith("s3"):
        path += "/" if not path.endswith("/") else ""
        path += f"scale-{scale}/"
    else:
        path = pathlib.Path(path) / f"scale-{scale}/"
        path.mkdir(parents=True, exist_ok=True)

    print(f"Scale: {scale}, Path: {path}, Partition Size: {partition_size}")

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
        """
    )

    print("Generating TPC-H data")
    con.sql(f"call dbgen(sf={scale})")
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
