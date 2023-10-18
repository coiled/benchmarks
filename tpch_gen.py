import tempfile
import psutil
import pathlib
import duckdb
import botocore.session
import coiled
import click


@click.command()
@click.option("--sf", default=10, help="Scale factor to use")
@click.option("--target-mb", default=50, help="Target output parquet file size")
@click.option(
    "--path",
    default="./tpch-data",
    help="Local or S3 base path, will affix 'scale-<scale>' subdirectory to this path",
)
@coiled.function(region="us-east-2", memory="512 GiB", disk_size=500)
def main(sf, target_mb, path):
    if path.startswith("s3"):
        path += "/" if not path.endswith("/") else "" 
        path += f"scale-{sf}/"
    else:
        path = pathlib.Path(path).joinpath(f"scale-{sf}")
        path.mkdir(parents=True, exist_ok=True)
        path = str(path)

    print(f"Scale: {sf}, Path: {path}, Target MB: {target_mb}")

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
    con.sql(f"call dbgen(sf={sf})")
    print("Finished generating data, exporting...")

    tables = (
        con.sql("select * from information_schema.tables").arrow().column("table_name")
    )
    for table in map(str, tables):
        print(f"Exporting table: {table}")
        out = path + table

        # TODO: duckdb doesn't (yet) support writing parquet files by limited file size
        #       so we estimate the page size required for each table to get files of about a target size
        n_rows_total = con.sql(f"select count(*) from {table}").fetchone()[0]
        n_rows_per_page = rows_approx_mb(con, table, target_mb=target_mb)

        for offset in range(0, n_rows_total, n_rows_per_page):
            print(f"Start Exporting Page from {table} - Page {offset} - {offset + n_rows_per_page}")
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

def rows_approx_mb(con, table_name, target_mb):
    sample_size = 10_000
    table = con.sql(f"select * from {table_name} limit {sample_size}").arrow()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = pathlib.Path(tmpdir).joinpath("out.parquet")
        con.sql(
            f"copy (select * from {table_name} limit {sample_size}) to '{tmp}' (format parquet)"
        )
        mb = tmp.stat().st_size / (1024 * 1024)
    return int((len(table) * ((len(table) / sample_size) * target_mb)) / mb) or len(
        table
    )


if __name__ == "__main__":
    main()
