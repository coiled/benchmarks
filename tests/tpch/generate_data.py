import enum
import functools
import pathlib
import tempfile
import time
import uuid
import warnings
from collections import namedtuple
from typing import Union

import botocore.session
import click
import coiled
import dask
import duckdb
import psutil
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from utils import get_bucket_region

REGION = None
SMALL_TABLES = {"region", "nation"}
SmallTable = namedtuple("SmallTable", ("name", "out_dir", "data", "step"))


class CompressionCodec(enum.Enum):
    SNAPPY = "SNAPPY"
    LZ4 = "LZ4"
    ZSTD = "ZSTD"
    GZIP = "GZIP"
    BROTLI = "BROTLI"
    NONE = "NONE"


def generate(
    scale: int = 10,
    partition_size: str = "128 MiB",
    path: str = "./tpch-data",
    relaxed_schema: bool = False,
    compression: CompressionCodec = CompressionCodec.SNAPPY,
) -> str:
    if str(path).startswith("s3"):
        path += "/" if not path.endswith("/") else ""
        path += f"scale-{scale}{'-strict' if not relaxed_schema else ''}/"
        use_coiled = True
        global REGION
        REGION = get_bucket_region(path)
    else:
        path = (
            pathlib.Path(path)
            / f"scale-{scale}{'-strict' if not relaxed_schema else ''}/"
        )
        path.mkdir(parents=True, exist_ok=True)
        use_coiled = False

    print(f"Scale: {scale}, Path: {path}, Partition Size: {partition_size}")
    kwargs = dict(
        scale=scale,
        path=path,
        relaxed_schema=relaxed_schema,
        partition_size=partition_size,
        compression=compression,
    )

    if use_coiled:
        with coiled.Cluster(
            n_workers=10,
            # workload is best with 1vCPU and ~6-8GiB memory
            worker_cpu=[1, 2],
            worker_memory=["6GiB", "8GiB"],
            worker_options={"nthreads": 1},
            region=REGION,
        ) as cluster:
            cluster.adapt(minimum=1, maximum=350)
            with cluster.get_client() as client:
                tpch_from_client(client, **kwargs)
    else:
        with dask.distributed.Client(
            threads_per_worker=1,
        ) as client:
            tpch_from_client(client, **kwargs)

            # TODO: hack to suppress noisy "CommClosedError"s
            # xref: https://github.com/dask/distributed/issues/7192
            client.retire_workers()
            time.sleep(5)
    return str(path)


def tpch_from_client(client, scale: int, compression: CompressionCodec, **kwargs):
    jobs = client.map(
        _tpch_data_gen, range(0, scale), scale=scale, compression=compression, **kwargs
    )
    small_tables = client.gather(jobs)
    client.submit(
        collect_small_tables, tables=small_tables, compression=compression
    ).result()


def collect_small_tables(
    tables: list[dict[str, SmallTable]], compression: CompressionCodec
):
    table_names = {t for d in tables for t in d}
    for table in table_names:
        # sorted to ensure the file rows are the same order as if generated in one go
        small_tables: list[SmallTable] = sorted(
            [t[table] for t in tables if table in t], key=lambda t: t.step
        )
        if small_tables:
            name = small_tables[0].name
            out_dir = small_tables[0].out_dir
            table = pa.concat_tables(t.data for t in small_tables)
            write_table(name, table, out_dir, compression)
            print(f"Combined {len(small_tables)} partitions for small table '{name}'")


def write_table(
    name: str,
    table: pa.Table,
    dir: Union[str, pathlib.Path],
    compression: CompressionCodec,
) -> str:
    filename = f"{name}_{uuid.uuid4()}.{compression.value.lower()}.parquet"

    if isinstance(dir, str) and dir.startswith("s3"):
        path = f"{dir}/{filename}"
    else:
        path = pathlib.Path(dir)
        path.mkdir(exist_ok=True, parents=True)
        path = str(path / filename)

    pq.write_table(
        table,
        path,
        compression=compression.value.lower(),
        write_statistics=True,
        write_page_index=True,
    )
    return path


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
def _tpch_data_gen(
    step: int,
    scale: int,
    path: str,
    partition_size: str,
    relaxed_schema: bool,
    compression: CompressionCodec,
) -> dict[str, SmallTable]:
    """
    Run TPC-H dbgen for generating the <step>th part of a multi-part load or update set
    into an output directory.

    step: Union[int, None]
        Generate the <n>th part of a multi-part load or update set in this scale, if None
        then generate the whole scale in one call.
    scale: int
        The TPC-H scale to generate
    path: str
        Output path of the generated parquet files
    partition_size: str
        Target parquet file output size. Some files may be smaller than this, when the remaining
        data from a given table is less than this size.
    relaxed_schema: bool
        To cast certain datatypes like `date` or `decimal` types to `timestamp_s` and `double`;
        this flag will call the casting done in `_alter_tables` function before outputting parquet files.
    """
    small_tables = dict()
    with duckdb.connect() as con:
        con.install_extension("tpch")
        con.load_extension("tpch")

        if str(path).startswith("s3://"):
            session = botocore.session.Session()
            creds = session.get_credentials()
            con.install_extension("httpfs")
            con.load_extension("httpfs")
            con.sql(
                f"""
                SET s3_region='{REGION}';
                SET s3_access_key_id='{creds.access_key}';
                SET s3_secret_access_key='{creds.secret_key}';
                SET s3_session_token='{creds.token}';
                """
            )

        con.sql(
            f"""
            SET memory_limit='{psutil.virtual_memory().available // 2**30 }G';
            SET preserve_insertion_order=false;
            SET threads TO 1;
            SET enable_progress_bar=false;
            """
        )

        print("Generating TPC-H data")
        if step is None:
            query = f"call dbgen(sf={scale})"
        else:
            query = f"call dbgen(sf={scale}, children={scale}, step={step})"
        con.sql(query)
        print("Finished generating data, exporting...")

        if relaxed_schema:
            print("Converting types date -> timestamp_s and decimal -> double")
            _alter_tables(con)
            print("Done altering tables")

        tables = (
            con.sql("select * from information_schema.tables")
            .arrow()
            .column("table_name")
        )
        for table in map(str, tables):
            print(f"Exporting table: {table}")

            if str(path).startswith("s3://"):
                out_dir = path + table
            else:
                out_dir = path / table

            # Avoid writing out partitions for small tables, they'll be collected
            # and written out at the end.
            if table in SMALL_TABLES:
                df = con.sql(f"select * from {table}").arrow()
                small_tables[table] = SmallTable(table, out_dir, df, step)
                continue

            n_rows_total = con.sql(f"select count(*) from {table}").fetchone()[0]
            n_rows_per_page = rows_approx_mb(
                con, table, partition_size=partition_size, compression=compression
            )
            if n_rows_total == 0:
                continue  # In case of step based production, some tables may already be fully generated

            for offset in range(0, n_rows_total, n_rows_per_page):
                print(
                    f"Start Exporting Page from {table} - Page {offset} - {offset + n_rows_per_page}"
                )
                stmt = f"select * from {table} offset {offset} limit {n_rows_per_page}"
                df = con.sql(stmt).arrow()
                out_path = write_table(table, df, out_dir, compression)
                print(f"Wrote {len(df)} rows of table {table} to {out_path}")
        return small_tables


def rows_approx_mb(con, table_name, partition_size: str, compression: CompressionCodec):
    """
    Estimate the number of rows from this table required to
    result in a parquet file output size of `partition_size`
    """
    partition_size = dask.utils.parse_bytes(partition_size)
    sample_size = 10_000
    table = con.sql(f"select * from {table_name} limit {sample_size}").arrow()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = pathlib.Path(tmpdir) / "tmp.parquet"
        stmt = f"select * from {table_name} limit {sample_size}"
        df = con.sql(stmt).arrow()
        write_table(table_name, df, tmp, compression)
        mb = tmp.stat().st_size
    return int(
        (len(table) * ((len(table) / sample_size) * partition_size)) / mb
    ) or len(table)


def _alter_tables(con):
    """
    Temporary, used for debugging performance in data types.

    ref discussion here: https://github.com/coiled/benchmarks/pull/1131
    """
    tables = (
        con.sql("select * from information_schema.tables").arrow().column("table_name")
    )
    for table in tables:
        schema = con.sql(f"describe {table}").arrow()

        # alter decimals to floats
        for column in schema.filter(
            pc.match_like(pc.field("column_type"), "DECIMAL%")
        ).column("column_name"):
            con.sql(f"alter table {table} alter {column} type double")

        # alter date to timestamp_s
        for column in schema.filter(pc.field("column_type") == "DATE").column(
            "column_name"
        ):
            con.sql(f"alter table {table} alter {column} type timestamp_s")


@click.command()
@click.option(
    "--scale", default=10, help="Scale factor to use, roughly equal to number of GB"
)
@click.option(
    "--partition-size", default="128 MiB", help="Target output parquet file size "
)
@click.option(
    "--path",
    default="./tpch-data",
    help="Local or S3 base path, will affix 'scale-<scale>' subdirectory to this path",
)
@click.option(
    "--relaxed-schema",
    default=False,
    flag_value=True,
    help="Set flag to convert official TPC-H types decimal -> float and date -> timestamp_s",
)
@click.option(
    "--compression",
    type=click.Choice([v.upper() for v in CompressionCodec.__members__]),
    callback=lambda _c, _p, v: getattr(CompressionCodec, v.upper()),
    default=CompressionCodec.SNAPPY.value,
    help="Set compression codec",
)
def main(
    scale: int,
    partition_size: str,
    path: str,
    relaxed_schema: bool,
    compression: CompressionCodec,
):
    generate(scale, partition_size, path, relaxed_schema, compression)


if __name__ == "__main__":
    main()
