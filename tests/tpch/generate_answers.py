import os
import pathlib

import botocore.session
import click
import coiled
import duckdb
import duckdb_queries
import pyarrow as pa
import pyarrow.parquet as pq
from utils import (
    get_answers_path,
    get_bucket_region,
    get_dataset_path,
    get_single_vm_spec,
)


def generate(scale: int, path: str, local: bool) -> None:
    dataset_path = get_dataset_path(local, scale)
    use_coiled = False

    if path.startswith("s3"):
        use_coiled = True
        global REGION
        REGION = get_bucket_region(path)
    else:
        path = pathlib.Path(path)
        path.mkdir(parents=True, exist_ok=True)

    def connection():
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

    def generate_answer(query):
        table = getattr(duckdb_queries, f"query_{query}")(
            connection(), dataset_path, scale
        )
        relaxed_schema = table.schema
        for i, field in enumerate(table.schema):
            if pa.types.is_decimal(field.type):
                relaxed_schema = relaxed_schema.set(i, field.with_type(pa.float64()))
            elif pa.types.is_date(field.type):
                relaxed_schema = relaxed_schema.set(
                    i, field.with_type(pa.timestamp("ms"))
                )
        table = table.cast(relaxed_schema)
        pq.write_table(table, os.path.join(str(path), f"answer_{query}.parquet"))

    if use_coiled:
        generate_answer = coiled.function(
            name=f"tpch-generate-answers-{scale}", **get_single_vm_spec(scale)
        )(generate_answer)
    for query in range(1, 23):
        generate_answer(query)

    print("Finished exporting all answers!")


@click.command()
@click.option(
    "--scale", default=10, help="Scale factor to use, roughly equal to number of GB"
)
@click.option(
    "--path",
    help="Local or S3 base path, will affix '/answers' subdirectory to this path",
)
@click.option(
    "--local",
    is_flag=True,
    default=False,
    help="Whether to generate the answers locally",
)
def main(
    scale: int,
    path: str | None,
    local: bool,
):
    if path is None:
        path = get_answers_path(local, scale)
    generate(scale, path, local)


if __name__ == "__main__":
    main()
