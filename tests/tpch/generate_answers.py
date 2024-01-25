import click
import duckdb
import fsspec

from tests.tpch.utils import (
    DEFAULT_DATA_BASE_DIR,
    DEFAULT_VERIFICATION_SCALE,
    compose_answer_dir,
    compose_answer_fpath,
    normalize_scale,
)


def generate(base_dir: str):
    scale = normalize_scale(DEFAULT_VERIFICATION_SCALE)
    path = compose_answer_dir(base_dir=base_dir, scale=scale)

    if path.startswith("s3://"):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")
    fs.mkdirs(path, exist_ok=True)

    print(f"Scale: {scale}, Path: {path}")

    with duckdb.connect() as con:
        con.install_extension("tpch")
        con.load_extension("tpch")

        print("Exporting TPC-H answers...")
        answers = con.sql(
            f"""
            SELECT
                query_nr,
                answer
            FROM tpch_answers()
            WHERE scale_factor = {scale};
            """
        ).df()

        for tpl in answers.itertuples():
            query = tpl.query_nr
            answer = tpl.answer
            with fs.open(compose_answer_fpath(base_dir, scale, query), mode="w") as f:
                f.write(answer)
        print("Finished exporting all answers!")


@click.command()
@click.option(
    "--path",
    default=DEFAULT_DATA_BASE_DIR,
    help="Local or S3 base path, will affix '/answers/scale-1' subdirectory to this path",
)
def main(
    path: str,
):
    generate(path)


if __name__ == "__main__":
    main()
