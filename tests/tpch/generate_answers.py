import pathlib

import click
import duckdb

VERIFICATION_SCALE = 1


def generate(base_path: pathlib.Path) -> pathlib.Path:
    scale = VERIFICATION_SCALE
    path = base_path / "answers" / f"scale-{scale}"
    path.mkdir(parents=True, exist_ok=True)

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
            with (path / f"q{query}.out").open(mode="w") as f:
                f.write(answer)
        print("Finished exporting all answers!")
    return path


@click.command()
@click.option(
    "--path",
    default="./tests/tpch",
    help="Local or S3 base path, will affix '/answers' subdirectory to this path",
)
def main(
    path: str,
):
    generate(path)


if __name__ == "__main__":
    main()
