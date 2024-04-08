import altair as alt
import click
import pandas as pd

try:
    from benchmark_schema import TPCHRun

    TABLENAME = TPCHRun.__tablename__
except ModuleNotFoundError:
    TABLENAME = "tpch_run"


def generate(outfile="chart.json", name=None, scale=None, db_name="benchmark.db"):
    df = pd.read_sql_table(
        table_name=TABLENAME,
        con=f"sqlite:///{db_name}",
    )

    df = df[
        (df.call_outcome == "passed")
        & (df.path.str.contains("^tpch/test_(?:dask|duckdb|polars|pyspark)"))
    ]
    df = df[["path", "name", "duration", "start", "cluster_name", "scale"]]
    df["library"] = df.path.map(lambda path: path.split("_")[-1].split(".")[0])
    df["query"] = df.name.map(lambda name: int(name.split("_")[-1]))
    df["name"] = df.cluster_name

    del df["path"]
    del df["cluster_name"]

    # if name:
    #     df = df[df.name == name]

    if scale:
        df = df[df.scale == scale]
    if df.empty:
        raise RuntimeError("No data.")

    df = df.sort_values(["query", "library"])

    def recent(df):
        return df.sort_values("start").iloc[-1]

    df = df.groupby(["library", "query"]).apply(recent).reset_index(drop=True)
    del df["start"]

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="query:N",
            y="duration:Q",
            xOffset="library:N",
            color=alt.Color("library").scale(
                domain=["dask", "duckdb", "polars", "pyspark"],
                range=["#5677a4ff", "#e68b39ff", "#d4605bff", "#82b5b2ff"],
            ),
            tooltip=["library", "duration"],
        )
        .properties(title=f"TPC-H -- scale:{df.scale.iloc[0]} name:{df.name.iloc[0]}")
        .configure_title(
            fontSize=20,
        )
    )
    chart.save(outfile)
    chart.save(outfile.replace(".json", ".html"))
    print("Saving chart to", outfile)


@click.command()
@click.option(
    "--outfile",
    default="chart.json",
    help="Destination file written by Altair. Defaults to chart.json.",
)
@click.option(
    "--scale",
    default=10,
    type=int,
)
def main(outfile, scale):
    return generate(outfile, scale=scale)


if __name__ == "__main__":
    main()
