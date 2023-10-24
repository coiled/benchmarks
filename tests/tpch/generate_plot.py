import altair as alt
import click
import pandas as pd


def generate(outfile="chart.json", name=None, scale=None):
    df = pd.read_sql_table(table_name="test_run", con="sqlite:///benchmark.db")

    df = df[
        (df.call_outcome == "passed")
        & (df.path.str.startswith("tpch/"))
        & df.cluster_name
    ]
    df = df[["path", "name", "duration", "start", "cluster_name"]]

    df["library"] = df.path.map(lambda path: path.split("_")[-1].split(".")[0])
    df["query"] = df.name.map(lambda name: int(name.split("_")[-1]))
    df["name"] = df.cluster_name.map(lambda name: name.split("-", 3)[-1])
    df["scale"] = df.cluster_name.map(lambda name: int(name.split("-")[2]))
    del df["path"]
    del df["cluster_name"]

    if name:
        df = df[df.name == name]

    if scale:
        df = df[df.scale == scale]

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
    print("Saving chart to", outfile)


@click.command()
@click.option(
    "--outfile",
    default="chart.json",
    help="Destination file written by Altair. Defaults to chart.json.",
)
def main(outfile):
    return generate(outfile)


if __name__ == "__main__":
    main()
