import altair as alt
import click
import pandas as pd

try:
    from benchmark_schema import TPCHRun

    TABLENAME = TPCHRun.__tablename__
except ModuleNotFoundError:
    TABLENAME = "tpch_run"


def normalize(df):
    dask_durations = df[df["library"] == "dask"].set_index("query")["duration"]
    data = df.groupby("query").apply(
        lambda group: group.assign(
            relative_duration=group["duration"] / dask_durations[group.name]
        )
    )
    data["relative_diff"] = data["relative_duration"] - 1
    return data.reset_index(drop=True)


def generate(
    outfile="chart.json", name=None, scale=None, db_name="benchmark.db", relative=False
):
    df = pd.read_sql_table(
        table_name=TABLENAME,
        con=f"sqlite:///{db_name}",
    )
    df = df[
        (df.call_outcome == "passed")
        & (df.path.str.contains("^tpch/test_(?:dask|duckdb|polars|pyspark)"))
    ]
    df["query"] = df.name.str.extract(r"test_query_(\d+)", expand=True).astype(int)
    df = df[["path", "name", "duration", "start", "cluster_name", "scale", "query"]]
    df["library"] = df.path.map(lambda path: path.split("_")[-1].split(".")[0])
    df["name"] = df.cluster_name
    del df["path"]
    del df["cluster_name"]

    if scale:
        df = df[df.scale == scale]
    if df.empty:
        raise RuntimeError("No data.")

    df = df.sort_values(["query", "library"])

    def recent(df):
        return df.sort_values("start").iloc[-1]

    df = df.groupby(["library", "query"]).apply(recent).reset_index(drop=True)
    del df["start"]
    df = normalize(df)
    dropdown = alt.binding_select(
        options=["duration", "relative_duration", "relative_diff"], name="Y scale: "
    )
    xcol_param = alt.param(value="duration", bind=dropdown)
    color = alt.Color("library").scale(
        domain=["dask", "duckdb", "polars", "pyspark"],
        range=["#5677a4", "#e68b39", "#d4605b", "green"],
    )
    base = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="query:N",
            y=alt.Y("y:Q", title="Wall time"),
            xOffset="library:N",
            color=color,
            tooltip=["library", "duration"],
        )
        .transform_calculate(y=f"datum[{xcol_param.name}]")
        .add_params(xcol_param)
    )
    chart = (
        alt.vconcat()
        .properties(title=f"TPC-H -- scale:{df.scale.iloc[0]} name:{df.name.iloc[0]}")
        .configure_title(
            fontSize=20,
        )
    )
    chart &= base.transform_filter(alt.FieldLTEPredicate(field="query", lte=11))
    chart &= base.transform_filter(alt.FieldGTPredicate(field="query", gt=11))

    relative = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            y="library:N",
            x="average(relative_diff):Q",
            color=color,
            tooltip=["library", "average(relative_diff):Q"],
        )
    )
    chart &= relative
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
@click.option(
    "--relative",
    default=False,
    flag_value=True,
)
def main(outfile, scale, relative):
    return generate(outfile, scale=scale, relative=relative)


if __name__ == "__main__":
    main()
