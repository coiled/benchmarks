import os

import altair as alt
import click
import pandas as pd

try:
    from benchmark_schema import TPCHRun

    TABLENAME = TPCHRun.__tablename__
except ModuleNotFoundError:
    TABLENAME = "tpch_run"


def normalize(df):
    normalize_on = ["query", "scale", "local"]
    dask_durations = df[df["library"] == "dask"].set_index(normalize_on)["duration"]
    data = df.groupby(normalize_on).apply(
        lambda group: group.assign(
            relative_duration=group["duration"] / dask_durations.get(group.name, 0)
        )
    )
    data["relative_diff"] = data["relative_duration"] - 1
    return data.reset_index(drop=True)


def generate(
    outfile="chart.json",
    name=None,
    scale=None,
    db_names=[
        "benchmark.db",
        # "benchmark_local_once.db",
        # "benchmarks_tpch_10000_run1.db",
        # "benchmarks_tpch_10000_run2.db",
        # "benchmarks_tpch_10000_run3.db",
        # "tpch_10000_pyspark.db",
    ],
    export_csv=False,
):
    df = pd.concat(
        [
            pd.read_sql_table(
                table_name=TABLENAME,
                con=f"sqlite:///{db_name}",
            )
            for db_name in db_names
        ]
    )
    df = df[
        (df.call_outcome == "passed")
        & (df.path.str.contains("^tpch/test_(?:dask|duckdb|polars|pyspark)"))
    ]
    df["query"] = df.name.str.extract(r"test_query_(\d+)", expand=True).astype(int)
    df["name"] = df.cluster_name
    df["library"] = df.path.map(lambda path: path.split("_")[-1].split(".")[0])
    df_raw = df.copy()
    df = df[
        [
            "path",
            "name",
            "duration",
            "start",
            "cluster_name",
            "scale",
            "query",
            "library",
            "name",
            "local",
        ]
    ]
    del df["path"]
    del df["cluster_name"]
    df.local = df.local.astype(str)
    # df = df[df.scale == 10_000]
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
    dropdown_local = alt.binding_select(
        options=sorted(df.local.unique()), name="Local: "
    )
    local_select = alt.selection_single(fields=["local"], bind=dropdown_local)
    dropdown_scale = alt.binding_select(
        options=sorted(df.scale.unique()), name="Scale: "
    )
    scale_select = alt.selection_single(
        fields=["scale"], bind=dropdown_scale, name="TPCH Scale"
    )
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
            tooltip=["library", "duration", "relative_duration", "relative_diff"],
        )
        .transform_calculate(y=f"datum[{xcol_param.name}]")
        .add_params(xcol_param)
        .add_selection(local_select)
        .transform_filter(local_select)
        .add_selection(scale_select)
        .transform_filter(scale_select)
    )
    chart = (
        alt.vconcat()
        .properties(title="TPC-H")
        .configure_title(fontSize=20)
        .add_selection(local_select)
        .transform_filter(local_select)
        .add_selection(scale_select)
        .transform_filter(scale_select)
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
    print("Saving chart to", outfile)
    if export_csv:
        csv_path = os.path.splitext(outfile)[0] + ".csv"
        df_raw.to_csv(csv_path, index=False)
        print("Saving csv to", csv_path)


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
    "--export-csv",
    default=False,
    flag_value=True,
)
def main(outfile, scale, export_csv):
    return generate(outfile, scale=scale, export_csv=export_csv)


if __name__ == "__main__":
    main()
