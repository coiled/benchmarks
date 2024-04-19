import os

import altair as alt
import click
import pandas as pd

try:
    from benchmark_schema import TPCHRun

    TABLENAME = TPCHRun.__tablename__
except ModuleNotFoundError:
    TABLENAME = "tpch_run"


def normalize(df, normalize_on):
    dask_durations = df[df["library"] == "dask"][normalize_on + ["duration", "library"]]
    frames = [dask_durations.copy()]
    del dask_durations["library"]
    for lib in df["library"].unique():
        if lib == "dask":
            continue
        other_durations = df[df["library"] == lib][
            normalize_on + ["duration", "library"]
        ]
        merged = dask_durations.merge(
            other_durations, on=normalize_on, how="outer", suffixes=("", "_other")
        )

        # merged["duration_other"] = merged["duration_other"].fillna(20_000)
        merged["library"] = merged["library"].fillna(lib)
        merged["relative_duration"] = merged["duration_other"] / merged["duration"]
        merged["relative_diff"] = merged["relative_duration"] - 1
        merged["duration"] = merged["duration_other"]
        merged = merged.drop(columns=["duration_other"])
        frames.append(merged)
    data = pd.concat(frames)
    return data


cpus_per_worker = {
    "m6i.xlarge": 4,
    "r6i.2xlarge": 8,
    "m6i.2xlarge": 8,
}

num_workers = {
    1: 4,
    10: 8,
    100: 16,
    1000: 32,
    10000: 32,
}


def generate(
    outfile="chart.json",
    name=None,
    scale=None,
    db_names=[
        "benchmark.db",
        "tpch_100.db",
        "tpch_1000.db",
        # "tpch_10000_pyspark_1_to_9.db",
        # "tpch_10000_pyspark_10_17.db",
        # "tpch_10000_pyspark_19_21.db",
        # "tpch_10000_dask_full.db",
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
    ).drop_duplicates(ignore_index=True)
    df = df[
        (df.call_outcome == "passed")
        & (df.path.str.contains("^tpch/test_(?:dask|duckdb|polars|pyspark)"))
    ]
    all_columns = [
        "average_memory",
        # 'call_outcome',
        # 'ci_run_url',
        "cluster_details_url",
        # 'cluster_disk_size',
        # 'cluster_id',
        "cluster_name",
        # 'compression',
        "dask_expr_version",
        "dask_version",
        "distributed_version",
        "duckdb_version",
        "duration",
        # 'end',
        # 'id',
        "local",
        "n_workers",
        "name",
        "originalname",
        # 'partition_size',
        "path",
        "peak_memory",
        "platform",
        "polars_version",
        "pyspark_version",
        "python_version",
        "query",
        "scale",
        # 'session_id',
        # 'setup_outcome',
        "start",
        # 'teardown_outcome',
        "worker_vm_type",
    ]
    df = df[all_columns]
    df["library"] = df.path.map(lambda path: path.split("_")[-1].split(".")[0])

    default_num_workers = {
        1: 4,
        10: 8,
        100: 16,
        1000: 32,
        10000: 32,
    }
    cpus_per_worker = {
        "M1": 8,
        "m6i.large": 2,
        "m6i.xlarge": 4,
        "r6i.2xlarge": 8,
        "m6i.2xlarge": 8,
        "m6i.8xlarge": 32,
        "m6i.32xlarge": 128,
    }
    default_worker_type = {
        1: "m6i.large",
        10: "m6i.large",
        100: "m6i.large",
        1000: "m6i.xlarge",
        10000: "m6i.xlarge",
    }
    df.loc[df.local, "worker_vm_type"] = "M1"
    df.loc[df.local, "n_workers"] = 1
    df["worker_vm_type"] = df["worker_vm_type"].fillna(
        df["scale"].map(default_worker_type)
    )
    df["n_workers"] = (
        df["n_workers"].fillna(df["scale"].map(default_num_workers)).astype(int)
    )
    df["num_cpus"] = (
        df["worker_vm_type"].map(cpus_per_worker) * df["n_workers"]
    ).astype(int)

    df["testrun"] = (
        "Cloud - Scale: "
        + df["scale"].astype(str)
        + " - Cluster "
        + df["num_cpus"].astype(str)
        + " CPUs"
    )
    df.loc[df.local, "testrun"] = "Local - Scale: " + df["scale"].astype(str)

    df = (
        df.groupby(
            [
                # 'duration',
                "library",
                "local",
                "n_workers",
                "num_cpus",
                "query",
                "scale",
                "testrun",
                "worker_vm_type",
            ]
        )
        .duration.mean()
        .reset_index()
    )

    isdask = df.library == "dask"
    df_others = df[~isdask]
    df_dask = df[isdask]
    columns = list(df_others.columns)
    merged = df_others.merge(
        df_dask,
        on=[
            "local",
            "query",
            "testrun",
        ],
        how="outer",
        suffixes=("", "_dask"),
    )
    merged["relative_diff"] = (merged["duration"] / merged["duration_dask"]) - 1
    columns += ["relative_diff"]
    data = pd.concat([df_dask, merged[columns]]).drop_duplicates()
    data = data.dropna(subset=["library"])
    data = data.sort_values(by=["testrun", "query", "library"])
    df_raw = data.copy()

    df = data
    df.local = df.local.astype(str)
    dropdown_local = alt.binding_select(
        options=sorted(df.testrun.unique()), name="Run: "
    )
    local_select = alt.selection_point(fields=["testrun"], bind=dropdown_local)

    color = alt.Color("library").scale(
        domain=[
            "dask",
            "duckdb",
            "polars",
            "pyspark",
        ],
        range=[
            "#5677a4",
            "#e68b39",
            "#d4605b",
            "green",
        ],
    )
    base = (
        alt.Chart(df, width=alt.Step(10))
        .mark_bar(size=12, clip=True)
        .add_params(local_select)
        .transform_filter(local_select)
    )

    chart = alt.vconcat().properties(title="TPC-H").configure_title(fontSize=20)
    chart &= base.encode(
        x=alt.X("query:O", axis=alt.Axis(grid=True), title=""),
        y=alt.Y(
            "duration:Q",
            title="Wall time",
        ),
        xOffset="library:N",
        color=color,
        tooltip=["library", "duration", "relative_diff"],
    ).properties(width=800)

    chart &= base.encode(
        x=alt.X("query:O", axis=alt.Axis(grid=True)),
        y=alt.Y(
            "relative_diff:Q",
            title="(Other / Dask) - 1",
        ),
        xOffset="library:N",
        color=color,
        tooltip=["library", "duration", "relative_diff"],
    ).properties(height=80, width=800)

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
