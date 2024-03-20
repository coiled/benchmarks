import altair as alt
import pandas as pd

LIBRARY_COLORS = {
    "dask": "#5677a4",
    "duckdb": "#e68b39",
    "polars": "#d4605b",
    "pyspark": "green",
}


def from_db(path):
    df = pd.read_sql_table(table_name="test_run", con=f"sqlite:///{path}")

    df = df[
        (df.call_outcome == "passed")
        & (df.path.str.contains("^tpch/test_(?:dask|duckdb|polars|pyspark)"))
        & df.cluster_name
    ]
    df = df[["path", "name", "duration", "start", "cluster_name"]]
    df["library"] = df.path.map(lambda path: path.split("_")[-1].split(".")[0])
    df["query"] = df.name.map(lambda name: int(name.split("_")[-1]))
    df["name"] = df.cluster_name.map(lambda name: name.split("-", 3)[-1])
    df["scale"] = df.cluster_name.map(lambda name: int(name.split("-")[2]))
    del df["path"]
    del df["cluster_name"]
    return df


def latest(df, n=1):
    df = df.sort_values(["query", "library"])

    def recent(df):
        return df.sort_values("start").tail(n)

    df = df.groupby(["library", "query"]).apply(recent).reset_index(drop=True)
    del df["start"]
    return df


def normalize(df):
    dask_durations = df[df["library"] == "dask"].set_index("query")["duration"]
    data = df.groupby("query").apply(
        lambda group: group.assign(
            relative_duration=group["duration"] / dask_durations[group.name]
        )
    )
    return data.reset_index(drop=True)


def subplot(df, column, libraries):
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="query:N",
            y=f"{column}:Q",
            xOffset="library:N",
            color=alt.Color("library").scale(
                domain=libraries,
                range=[LIBRARY_COLORS[lib] for lib in libraries],
            ),
            tooltip=["library", column],
        )
    )


def plot(df, libraries=None, column="duration"):
    if libraries is None:
        libraries = ["dask", "duckdb", "polars", "pyspark"]
    plot = subplot(df[df["query"] < 12], column=column, libraries=libraries) & subplot(
        df[df["query"] >= 12], column=column, libraries=libraries
    )
    return plot.properties(
        title=f"TPC-H -- scale:{df.scale.iloc[0]} name:{df.name.iloc[0]}"
    ).configure_title(
        fontSize=20,
    )
