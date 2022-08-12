from __future__ import annotations

import collections
import glob
import importlib
import inspect
import os
import pathlib
import sys

import altair
import pandas
import panel
import sqlalchemy
from bokeh.resources import INLINE

panel.extension("vega")


def get_test_source():
    """
    Crawl the tests directory and try to grab code for each test on a best-effort
    basis. This relies on the tests being importable from this script, so the
    environment should be similar enough that that is possible.
    """
    source: dict[str, str] = {}
    files = glob.glob("tests/**/test_*.py", recursive=True)
    for f in files:
        try:
            # Fragile!
            mod = importlib.import_module(f.replace("/", ".")[: -len(".py")])
            tests = [a for a in dir(mod) if a.startswith("test_")]
            for test in tests:
                if fn := getattr(mod, test, None):
                    if not callable(fn):
                        continue
                    source[f[len("tests/") :] + "::" + test] = inspect.getsource(fn)
        except Exception:
            pass
    return source


source = get_test_source()


def make_timeseries(originalname, df, spec) -> altair.Chart | None:
    """
    Make a single timeseries altair chart for a given test.

    originalname: str
        The name of the test without any fixture or other modifications.

    df: pandas.DataFrame
        A dataframe with the test data in it.

    spec: ChartSpec
        Data for how to render the timeseries
    """
    df = df.dropna(subset=[spec.field, "start"])
    if not len(df):
        return None
    df = df.assign(**{spec.field: df[spec.field] / spec.scale})
    df = df.fillna({"ci_run_url": "https://github.com/coiled/coiled-runtime"})
    path = df.path.iloc[0]
    kwargs = {}
    if len(df.name.unique()) > 1:
        kwargs["color"] = altair.Color("name:N")
    return (
        altair.Chart(df, width=800, height=256)
        .mark_line(point=True)
        .encode(
            x=altair.X("start:T"),
            y=altair.Y(f"{spec.field}:Q", title=spec.label),
            href=altair.Href("ci_run_url:N"),
            tooltip=[
                altair.Tooltip("name:N", title="Test Name"),
                altair.Tooltip("call_outcome:N", title="Test Outcome"),
                altair.Tooltip("coiled_runtime_version:N", title="Coiled Runtime"),
                altair.Tooltip("dask_version:N", title="Dask"),
                altair.Tooltip(f"{spec.field}:Q", title=spec.label),
                altair.Tooltip("ci_run_url:N", title="CI Run URL"),
            ],
            **kwargs,
        )
        .properties(title=f"{path}::{originalname}")
        .configure(autosize="fit")
    )


def make_test_report(group_keys, df):
    """
    Make a tab panel for a single test.

    originalname: str
        The name of the test without any fixture or other modifications.

    df: pandas.DataFrame
        A dataframe with the test data in it.
    """
    path, originalname = group_keys

    ChartSpec = collections.namedtuple("ChartSpec", ["field", "scale", "label"])
    specs = [
        ChartSpec("duration", 1, "Wall Clock (s)"),
        ChartSpec("average_memory", 1024**3, "Average Memory (GiB)"),
        ChartSpec("peak_memory", 1024**3, "Peak Memory (GiB)"),
    ]
    tabs = []
    for s in specs:
        chart = make_timeseries(originalname, df, s)
        if not chart:
            continue
        tabs.append((s.label, chart))

    sourcename = path + "::" + originalname
    if sourcename in source:
        code = panel.pane.Markdown(
            f"```python\n{source[sourcename]}\n```",
            width=800,
            height=384,
            style={"overflow": "auto"},
        )
        tabs.append(("Source", code))
    return panel.Tabs(*tabs, margin=12, width=800)


if __name__ == "__main__":
    DB_NAME = (
        sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DB_NAME", "benchmark.db")
    )
    static = pathlib.Path("static")
    static.mkdir(exist_ok=True)

    engine = sqlalchemy.create_engine(f"sqlite:///{DB_NAME}")
    df = pandas.read_sql("select * from test_run where platform = 'linux'", engine)
    df = df.assign(
        runtime=(
            "coiled-"
            + df.coiled_runtime_version
            + "-py"
            + df.python_version.str.split(".", n=2).str[:2].str.join(".")
        ),
        category=df.path.str.split("/", n=1).str[0],
    )

    runtimes = list(df.runtime.unique())
    for runtime in runtimes:
        print(f"Generating dashboard for {runtime}")
        categories = df[df.runtime == runtime].category.unique()
        tabs = []
        for category in categories:
            by_test = (
                df[(df.runtime == runtime) & (df.category == category)]
                .sort_values(["path", "originalname"])
                .groupby(["path", "originalname"])
            )
            panes = [
                make_test_report(test_name, by_test.get_group(test_name))
                for test_name in by_test.groups
            ]
            flex = panel.FlexBox(*panes, align_items="start", justify_content="start")
            tabs.append((category.title(), flex))
        doc = panel.Tabs(*tabs, margin=12)

        doc.save(
            str(static.joinpath(runtime + ".html")), title=runtime, resources=INLINE
        )
    index = """# Coiled Runtime Benchmarks\n\n"""
    index += "\n\n".join([f"[{r}](./{r}.html)" for r in reversed(sorted(runtimes))])
    index = panel.pane.Markdown(index, width=800)
    index.save(
        str(static.joinpath("index.html")),
        title="Coiled Runtime Benchmarks",
        resources=INLINE,
    )
