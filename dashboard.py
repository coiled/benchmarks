import glob
import importlib
import inspect
import os
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

    TODO: this will fail if there are multiple tests with the same name,
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
                    source[test] = inspect.getsource(fn)
        except Exception:
            pass
    return source


source = get_test_source()


def make_timeseries(originalname, df, field):
    """
    Make a single timeseries altair chart for a given test.

    originalname: str
        The name of the test without any fixture or other modifications.

    df: pandas.DataFrame
        A dataframe with the test data in it.

    field: str
        The name of the field to chart in the timeseries.
    """
    df = df.fillna({"ci_run_url": "https://github.com/coiled/coiled-runtime"})
    kwargs = {}
    if len(df.name.unique()) > 1:
        kwargs["color"] = altair.Color("name:N")
    return (
        altair.Chart(df, width=600, height=256)
        .mark_line(point=True)
        .encode(
            x=altair.X("start:T"),
            y=altair.Y(f"{field}:Q"),
            href=altair.Href("ci_run_url:N"),
            tooltip=[
                altair.Tooltip("name:N", title="Test Name"),
                altair.Tooltip("call_outcome:N", title="Test Outcome"),
                altair.Tooltip("coiled_runtime:N", title="Coiled Runtime"),
                altair.Tooltip("dask_version:N", title="Dask"),
                altair.Tooltip("duration:Q", title="Duration"),
                altair.Tooltip("ci_run_url:N", title="CI Run URL"),
            ],
            **kwargs,
        )
        .properties(title=originalname)
        .configure(autosize="fit")
    )


def make_test_report(originalname, df):
    """
    Make a tab panel for a single test.

    originalname: str
        The name of the test without any fixture or other modifications.

    df: pandas.DataFrame
        A dataframe with the test data in it.
    """
    fields = {"duration": "Wall Clock"}
    tabs = []
    for field, label in fields.items():
        df = df[~df[field].isna()]
        if not len(df):
            continue
        chart = make_timeseries(originalname, df, field)
        tabs.append((label, chart))

    if originalname in source:
        code = panel.pane.Markdown(
            f"```python\n{source[originalname]}\n```",
            width=600,
            height=384,
            style={"overflow": "auto"},
        )
        tabs.append(("Source", code))
    return panel.Tabs(*tabs, margin=12, width=600)


if __name__ == "__main__":
    DB_NAME = (
        sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DB_NAME", "benchmark.db")
    )
    engine = sqlalchemy.create_engine(f"sqlite:///{DB_NAME}")
    df = pandas.read_sql_table("test_run", engine)
    grouped = df.groupby("originalname")
    panes = [make_test_report(name, grouped.get_group(name)) for name in grouped.groups]
    flex = panel.FlexBox(*panes, align_items="start", justify_content="start")

    flex.save(DB_NAME[: -len(".db")] + ".html", resources=INLINE)
