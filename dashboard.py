import glob
import importlib
import inspect

import altair
import pandas
import panel
import sqlalchemy
from bokeh.resources import INLINE

panel.extension("vega")

engine = sqlalchemy.create_engine("sqlite:///benchmark.db")


def get_test_source():
    source: dict[str, str] = {}
    files = glob.glob("tests/**/test_*.py", recursive=True)
    for f in files:
        try:
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
    df = pandas.read_sql_table("test_run", engine)
    grouped = df.groupby("originalname")
    panes = [make_test_report(name, grouped.get_group(name)) for name in grouped.groups]
    flex = panel.FlexBox(*panes, align_items="start", justify_content="space-between")

    flex.save("benchmarks.html", resources=INLINE)
