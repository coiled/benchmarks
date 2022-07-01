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


def make_timeseries(originalname, df):
    return (
        altair.Chart(df, width=512, height=256)
        .mark_line()
        .encode(
            x=altair.X("start:T"),
            y=altair.Y("duration:Q"),
            color=altair.Color("name:N")
            if len(df.name.unique()) > 1
            else altair.Undefined,
        )
        .properties(title=originalname)
    )


def make_test_report(originalname, df):
    chart = make_timeseries(originalname, df)
    code = panel.pane.Markdown(f"```python\n{source[originalname]}\n```")
    return panel.Tabs(
        ("Timeseries", chart),
        ("Source", code),
    )


df = pandas.read_sql_table("test_run", engine)
grouped = df.groupby("originalname")
panes = [make_test_report(name, grouped.get_group(name)) for name in grouped.groups]
panes = panes * 4
flex = panel.FlexBox(*panes)

flex.save("test.html", resources=INLINE)
