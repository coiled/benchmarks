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
    return (
        altair.Chart(df, width=512, height=256)
        .mark_line()
        .encode(
            x=altair.X("start:T"),
            y=altair.Y(f"{field}:Q"),
            color=altair.Color("name:N")
            if len(df.name.unique()) > 1
            else altair.Undefined,
        )
        .properties(title=originalname)
    )


def make_test_report(originalname, df):
    fields = {"duration": "Wall Clock"}
    tabs = []
    print(originalname)
    for field, label in fields.items():
        chart = make_timeseries(originalname, df, field)
        tabs.append((label, chart))

    if originalname in source:
        code = panel.pane.Markdown(f"```python\n{source[originalname]}\n```")
        tabs.append(("Source", code))
    return panel.Tabs(*tabs)


if __name__ == "__main__":
    df = pandas.read_sql_table("test_run", engine)
    grouped = df.groupby("originalname")
    panes = [make_test_report(name, grouped.get_group(name)) for name in grouped.groups]
    flex = panel.FlexBox(*panes)

    flex.save("benchmarks.html", resources=INLINE)
