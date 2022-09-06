from __future__ import annotations

import argparse
import collections
import glob
import importlib
import inspect
import pathlib
from typing import Literal

import altair
import pandas
import panel
import sqlalchemy
from bokeh.resources import INLINE

panel.extension("vega")


source: dict[str, str] = {}


def load_test_source() -> None:
    """Crawl the tests directory and try to grab code for each test. This relies on the
    tests being importable from this script.
    """
    for fname in glob.iglob("tests/**/test_*.py", recursive=True):
        mod = importlib.import_module(fname.replace("/", ".")[: -len(".py")])
        tests = [a for a in dir(mod) if a.startswith("test_")]
        for test in tests:
            if (func := getattr(mod, test, None)) and callable(func):
                # FIXME missing decorators, namely @pytest.mark.parametrize
                source[fname[len("tests/") :] + "::" + test] = inspect.getsource(func)


def make_barchart(
    df: pandas.DataFrame, spec: altair.ChartSpec, title: str
) -> altair.Chart | None:
    """Make a single Altair barchart for a given test."""
    df = df.dropna(subset=[spec.field, "start"])
    if not len(df):
        # Some tests do not have average_memory or peak_memory measures, only runtime
        return None

    df = df.sort_values("runtime", key=runtime_sort_key_pd)
    df = df.assign(**{spec.field: df[spec.field] / spec.scale})
    df = df[
        [
            spec.field,
            "name",
            "dask_version",
            "distributed_version",
            "runtime",
        ]
    ]
    # Altair will re-sort alphabetically, unless it's explicitly asked to sort by
    # something else. Could not find an option to NOT sort.
    df["order"] = pandas.RangeIndex(0, df.shape[0])
    height = df.shape[0] * 20 + 50
    return (
        altair.Chart(df, width=800, height=height)
        .mark_bar()
        .encode(
            x=altair.X(spec.field, title=spec.label),
            y=altair.Y("runtime", sort=altair.EncodingSortField(field="order")),
            tooltip=[
                altair.Tooltip("dask_version:N", title="Dask"),
                altair.Tooltip("distributed_version:N", title="Distributed"),
                altair.Tooltip(f"{spec.field}:Q", title=spec.label),
            ],
        )
        .properties(title=title)
        .configure(autosize="fit")
    )


def make_timeseries(
    df: pandas.DataFrame, spec: altair.ChartSpec, title: str
) -> altair.Chart | None:
    """Make a single Altair timeseries chart for a given test"""
    df = df.dropna(subset=[spec.field, "start"])
    if not len(df):
        # Some tests do not have average_memory or peak_memory measures, only runtime
        return None

    df = df.assign(**{spec.field: df[spec.field] / spec.scale})
    df = df.fillna({"ci_run_url": "https://github.com/coiled/coiled-runtime"})
    kwargs = {}
    # Reduce the size of the altair spec
    df = df[
        [
            spec.field,
            "start",
            "ci_run_url",
            "name",
            "call_outcome",
            "coiled_runtime_version",
            "dask_version",
            "distributed_version",
        ]
    ]
    if len(df.name.unique()) > 1:
        kwargs["color"] = altair.Color("name:N")
    if len(df.call_outcome.unique()) > 1:
        kwargs["shape"] = altair.Shape(
            "call_outcome:N",
            scale=altair.Scale(domain=["passed", "failed"], range=["circle", "cross"]),
            title="Outcome",
        )
    return (
        altair.Chart(df, width=800, height=256)
        .mark_line(point=altair.OverlayMarkDef(size=64))
        .encode(
            x=altair.X("start:T"),
            y=altair.Y(f"{spec.field}:Q", title=spec.label),
            href=altair.Href("ci_run_url:N"),
            tooltip=[
                altair.Tooltip("name:N", title="Test Name"),
                altair.Tooltip("start:T", title="Date"),
                altair.Tooltip("call_outcome:N", title="Test Outcome"),
                altair.Tooltip("coiled_runtime_version:N", title="Coiled Runtime"),
                altair.Tooltip("dask_version:N", title="Dask"),
                altair.Tooltip("distributed_version:N", title="Distributed"),
                altair.Tooltip(f"{spec.field}:Q", title=spec.label),
                altair.Tooltip("ci_run_url:N", title="CI Run URL"),
            ],
            **kwargs,
        )
        .properties(title=title)
        .configure(autosize="fit")
    )


def make_test_report(
    df: pandas.DataFrame,
    kind: Literal["barchart" | "timeseries"],
    title: str,
    sourcename: str,
) -> panel.Tabs:
    """Make a tab panel for a single test"""
    ChartSpec = collections.namedtuple("ChartSpec", ["field", "scale", "label"])
    specs = [
        ChartSpec("duration", 1, "Wall Clock (s)"),
        ChartSpec("average_memory", 1024**3, "Average Memory (GiB)"),
        ChartSpec("peak_memory", 1024**3, "Peak Memory (GiB)"),
    ]
    tabs = []
    for s in specs:
        if kind == "timeseries":
            chart = make_timeseries(df, s, title)
        else:
            chart = make_barchart(df, s, title)
        if not chart:
            continue
        tabs.append((s.label, chart))

    if kind == "timeseries":
        height = 384
    else:
        height = df.shape[0] * 20 + 50

    if sourcename in source:
        code = panel.pane.Markdown(
            f"```python\n{source[sourcename]}\n```",
            width=800,
            height=height,
            style={"overflow": "auto"},
        )
        tabs.append(("Source", code))
    else:
        print("Source code not found for", sourcename)
    return panel.Tabs(*tabs, margin=12, width=800)


def make_timeseries_html_report(
    df: pandas.DataFrame, output_dir: pathlib.Path, runtime: str
) -> None:
    """Generate HTML report for one runtime (e.g. coiled-upstream-py3.9), showing
    evolution of measures (wall clock, average memory, peak memory) over historical CI
    runs.

    Create one tab for each test category (e.g. benchmarks, runtime, stability),
    one graph for each test,
    and one graph tab for each measure (wall clock, average memory, peak memory).
    """
    out_fname = str(output_dir.joinpath(runtime + ".html"))
    print(f"Generating {out_fname}")
    categories = sorted(df[df.runtime == runtime].category.unique())
    tabs = []
    for category in categories:
        df_by_test = (
            df[(df.runtime == runtime) & (df.category == category)]
            .sort_values("sourcename")
            .groupby("sourcename")
        )
        panes = [
            make_test_report(
                df_by_test.get_group(sourcename),
                kind="timeseries",
                title=sourcename,
                sourcename=sourcename,
            )
            for sourcename in df_by_test.groups
        ]
        flex = panel.FlexBox(*panes, align_items="start", justify_content="start")
        tabs.append((category.title(), flex))
    doc = panel.Tabs(*tabs, margin=12)

    doc.save(out_fname, title=runtime, resources=INLINE)


def make_ab_html_report(
    df: pandas.DataFrame,
    output_dir: pathlib.Path,
    by_test: bool,
    baseline: str | None,
) -> None:
    """Generate HTML report for the latest CI run, comparing all runtimes (e.g.
    coiled-upstream-py3.9) against a baseline runtime

    Create one tab for each test category (e.g. benchmarks, runtime, stability),
    one graph for each runtime and one bar for each test
    OR one graph for each test and one bar for each runtime,
    and one graph tab for each measure (wall clock, average memory, peak memory).

    If a baseline runtime is defined, all measures are expressed relative to the
    baseline; otherwise they're expressed in absolute terms.
    """
    out_fname = str(
        output_dir.joinpath(
            "AB_by_"
            + ("test" if by_test else "runtime")
            + (f"_vs_{baseline}" if baseline else "")
            + ".html"
        )
    )
    print(f"Generating {out_fname}")

    categories = sorted(df.category.unique())
    tabs = []
    for category in categories:
        if by_test:
            df_by_test = (
                df[df.category == category]
                .sort_values(["sourcename", "fullname"])
                .groupby(["sourcename", "fullname"])
            )
            panes = [
                make_test_report(
                    df_by_test.get_group((sourcename, fullname)),
                    kind="barchart",
                    title=fullname,
                    sourcename=sourcename,
                )
                for sourcename, fullname in df_by_test.groups
            ]
        else:
            return  # WIP
        flex = panel.FlexBox(*panes, align_items="start", justify_content="start")
        tabs.append((category.title(), flex))
    doc = panel.Tabs(*tabs, margin=12)

    doc.save(
        out_fname,
        title="A/B by "
        + ("test" if by_test else "runtime")
        + (f" vs. {baseline}" if baseline else ""),
        resources=INLINE,
    )


def make_index_html_report(
    output_dir: pathlib.Path, runtimes: list[str], baselines: list[str]
) -> None:
    """Generate index.html"""
    index_txt = """# Coiled Runtime Benchmarks\n"""
    index_txt += "### Historical timeseries\n"
    for runtime in runtimes:
        index_txt += f"- [{runtime}](./{runtime}.html)\n"
    index_txt += "\n\n### A/B tests\n"
    for baseline in baselines:
        index_txt += (
            f"- [by runtime vs. {baseline}](./AB_by_runtime_vs_{baseline}.html)\n"
        )
    index_txt += "- [by test](./AB_by_test.html)\n"

    index = panel.pane.Markdown(index_txt, width=800)
    out_fname = str(output_dir.joinpath("index.html"))
    print(f"Generating {out_fname}")
    index.save(
        out_fname,
        title="Coiled Runtime Benchmarks",
        resources=INLINE,
    )


def runtime_sort_key(runtime: str) -> tuple:
    """Runtimes are in the format coiled-<version>-py<python version>
    e.g. coiled-latest-py3.8

    Sort them by version descending and by python version ascending
    """
    t = runtime.split("-")
    assert len(t) == 3
    assert t[0] == "coiled"
    # upstream > latest > 0.1.0 > 0.0.4
    if t[1] == "upstream":
        coiled_version = [-2]
    elif t[1] == "latest":
        coiled_version = [-1]
    else:
        coiled_version = [0] + [-int(v) for v in t[1].split(".")]

    assert t[2][:2] == "py"
    py_version = [int(v) for v in t[2][2:].split(".")]
    return coiled_version, py_version


def runtime_sort_key_pd(s: pandas.Series) -> pandas.Series:
    return pandas.Series([runtime_sort_key(v) for v in s], index=s.index)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a static HTML report comparing metrics from the runs"
    )
    parser.add_argument(
        "--db-file",
        "-d",
        help="Path to SQLite database file containing the metrics",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        help="Output directory",
        default="build/html",
    )
    parser.add_argument(
        "--baseline",
        "-b",
        nargs="+",
        default=[],
        help="Baseline runtime(s) for A/B comparison",
    )
    parser.add_argument(
        "--pickle",
        action="store_true",
        help="Dump raw dataframe to pickle file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    load_test_source()
    print(f"Discovered {len(source)} tests")

    # Load SQLite database into a pandas DataFrame
    engine = sqlalchemy.create_engine(f"sqlite:///{args.db_file}")
    df = pandas.read_sql(
        "select * from test_run where platform = 'linux' "
        "and call_outcome in ('passed', 'failed')",
        engine,
    )
    df = df.assign(
        runtime=(
            "coiled-"
            + df.coiled_runtime_version
            + "-py"
            + df.python_version.str.split(".", n=2).str[:2].str.join(".")
        ),
        category=df.path.str.split("/", n=1).str[0],
    )
    df["start"] = pandas.to_datetime(df["start"])
    df["end"] = pandas.to_datetime(df["end"])
    df["sourcename"] = df["path"].str.cat(df["originalname"], "::")
    df["fullname"] = df["path"].str.cat(df["name"], "::")
    df = df.set_index("id")

    if args.pickle:
        out_fname = str(output_dir.joinpath("records.pickle"))
        print(f"Generating {out_fname}")
        df.to_pickle(out_fname)

    # Generate HTML pages
    runtimes = sorted(df.runtime.unique(), key=runtime_sort_key)
    for runtime in runtimes:
        make_timeseries_html_report(df, output_dir, runtime)

    # Select only the latest run for each runtime. This may pick up historical runs (up
    # to 2d old) if they have not been rerun in the current pull/PR.
    max_end = df.sort_values("end").groupby(["runtime", "category"]).tail(1)
    max_end = max_end[max_end["end"] > max_end["end"].max() - pandas.Timedelta("2d")]
    session_ids = max_end["session_id"].unique()
    latest_run = df[df["session_id"].isin(session_ids)]

    for baseline in args.baseline:
        make_ab_html_report(latest_run, output_dir, by_test=False, baseline=baseline)
    make_ab_html_report(latest_run, output_dir, by_test=True, baseline=None)

    make_index_html_report(output_dir, runtimes, args.baseline)


if __name__ == "__main__":
    main()
