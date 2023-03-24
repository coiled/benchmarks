from __future__ import annotations

import argparse
import glob
import importlib
import inspect
import math
import operator
import pathlib
from collections.abc import Callable
from textwrap import dedent
from typing import Any, Literal, NamedTuple, Optional
from urllib.parse import quote

import altair
import numpy
import pandas
import panel
import sqlalchemy
from bokeh.resources import Resources

CDN = Resources("cdn")

altair.data_transformers.enable("default", max_rows=None)
panel.extension("vega")


class ChartSpec(NamedTuple):
    field_name: str
    field_desc: str
    unit: str
    scale: float


SPECS = [
    ChartSpec("duration", "Wall Clock", "(s)", 1),
    ChartSpec("average_memory", "Average Memory", "(GiB)", 2**30),
    ChartSpec("peak_memory", "Peak Memory", "(GiB)", 2**30),
]
OLD_PROMETHEUS_DATASOURCE = "AWS Prometheus - Sandbox (us east 2)"

source: dict[str, str] = {}


def load_test_source() -> None:
    """Crawl the tests directory and try to grab code for each test. This relies on the
    tests being importable from this script.
    """
    for fname in glob.iglob("tests/**/test_*.py", recursive=True):
        try:
            mod = importlib.import_module(fname.replace("/", ".")[: -len(".py")])
        # Some pytest exceptions inherit directly from BaseException
        except BaseException as e:
            print(f"Could not import {fname}: {e.__class__.__name__}: {e}")
            continue
        tests = [a for a in dir(mod) if a.startswith("test_")]
        for test in tests:
            if (func := getattr(mod, test, None)) and callable(func):
                # FIXME missing decorators, namely @pytest.mark.parametrize
                source[fname[len("tests/") :] + "::" + test] = inspect.getsource(func)

    print(f"Discovered {len(source)} tests")


def calc_ab_confidence_intervals(
    df: pandas.DataFrame, field_name: str, A: str, B: str
) -> pandas.DataFrame:
    """Calculate p(B / A - 1) > x and p(B / A - 1) < -x for discrete x, where A and B
    are runtimes, for all tests in df.

    Algorithm
    ---------
    https://towardsdatascience.com/a-practical-guide-to-a-b-tests-in-python-66666f5c3b02

    Returns
    -------
    DataFrame:

    fullname
        Test name with category, e.g. bencharks/test_foo.py::test_123[1]
    fullname_no_category
        Test name without category, e.g. test_foo.py::test_123[1]
    x
        Confidence interval [-0.5, 0.5]. Note that element 0 will be repeated.
    xlabel
        "<-{p*100}% | x < 0
        ">{p*100}% | x > 0
    p
        p(B/A-1) < x | x < 0
        p(B/A-1) > x | x > 0
    color
        0 if p=1 and x < 0
        0.5 if p=0
        1 if p=1 and x > 0
        plus all shades in between
    """

    def bootstrap_mean(df_i: pandas.DataFrame) -> pandas.DataFrame:
        boot = df_i[field_name].sample(frac=10_000, replace=True).to_frame()
        boot["i"] = pandas.RangeIndex(boot.shape[0]) // df_i.shape[0]
        out = boot.groupby("i").mean().reset_index()[[field_name]]
        assert out.shape == (10_000, 1)
        out.index.name = "bootstrap_run"
        return out

    # DataFrame with 20,000 rows per test exactly, with columns
    # [fullname, fullname_no_category, runtime, bootstrap_run, {field_name}]
    bootstrapped = (
        df.groupby(["fullname", "fullname_no_category", "runtime"])
        .apply(bootstrap_mean)
        .reset_index()
    )

    # DataFrame with 10,000 rows per test exactly, with columns
    # [fullname, fullname_no_category, bootstrap_run, {A}, {B}, diff]
    pivot = bootstrapped.pivot(
        index=["fullname", "fullname_no_category", "bootstrap_run"],
        columns="runtime",
        values=field_name,
    ).reset_index()
    pivot["diff"] = pivot[B] / pivot[A] - 1

    def confidence(
        df_i: pandas.DataFrame,
        x: numpy.ndarray,
        op: Literal["<", ">"],
        cmp: Callable[[Any, Any], bool],
        color_factor: float,
    ) -> pandas.DataFrame:
        xlabel = [f"{op}{xi * 100:.0f}%" for xi in x]
        p = (cmp(df_i["diff"].values.reshape([-1, 1]), x)).sum(axis=0) / df_i.shape[0]
        color = color_factor * p / 2 + 0.5
        return pandas.DataFrame({"x": x, "xlabel": xlabel, "p": p, "color": color})

    pivot_groups = pivot.groupby(["fullname", "fullname_no_category"])[["diff"]]
    x_neg = numpy.linspace(-0.8, 0, 17)
    x_pos = numpy.linspace(0, 0.8, 17)
    conf_neg, conf_pos = [
        # DataFrame with 1 row per element of x_neg/x_pos and columns
        # [fullname, fullname_no_category, x, xlabel, p, color]
        (
            pivot_groups.apply(confidence, p, op, cmp, color_factor)
            .reset_index()
            .drop("level_2", axis=1)
        )
        for (p, op, cmp, color_factor) in (
            (x_neg, "<", operator.lt, -1),
            (x_pos, ">", operator.gt, 1),
        )
    ]
    return pandas.concat([conf_neg, conf_pos], axis=0)


def make_barchart(
    df: pandas.DataFrame,
    spec: ChartSpec,
    title: str,
) -> tuple[altair.Chart | None, int]:
    """Make a single Altair barchart for a given test or runtime"""
    df = df.dropna(subset=[spec.field_name, "start"])
    if not len(df):
        # Some tests do not have average_memory or peak_memory measures, only runtime
        return None, 0

    df = df[
        [
            spec.field_name,
            "fullname",
            "fullname_no_category",
            "dask_version",
            "distributed_version",
            "runtime",
        ]
    ]

    tooltip = [
        altair.Tooltip("fullname:N", title="Test"),
        altair.Tooltip("runtime:N", title="Runtime"),
        altair.Tooltip("min(dask_version):N", title="Dask (min)"),
        altair.Tooltip("max(dask_version):N", title="Dask (max)"),
        altair.Tooltip("min(distributed_version):N", title="Distributed (min)"),
        altair.Tooltip("max(distributed_version):N", title="Distributed (max)"),
        altair.Tooltip(f"count({spec.field_name}):N", title="Number of runs"),
        altair.Tooltip(f"stdev({spec.field_name}):Q", title=f"std dev {spec.unit}"),
        altair.Tooltip(f"min({spec.field_name}):Q", title=f"min {spec.unit}"),
        altair.Tooltip(f"median({spec.field_name}):Q", title=f"median {spec.unit}"),
        altair.Tooltip(f"mean({spec.field_name}):Q", title=f"mean {spec.unit}"),
        altair.Tooltip(f"max({spec.field_name}):Q", title=f"max {spec.unit}"),
    ]

    by_test = len(df["fullname"].unique()) == 1
    if by_test:
        df = df.sort_values("runtime", key=runtime_sort_key_pd)
        y = altair.Y("runtime", title="Runtime", sort=None)
        n_bars = df["runtime"].unique().size
    else:
        y = altair.Y("fullname_no_category", title="Test name")
        n_bars = df["fullname_no_category"].unique().size

    height = max(n_bars * 20 + 50, 90)

    bars = (
        altair.Chart(width=800, height=height)
        .mark_bar()
        .encode(
            x=altair.X(
                f"median({spec.field_name}):Q", title=f"{spec.field_desc} {spec.unit}"
            ),
            y=y,
            tooltip=tooltip,
        )
    )
    ticks = (
        altair.Chart()
        .mark_tick(color="black")
        .encode(x=f"mean({spec.field_name})", y=y)
    )
    error_bars = (
        altair.Chart().mark_errorbar(extent="stdev").encode(x=spec.field_name, y=y)
    )
    chart = (
        altair.layer(bars, ticks, error_bars, data=df)
        .properties(title=title)
        .configure(autosize="fit")
    )

    return chart, height


def make_ab_confidence_map(
    df: pandas.DataFrame,
    spec: ChartSpec,
    title: str,
    baseline: str,
) -> tuple[altair.Chart | None, int]:
    """Make a single Altair heatmap of p(B/A - 1) confidence intervals, where B is the
    examined runtime and A is the baseline, for all tests for a given measure.
    """
    df = df.dropna(subset=[spec.field_name, "start"])
    if not len(df):
        # Some tests do not have average_memory or peak_memory measures, only runtime
        return None, 0

    df = df[
        [
            spec.field_name,
            "fullname",
            "fullname_no_category",
            "runtime",
        ]
    ]

    runtimes = df["runtime"].unique()
    if len(runtimes) < 2 or baseline not in runtimes:
        return None, 0
    A = baseline
    B = next(r for r in runtimes if r != baseline)

    conf = calc_ab_confidence_intervals(df, spec.field_name, A, B)

    n_bars = df["fullname_no_category"].unique().size
    height = max(n_bars * 20 + 50, 90)

    chart = (
        altair.Chart(conf, width=800, height=height)
        .mark_rect()
        .encode(
            x=altair.X("xlabel:O", title="confidence threshold (B/A - 1)", sort=None),
            y=altair.Y("fullname_no_category:O", title="Test"),
            color=altair.Color(
                "color:Q",
                scale=altair.Scale(scheme="redblue", domain=[0, 1], reverse=True),
                legend=None,
            ),
            tooltip=[
                altair.Tooltip("fullname:O", title="Test Name"),
                altair.Tooltip("xlabel:O", title="Confidence threshold"),
                altair.Tooltip("p:Q", format=".2p", title="p(B/A-1) exceeds threshold"),
            ],
        )
        .properties(title=title)
        .configure(autosize="fit")
    )

    return chart, height


def details_report_fname(runtime: str, fullname: str) -> str:
    fullname = fullname.replace("/", "-").replace(".py::", "-")
    return f"details/{runtime}-{fullname}.html"


def make_timeseries(
    df: pandas.DataFrame, spec: ChartSpec, title: str
) -> altair.Chart | None:
    """Make a single Altair timeseries chart for a given test"""
    df = df.dropna(subset=[spec.field_name, "start"]).reset_index().copy()
    if not len(df):
        # Some tests do not have average_memory or peak_memory measures, only runtime
        return None

    df["details_url"] = [
        details_report_fname(runtime, fullname)
        for runtime, fullname in zip(df.runtime, df.fullname)
    ]

    kwargs = {}
    # Reduce the size of the altair spec
    df = df[
        [
            "id",
            spec.field_name,
            "start",
            "details_url",
            "name",
            "call_outcome",
            "coiled_runtime_version",
            "dask_version",
            "distributed_version",
            "cluster_id",
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
            y=altair.Y(f"{spec.field_name}:Q", title=f"{spec.field_desc} {spec.unit}"),
            href=altair.Href("details_url:N"),
            tooltip=[
                altair.Tooltip("id:N", title="Test id"),
                altair.Tooltip("name:N", title="Test Name"),
                altair.Tooltip("start:T", title="Date"),
                altair.Tooltip("call_outcome:N", title="Test Outcome"),
                altair.Tooltip("coiled_runtime_version:N", title="Coiled Runtime"),
                altair.Tooltip("dask_version:N", title="Dask"),
                altair.Tooltip("distributed_version:N", title="Distributed"),
                altair.Tooltip(
                    f"{spec.field_name}:Q", title=f"{spec.field_desc} {spec.unit}"
                ),
                altair.Tooltip("cluster_id:Q", title="Cluster ID"),
            ],
            **kwargs,
        )
        .properties(title=title)
        .configure(autosize="fit")
        .interactive()
    )


def make_test_report(
    df: pandas.DataFrame,
    kind: Literal["barchart" | "timeseries" | "A/B"],
    title: str,
    sourcename: str | None = None,
    baseline: str | None = None,
) -> panel.Tabs:
    """Make a tab panel for a single test"""
    tabs = []
    for spec in SPECS:
        if kind == "timeseries":
            assert not baseline
            chart = make_timeseries(df, spec, title)
            height = 384
        elif kind == "barchart":
            assert not baseline
            chart, height = make_barchart(df, spec, title)
        elif kind == "A/B":
            assert baseline
            chart, height = make_ab_confidence_map(df, spec, title, baseline=baseline)
        else:
            raise ValueError(kind)  # pragma: nocover
        if not chart:
            continue
        tabs.append((spec.field_desc, chart))

    if sourcename in source:
        code = panel.pane.Markdown(
            f"```python\n{source[sourcename]}\n```",
            width=800,
            height=height,
            style={"overflow": "auto"},
        )
        tabs.append(("Source", code))
    elif sourcename is not None:
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
        df_by_test = df[(df.runtime == runtime) & (df.category == category)].groupby(
            "sourcename"
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

    doc.save(out_fname, title=runtime, resources=CDN)


def make_barchart_html_report(
    df: pandas.DataFrame,
    output_dir: pathlib.Path,
    by_test: bool,
) -> None:
    """Generate HTML report containing bar charts showing statistical information
    (mean, median, etc).

    Create one tab for each test category (e.g. benchmarks, runtime, stability),
    one graph for each runtime and one bar for each test
    OR one graph for each test and one bar for each runtime,
    and one graph tab for each measure (wall clock, average memory, peak memory).
    """
    out_fname = str(
        output_dir.joinpath(
            "barcharts_by_" + ("test" if by_test else "runtime") + ".html"
        )
    )
    print(f"Generating {out_fname}")

    categories = sorted(df.category.unique())
    tabs = []
    for category in categories:
        if by_test:
            df_by_test = df[df.category == category].groupby(["sourcename", "fullname"])
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
            df_by_runtime = df[df.category == category].groupby("runtime")
            panes = [
                make_test_report(
                    df_by_runtime.get_group(runtime),
                    kind="barchart",
                    title=runtime,
                )
                for runtime in sorted(df_by_runtime.groups, key=runtime_sort_key)
            ]
        flex = panel.FlexBox(*panes, align_items="start", justify_content="start")
        tabs.append((category.title(), flex))
    doc = panel.Tabs(*tabs, margin=12)

    doc.save(
        out_fname,
        title="Bar charts by " + ("test" if by_test else "runtime"),
        resources=CDN,
    )


def make_ab_html_report(
    df: pandas.DataFrame,
    output_dir: pathlib.Path,
    baseline: str,
) -> bool:
    """Generate HTML report containing heat maps for confidence intervals relative to
    a baseline runtime, e.g. p(B/A-1) > 10%

    Create one tab for each test category (e.g. benchmarks, runtime, stability), one
    graph for each runtime, and one graph tab for each measure (wall clock, average
    memory, peak memory).

    Returns
    -------
    True if the report was generated; False otherwise
    """
    out_fname = str(output_dir.joinpath(f"AB_vs_{baseline}.html"))
    print(f"Generating {out_fname}")

    categories = sorted(df.category.unique())
    tabs = []
    for category in categories:
        df_by_runtime = df[df.category == category].groupby("runtime")
        if baseline not in df_by_runtime.groups:
            # Typically a misspelling. However, this can legitimately happen in CI if
            # all three jobs of the baseline runtime failed early.
            print(
                f"Baseline runtime {baseline!r} not found; valid choices are:",
                ", ".join(df["runtime"].unique()),
            )
            return False

        panes = [
            make_test_report(
                pandas.concat(
                    [
                        df_by_runtime.get_group(runtime),
                        df_by_runtime.get_group(baseline),
                    ],
                    axis=0,
                ),
                kind="A/B",
                title=runtime,
                baseline=baseline,
            )
            for runtime in sorted(df_by_runtime.groups, key=runtime_sort_key)
            if runtime != baseline
        ]
        flex = panel.FlexBox(*panes, align_items="start", justify_content="start")
        tabs.append((category.title(), flex))
    doc = panel.Tabs(*tabs, margin=12)

    doc.save(
        out_fname,
        title="A/B confidence intervals vs. " + baseline,
        resources=CDN,
    )
    return True


def make_details_html_report(
    df: pandas.DataFrame,
    output_dir: pathlib.Path,
    runtime: str,
    fullname: str,
) -> None:
    """Generate raw tabular info dump for all runs of a single test"""
    df = df.reset_index()
    # Delete redundant columns
    for k in (
        "name",
        "originalname",
        "path",
        "coiled_runtime_version",
        "coiled_software_name",
        "python_version",
        "category",
        "sourcename",
        "runtime",
        "fullname",
        "fullname_no_category",
    ):
        del df[k]

    header = list(df.columns) + ["grafana_url"]
    txt = dedent(
        f"""
        <style>
            table, th, td {{border: 1px solid black; border-collapse: collapse;}}
            th {{position: sticky; top: 0; background-color: lightgrey;}}
        </style>
        ### {runtime}
        ### {fullname}
        | {' | '.join(header)} |
        | {' | '.join("---" for _ in header)} |
        """
    )

    for id_, ds_row in df.iterrows():
        row = ds_row.to_dict()

        row["grafana_url"] = make_grafana_url(
            cluster_name=row["cluster_name"], start=row["start"], end=row["end"]
        )

        for k, v in row.items():
            if v is None or (isinstance(v, float) and math.isnan(v)):
                txt += "| "
            elif k == "duration" or k.endswith("_time"):
                txt += f"| {v:.1f}s "
            elif k.endswith("_memory"):
                txt += f"| {v:.1f}GiB "
            elif isinstance(v, str) and "://" in v:
                txt += f"| [🔗]({v}) "
            else:
                txt += f"| {v} "
        txt += " |\n"

    md = panel.pane.Markdown(txt, width=800)
    out_fname = output_dir / details_report_fname(runtime, fullname)
    print(f"Generating {out_fname}")
    md.save(
        str(out_fname),
        title=f"{runtime} - {fullname}",
        resources=CDN,
    )


def make_grafana_url(cluster_name, start, end) -> Optional[str]:
    if cluster_name:
        # Add some padding to compensate for clock differences between
        # GitHub actions and Prometheus, as well for sample granularity
        # (at the moment of writing, Prometheus data is sampled every 5s)
        ts_padding = pandas.Timedelta("10s")
        start_ts = int((start - ts_padding).timestamp() * 1000)
        end_ts = int((end + ts_padding).timestamp() * 1000)

        # We switched to new datasource and new (now public) Grafana instance,
        # so use different URL depending on when this test ran
        if start_ts < 1679590932198:
            return (
                "https://grafana.dev-sandbox.coiledhq.com/d/eU1bT-nVz/cluster-metrics-prometheus"
                f"?var-datasource={quote(OLD_PROMETHEUS_DATASOURCE)}"
                f"&from={start_ts}&to={end_ts}&var-cluster={cluster_name}"
            )
        else:
            return (
                "https://benchmarks-grafana.oss.coiledhq.com/d/GvbFsqKVk/coiled-cluster-metrics-basic"
                "?var-datasource=Benchmarks&var-account=dask-benchmarks&"
                f"var-cluster={cluster_name}&from={start_ts}&to={end_ts}"
            )
    else:
        return None


def make_index_html_report(
    output_dir: pathlib.Path, runtimes: list[str], baselines: list[str]
) -> None:
    """Generate index.html"""
    txt = """# Coiled Runtime Benchmarks\n"""
    txt += "### Historical timeseries\n"
    for runtime in runtimes:
        txt += f"- [{runtime}](./{runtime}.html)\n"
    txt += "\n\n### Statistical analysis\n"
    txt += "- [Bar charts, by test](./barcharts_by_test.html)\n"
    txt += "- [Bar charts, by runtime](./barcharts_by_runtime.html)\n"
    for baseline in baselines:
        txt += f"- [A/B confidence intervals vs. {baseline}](./AB_vs_{baseline}.html)\n"

    md = panel.pane.Markdown(txt, width=800)
    out_fname = str(output_dir / "index.html")
    print(f"Generating {out_fname}")
    md.save(
        out_fname,
        title="Coiled Runtime Benchmarks",
        resources=CDN,
    )


def runtime_sort_key(runtime: str) -> tuple:
    """Runtimes are in the format coiled-<coiled-runtime version>-py<python version>
    e.g. coiled-latest-py3.8

    Sort them by coiled-runtime and python version, both descending.
    """
    t = runtime.split("-")
    assert len(t) == 3
    assert t[0] == "coiled"
    # AB_a > AB_b > upstream > latest > 0.1.0 > 0.0.4
    if t[1].startswith("AB_"):
        coiled_version = [-3, t[1]]
    elif t[1] == "upstream":
        coiled_version = [-2]
    elif t[1] == "latest":
        coiled_version = [-1]
    else:
        coiled_version = [0] + [-int(v) for v in t[1].split(".")]

    assert t[2][:2] == "py"
    py_version = [-int(v) for v in t[2][2:].split(".")]
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
    (output_dir / "details").mkdir(parents=True, exist_ok=True)

    load_test_source()

    # Load SQLite database into a pandas DataFrame
    engine = sqlalchemy.create_engine(f"sqlite:///{args.db_file}")
    df = pandas.read_sql(
        "select * from test_run where platform = 'linux' "
        "and call_outcome in ('passed', 'failed')"
        "and coiled_runtime_version >= '0.2.1' ",
        engine,
    )
    df = df.assign(
        start=pandas.to_datetime(df.start),
        end=pandas.to_datetime(df.end),
        runtime=(
            "coiled-"
            + df.coiled_runtime_version
            + "-py"
            + df.python_version.str.split(".", n=2).str[:2].str.join(".")
        ),
        category=df.path.str.split("/", n=1).str[0],
        sourcename=df.path.str.cat(df.originalname, "::"),
        fullname=df.path.str.cat(df.name, "::"),
        fullname_no_category=df.path.str.partition("/")[2].str.cat(df.name, "::"),
    )
    for spec in SPECS:
        df[spec.field_name] /= spec.scale
    df = df.set_index("id")

    if args.pickle:
        out_fname = str(output_dir.joinpath("records.pickle"))
        print(f"Generating {out_fname}")
        df.to_pickle(out_fname)

    # Generate HTML pages
    runtimes = sorted(df.runtime.unique(), key=runtime_sort_key)
    for runtime in runtimes:
        make_timeseries_html_report(df, output_dir, runtime)

    for (runtime, fullname), df2 in df.groupby(["runtime", "fullname"]):
        make_details_html_report(df2, output_dir, runtime, fullname)

    # Do not use data that is more than a week old in statistical analysis.
    # Also exclude failed tests.
    df_recent = df[
        (df["end"] > df["end"].max() - pandas.Timedelta("7d"))
        & (df["call_outcome"] == "passed")
    ]

    if args.pickle:
        out_fname = str(output_dir.joinpath("records_recent.pickle"))
        print(f"Generating {out_fname}")
        df_recent.to_pickle(out_fname)

    make_barchart_html_report(df_recent, output_dir, by_test=True)
    make_barchart_html_report(df_recent, output_dir, by_test=False)

    baselines = []
    for baseline in args.baseline:
        has_baseline = make_ab_html_report(df_recent, output_dir, baseline)
        if has_baseline:
            baselines.append(baseline)

    make_index_html_report(output_dir, runtimes, baselines)


if __name__ == "__main__":
    main()
