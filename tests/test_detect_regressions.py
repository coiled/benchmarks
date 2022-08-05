import pathlib

import pandas
import sqlalchemy


def test_detect_regressions():

    DB_FILE = pathlib.Path("./benchmark.db")
    engine = sqlalchemy.create_engine(f"sqlite:///{DB_FILE}")

    df = pandas.read_sql("select * from test_run where platform = 'linux'", engine)

    # join runtime + py version
    df = df.assign(
        runtime=(
            "coiled-"
            + df.coiled_runtime_version
            + "-py"
            + df.python_version.str.split(".", n=2).str[:2].str.join(".")
        ),
        category=df.path.str.split("/", n=1).str[0],
    )

    # by_name_runtime = df.groupby(["name", "runtime"])
    stats_dict = {}
    regressions = []
    runtimes = list(df.runtime.unique())
    for runtime in runtimes:
        by_test = df[(df.runtime == runtime)].groupby("name")

        test_names = list(by_test.groups.keys())
        for name in test_names:
            df_test = by_test.get_group(name)

            df_passed = df_test[df_test.call_outcome == "passed"]
            # check for empty dataframe
            if not df_passed.empty:
                # stats of latest 10 runs exclude last point
                stats_dict[f"({runtime, name})"] = {
                    "duration_mean": df_passed.duration[-13:-3].mean(),
                    "duration_std": df_passed.duration[-13:-3].std(),
                    "duration_last": df_passed.duration.iloc[-1],
                    "duration_last-1": df_passed.duration.iloc[-2],
                    "duration_last-2": df_passed.duration.iloc[-3],
                    "avg_memory_mean": df_passed.average_memory[-13:-3].mean(),
                    "avg_memory_std": df_passed.average_memory[-13:-3].std(),
                    "avg_memory_last": df_passed.average_memory.iloc[-1],
                    "avg_memory_last-1": df_passed.average_memory.iloc[-2],
                    "avg_memory_last-2": df_passed.average_memory.iloc[-3],
                    "peak_memory_mean": df_passed.peak_memory[-13:-3].mean(),
                    "peak_memory_std": df_passed.peak_memory[-13:-3].std(),
                    "peak_memory_last": df_passed.peak_memory.iloc[-1],
                    "peak_memory_last-1": df_passed.peak_memory.iloc[-2],
                    "peak_memory_last-2": df_passed.peak_memory.iloc[-3],
                }

                stats = stats_dict[f"({runtime, name})"]
                dur_threshold = stats["duration_mean"] + 1 * stats["duration_std"]
                avg_mem_threshold = (
                    stats["avg_memory_mean"] + 1 * stats["avg_memory_mean"]
                )
                peak_mem_threshold = (
                    stats["peak_memory_mean"] + 1 * stats["peak_memory_mean"]
                )

                if (
                    stats["duration_last"] >= dur_threshold
                    and stats["duration_last-1"] >= dur_threshold
                    and stats["duration_last-2"] >= dur_threshold
                ):
                    reg = (
                        f"{runtime= }, {name= }, "
                        f"last_three_durations = "
                        f"{(stats['duration_last'], stats['duration_last-1'],stats['duration_last-2'])}, "
                        f"{dur_threshold= } \n"
                    )

                    regressions.append(reg)

                if (
                    stats["avg_memory_last"] >= avg_mem_threshold
                    and stats["avg_memory_last-1"] >= avg_mem_threshold
                    and stats["avg_memory_last-2"] >= avg_mem_threshold
                ):
                    reg = (
                        f"{runtime= }, {name= }, "
                        f"avg_mem_last = "
                        f"{(stats['avg_memory_last'], stats['avg_memory_last-1'], stats['avg_memory_last-2'])}, "
                        f"{avg_mem_threshold= } \n"
                    )

                    regressions.append(reg)

                if (
                    stats["peak_memory_last"] >= peak_mem_threshold
                    and stats["peak_memory_last-1"] >= peak_mem_threshold
                    and stats["peak_memory_last-2"] >= peak_mem_threshold
                ):
                    reg = (
                        f"{runtime= }, {name= }, "
                        f"peak_mem_last = "
                        f"{(stats['peak_memory_last'], stats['peak_memory_last-1'], stats['peak_memory_last-2'])}, "
                        f"{peak_mem_threshold= } \n"
                    )

                    regressions.append(reg)

    # convert dict to dataframe to write to xml to use later ask ian if this is what he had in mind.
    # df_stats = pandas.DataFrame.from_dict(stats_dict, orient="index")
    # df_stats.to_xml("stats.xml")

    if regressions:
        raise Exception(
            f"Regressions detected {len(regressions)}: \n{''.join(regressions)}"
        )
    else:
        assert not regressions
