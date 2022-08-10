import datetime
import pathlib

import pandas
import sqlalchemy


def detect_regressions(database_file):

    engine = sqlalchemy.create_engine(f"sqlite:///{database_file}")

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
    reg_df = pandas.DataFrame(
        columns=["category", "type", "mean", "last", "last-1", "last-2", "threshold"]
    )

    runtimes = list(df.runtime.unique())
    for runtime in runtimes:
        by_test = df[(df.runtime == runtime)].groupby("name")

        test_names = list(by_test.groups.keys())
        for name in test_names:
            df_test = by_test.get_group(name)

            df_passed = df_test[df_test.call_outcome == "passed"]
            # check for empty dataframe and having enough data to do some stats
            # currently we don't have a lot of data but eventually use 10 instead of 6.

            if not df_passed.empty and (len(df_passed) >= 6):

                # check the test is not obsolete.
                date_last_run = datetime.datetime.strptime(
                    df_passed.start.values[-1].split()[0], "%Y-%m-%d"
                )
                date_threshold = datetime.datetime.today() - datetime.timedelta(7)

                if date_last_run < date_threshold:
                    # the latest run was 7+ days ago, test is obsolete
                    pass
                else:
                    # get category for report
                    category = df_passed.category.unique()[0]
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
                    dur_threshold = stats["duration_mean"] + stats["duration_std"]
                    avg_mem_threshold = (
                        stats["avg_memory_mean"] + stats["avg_memory_mean"]
                    )
                    peak_mem_threshold = (
                        stats["peak_memory_mean"] + stats["peak_memory_mean"]
                    )

                    # Only raise if the last three show regression
                    if (
                        stats["duration_last"] >= dur_threshold
                        and stats["duration_last-1"] >= dur_threshold
                        and stats["duration_last-2"] >= dur_threshold
                    ):
                        reg = (
                            f"{runtime= }, {name= }, {category= }, "
                            f"last_three_durations = "
                            f"{(stats['duration_last'], stats['duration_last-1'],stats['duration_last-2'])}, "
                            f"{dur_threshold= } \n"
                        )

                        regressions.append(reg)
                        # ["regression_type", "mean", "last", "last-1", "last-2", "thershold"])
                        reg_df.loc[f"{(runtime, name)}"] = [
                            category,
                            "duration",
                            stats["duration_mean"],
                            stats["duration_last"],
                            stats["duration_last-1"],
                            stats["duration_last-2"],
                            dur_threshold,
                        ]

                    if (
                        stats["avg_memory_last"] >= avg_mem_threshold
                        and stats["avg_memory_last-1"] >= avg_mem_threshold
                        and stats["avg_memory_last-2"] >= avg_mem_threshold
                    ):
                        reg = (
                            f"{runtime= }, {name= }, {category= }, "
                            f"avg_mem_last = "
                            f"{(stats['avg_memory_last'], stats['avg_memory_last-1'], stats['avg_memory_last-2'])}, "
                            f"{avg_mem_threshold= } \n"
                        )

                        regressions.append(reg)

                        reg_df.loc[f"{(runtime, name)}"] = [
                            category,
                            "avg_memory",
                            stats["avg_memory_mean"],
                            stats["avg_memory_last"],
                            stats["avg_memory_last-1"],
                            stats["avg_memory_last-2"],
                            avg_mem_threshold,
                        ]

                    if (
                        stats["peak_memory_last"] >= peak_mem_threshold
                        and stats["peak_memory_last-1"] >= peak_mem_threshold
                        and stats["peak_memory_last-2"] >= peak_mem_threshold
                    ):
                        reg = (
                            f"{runtime= }, {name= }, {category= }, "
                            f"peak_mem_last = "
                            f"{(stats['peak_memory_last'], stats['peak_memory_last-1'], stats['peak_memory_last-2'])}, "
                            f"{peak_mem_threshold= } \n"
                        )

                        regressions.append(reg)

                        reg_df.loc[f"{(runtime, name)}"] = [
                            category,
                            "peak_memory",
                            stats["peak_memory_mean"],
                            stats["peak_memory_last"],
                            stats["peak_memory_last-1"],
                            stats["peak_memory_last-2"],
                            peak_mem_threshold,
                        ]
            else:
                # df_pass is empty or we don't have enough stats
                pass

    return reg_df, regressions


def regressions_report(reg_df, regressions):

    # write reg_df to markdown for GHA summary
    reg_df.to_markdown("regressions_summary.md")

    if regressions:
        raise Exception(
            f"\x1b[31m Regressions detected {len(regressions)}: \n{''.join(regressions)} \x1b[0m"
        )
    else:
        assert not regressions
        return


if __name__ == "__main__":

    DB_FILE = pathlib.Path("./benchmark.db")
    regressions_df, regressions_list = detect_regressions(DB_FILE)

    regressions_report(regressions_df, regressions_list)
