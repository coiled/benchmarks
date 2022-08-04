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

            # stats of latest 10 runs exclude last point

            stats_dict[f"({runtime, name})"] = {
                "duration_mean": df_passed.duration[-11:-1].mean(),
                "duration_std": df_passed.duration[-11:-1].std(),
                "duration_last": df_passed.duration.iloc[-1],
                "avg_memory_mean": df_passed.average_memory[-11:-1].mean(),
                "avg_memory_std": df_passed.average_memory[-11:-1].std(),
                "avg_memory_last": df_passed.average_memory.iloc[-1],
                "peak_memory_mean": df_passed.peak_memory[-11:-1].mean(),
                "peak_memory_std": df_passed.peak_memory[-11:-1].std(),
                "peak_memory_last": df_passed.peak_memory.iloc[-1],
            }

            stats = stats_dict[f"({runtime, name})"]
            dur_threshold = stats["duration_mean"] + stats["duration_std"]
            avg_mem_threshold = stats["avg_memory_mean"] + stats["avg_memory_mean"]
            peak_mem_threshold = stats["peak_memory_mean"] + stats["duration_std"]

            if stats["duration_last"] >= dur_threshold:
                reg = f"{runtime= }, {name= }, duration_last = {stats['duration_last']}, {dur_threshold= } \n"
                regressions.append(reg)

            if stats["avg_memory_last"] >= avg_mem_threshold:
                reg = f"{runtime= }, {name= }, avg_mem_last = {stats['avg_memory_last']}, {avg_mem_threshold= } \n"
                regressions.append(reg)

            if stats["peak_memory_last"] >= peak_mem_threshold:
                reg = f"{runtime= }, {name= }, peak_mem_last = {stats['peak_memory_last']}, {peak_mem_threshold= } \n"
                regressions.append(reg)

        if regressions:
            raise Exception(f"Regressions detected: \n{''.join(regressions)}")
        else:
            assert not regressions
