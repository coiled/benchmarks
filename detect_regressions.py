import pathlib

import pandas as pd
import sqlalchemy


def detect_regressions(database_file):

    engine = sqlalchemy.create_engine(f"sqlite:///{database_file}")

    # regression analysis only on tests that passed
    df = pd.read_sql(
        "select * from test_run where platform = 'linux' and call_outcome = 'passed'",
        engine,
    )

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

    reg_df = pd.DataFrame(
        columns=[
            "category",
            "type",
            "mean",
            "last",
            "last-1",
            "last-2",
            "threshold",
            "str_report",
        ]
    )

    runtimes = list(df.runtime.unique())
    for runtime in runtimes:
        by_test = df[(df.runtime == runtime)].groupby("name")

        test_names = list(by_test.groups.keys())
        for name in test_names:
            df_test = by_test.get_group(name)

            # check the test is not obsolete.
            if pd.Timestamp(df_test.start.iloc[-1]) < (
                pd.Timestamp.now() - pd.Timedelta(days=7)
            ):
                # the latest run was 7+ days ago, test is obsolete
                pass
            else:
                for metric in ["duration", "average_memory", "peak_memory"]:
                    # check that we have enough data to do some stats
                    # currently we don't have a lot of data but eventually use 10 instead of 6.
                    if len(df_test[metric]):
                        category = df_test.category.unique()[0]
                        metric_threshold = (
                            df_test[metric][-13:-3].mean()
                            + 2 * df_test[metric][-13:-3].std()
                        )

                        if (df_test[metric].iloc[-3:] >= metric_threshold).all():

                            if metric in ["average_memory", "peak_memory"]:
                                units_norm = 1 / (
                                    1024**3
                                )  # to GiB to match dashboard
                                u = "[GiB]"
                            else:
                                units_norm = 1
                                u = "[s]"

                            last_three = (
                                df_test[metric].iloc[-1] * units_norm,
                                df_test[metric].iloc[-2] * units_norm,
                                df_test[metric].iloc[-3] * units_norm,
                            )
                            reg = (
                                f"{runtime = }, {name = }, {category = }, "
                                f"last_three_{metric} {u} = "
                                f"{last_three}, "
                                f"{metric}_threshold {u} = {metric_threshold * units_norm} \n"
                            )

                            # ["category", "type", "mean", "last", "last-1", "last-2", "threshold"])
                            reg_df.loc[f"{(runtime, name, metric)} {u}"] = [
                                category,
                                metric,
                                df_test[metric][-13:-3].mean() * units_norm,
                                df_test[metric].iloc[-1] * units_norm,
                                df_test[metric].iloc[-2] * units_norm,
                                df_test[metric].iloc[-3] * units_norm,
                                metric_threshold * units_norm,
                                reg,
                            ]

    return reg_df


def regressions_report(reg_df):

    # write reg_df to markdown for GHA summary
    cols_for_report = [
        "category",
        "type",
        "mean",
        "last",
        "last-1",
        "last-2",
        "threshold",
    ]
    reg_df[cols_for_report].to_markdown("regressions_summary.md")

    if not reg_df.empty:
        raise Exception(
            f"\x1b[31m Regressions detected {len(reg_df)}: \n{''.join(reg_df.str_report.values)} \x1b[0m"
        )
    else:
        return


if __name__ == "__main__":

    DB_FILE = pathlib.Path("./benchmark.db")
    regressions_df = detect_regressions(DB_FILE)

    regressions_report(regressions_df)
