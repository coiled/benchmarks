import altair as alt
import click
import ibis


def generate(outfile="chart.json"):
    con = ibis.sqlite.connect("benchmark.db")
    t = con.table("test_run")

    tt = t[(t.call_outcome == "passed") & (t.path.startswith("tpch/"))]
    df = tt[["path", "name", "duration", "start"]].to_pandas()

    df["library"] = df.path.map(lambda path: path.split("_")[-1].split(".")[0])
    df["query"] = df.name.map(lambda name: int(name.split("_")[-1]))
    del df["path"]
    del df["name"]
    df = df.sort_values(["query", "library"])

    def recent(df):
        return df.sort_values("start").iloc[-1]

    df = df.groupby(["library", "query"]).apply(recent).reset_index(drop=True)
    del df["start"]

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="query:N",
            y="duration:Q",
            xOffset="library:N",
            color="library:N",
            tooltip=["library", "duration"],
        )
    )
    chart.save(outfile)
    print("Saving chart to", outfile)


@click.command()
@click.option(
    "--outfile",
    default="chart.json",
    help="Destination file written by Altair. Defaults to chart.json.",
)
def main(outfile):
    return generate(outfile)


if __name__ == "__main__":
    main()
