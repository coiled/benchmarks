import io
import tarfile
import uuid

import coiled
import pandas as pd
import pytest
from dask.distributed import Client, wait

from ..conftest import dump_cluster_kwargs


@pytest.fixture(scope="module")
def embarrassingly_parallel_cluster(
    dask_env_variables,
    cluster_kwargs,
    github_cluster_tags,
):
    kwargs = dict(
        name=f"embarrassingly_parallel-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **cluster_kwargs["embarrassingly_parallel_cluster"],
    )
    dump_cluster_kwargs(kwargs, "embarassingly_parallel")
    with coiled.Cluster(**kwargs) as cluster:
        yield cluster


@pytest.fixture
def embarrassingly_parallel_client(
    embarrassingly_parallel_cluster,
    cluster_kwargs,
    upload_cluster_dump,
    benchmark_all,
):
    n_workers = cluster_kwargs["embarrassingly_parallel_cluster"]["n_workers"]
    with Client(embarrassingly_parallel_cluster) as client:
        embarrassingly_parallel_cluster.scale(n_workers)
        client.wait_for_workers(n_workers)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


def test_embarassingly_parallel(embarrassingly_parallel_client, s3_factory):
    # How popular is matplotlib?
    s3 = s3_factory(requester_pays=True)
    directories = s3.ls("s3://arxiv/pdf")

    # We only analyze files from 1991-2022 here in order to have a consistent data volume.
    # This is benchmarking purposes only, as this dataset is updated monthly.
    years = list(range(91, 100)) + list(range(23))
    directories = [
        d
        for d in directories
        if d.endswith(".tar") and int(d.split("_")[2][:2]) in years
    ]

    def extract(filename: str, fs):
        """Extract and process one directory of arXiv data

        Returns
        -------
        filename: str
        contains_matplotlib: boolean
        """
        out = []
        with fs.open(filename) as f:
            bytes_ = f.read()
            with io.BytesIO() as bio:
                bio.write(bytes_)
                bio.seek(0)
                with tarfile.TarFile(fileobj=bio) as tf:
                    for member in tf.getmembers():
                        if member.isfile() and member.name.endswith(".pdf"):
                            data = tf.extractfile(member).read()
                            out.append((member.name, b"matplotlib" in data.lower()))
                return out

    futures = embarrassingly_parallel_client.map(extract, directories, fs=s3)
    wait(futures)
    # We had one error in one file.  Let's just ignore and move on.
    good = [future for future in futures if future.status == "finished"]
    data = embarrassingly_parallel_client.gather(good)

    # Convert to Pandas
    dfs = [pd.DataFrame(d, columns=["filename", "has_matplotlib"]) for d in data]
    df = pd.concat(dfs)

    def filename_to_date(filename):
        year = int(filename.split("/")[0][:2])
        month = int(filename.split("/")[0][2:4])
        if year > 80:
            year = 1900 + year
        else:
            year = 2000 + year

        return pd.Timestamp(year=year, month=month, day=1)

    df["date"] = df.filename.map(filename_to_date)
    result = df.groupby("date").has_matplotlib.mean()
    # Some light validation to ensure results are consistent.
    # This is only for benchmarking.
    assert result.idxmin() == pd.Timestamp("1991-07-01")  # Earliest timestamp
    assert result.idxmax() == pd.Timestamp("2022-10-01")  # Row with maximum value
    assert result.ne(0).idxmax() == pd.Timestamp("2005-06-01")  # First non-zero row
