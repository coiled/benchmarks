import io
import tarfile

import pandas as pd
import pytest
from dask.distributed import wait

pytestmark = pytest.mark.workflows


@pytest.mark.client("embarrassingly_parallel")
def test_embarassingly_parallel(client, s3_factory):
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

    futures = client.map(extract, directories, fs=s3)
    wait(futures)
    # We had one error in one file.  Let's just ignore and move on.
    good = [future for future in futures if future.status == "finished"]
    data = client.gather(good)

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
