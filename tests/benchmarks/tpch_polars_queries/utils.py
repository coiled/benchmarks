import polars as pl
from pyarrow.dataset import dataset

from tests.benchmarks.conftest import DATASETS, ENABLED_DATASET


def read_data(name, source=None):
    pyarrow_dataset = dataset(
        source=source or f"{DATASETS[ENABLED_DATASET]}{name}/", format="parquet"
    )
    return pl.scan_pyarrow_dataset(pyarrow_dataset)
