import functools
import os

import coiled
import dask_expr as dx
import pytest

DATASETS = {
    "local": "./tpch-data/scale10/",
    "scale 100": "s3://coiled-runtime-ci/tpch_scale_100/",
    "scale 1000": "s3://coiled-runtime-ci/tpch-scale-1000/",
    "scale 1000-date": "s3://coiled-runtime-ci/tpch-scale-1000-date/",
}


if dx.__version__ == "0.1.10+1.ga61b4de":
    ENABLED_DATASET = os.getenv("TPCH_SCALE") or "scale 1000"
else:
    ENABLED_DATASET = "scale 1000-date"


if ENABLED_DATASET not in DATASETS:
    raise ValueError("Unknown tpch dataset: ", ENABLED_DATASET)

machine = {
    "vm_type": "m6i.8xlarge",
}


@pytest.fixture(scope="module")
def warm_start():
    @coiled.function(**machine)
    def _():
        pass

    _()  # run once to give us a warm start


@pytest.fixture(scope="function")
def restart(warm_start, benchmark_all):
    @coiled.function(**machine)
    def _():
        pass

    _.client.restart()
    with benchmark_all(_.client):
        yield


def coiled_function(**kwargs):
    # Shouldn't be necessary
    # See https://github.com/coiled/platform/issues/3519
    def _(function):
        return functools.wraps(function)(coiled.function(**kwargs, **machine)(function))

    return _


@pytest.fixture
def tpch_dataset_name():
    return ENABLED_DATASET


@pytest.fixture
def tpch_dataset_path(tpch_dataset_name):
    return DATASETS[tpch_dataset_name]


@pytest.fixture
def vm_type():
    return "m6i.16xlarge"


@pytest.fixture
def region():
    # Region of the TPCH data
    return "us-east-2"
