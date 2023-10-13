import functools
import os

import coiled
import pytest

DATASETS = {
    "local": "./tpch-data/scale10/",
    "scale 100": "s3://coiled-runtime-ci/tpch_scale_100/",
    "scale 1000": "s3://coiled-runtime-ci/tpch-scale-1000/",
}

ENABLED_DATASET = os.getenv("TPCH_SCALE")
if ENABLED_DATASET is not None:
    if ENABLED_DATASET not in DATASETS:
        raise ValueError("Unknown tpch dataset: ", ENABLED_DATASET)
else:
    ENABLED_DATASET = "scale 100"

machine = {
    "memory": "256 GiB",
}


@pytest.fixture(scope="module")
def warm_start():
    @coiled.function(**machine)
    def _():
        pass

    _()  # run once to give us a warm start


@pytest.fixture(scope="function")
def restart(warm_start):
    @coiled.function(**machine)
    def _():
        pass

    _.client.restart()
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
