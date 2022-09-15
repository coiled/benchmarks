import contextlib
import datetime
import json
import logging
import os
import pathlib
import shlex
import subprocess
import sys
import threading
import time
import uuid
from distutils.util import strtobool

import dask
import distributed
import filelock
import pytest
import s3fs
from distributed import Client
from distributed.diagnostics.memory_sampler import MemorySampler
from toolz import merge

try:
    from coiled.v2 import Cluster
except ImportError:
    from coiled._beta import ClusterBeta as Cluster

import sqlalchemy
from sqlalchemy.orm import Session

from benchmark_schema import TestRun
from plugins import Durations

logger = logging.getLogger("coiled-runtime")
logger.setLevel(logging.INFO)

# So coiled logs can be displayed on test failure
logging.getLogger("coiled").setLevel(logging.INFO)

TEST_DIR = pathlib.Path("./tests").absolute()


def pytest_addoption(parser):
    # Workaround for https://github.com/pytest-dev/pytest-xdist/issues/620
    if threading.current_thread() is not threading.main_thread():
        os._exit(1)

    parser.addoption(
        "--run-latest", action="store_true", help="Run latest coiled-runtime tests"
    )
    parser.addoption(
        "--benchmark", action="store_true", help="Collect benchmarking data for tests"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-latest"):
        # --run-latest given in cli: do not skip latest coiled-runtime tests
        return
    skip_latest = pytest.mark.skip(reason="need --run-latest option to run")
    for item in items:
        if "latest_runtime" in item.keywords:
            item.add_marker(skip_latest)


def get_coiled_runtime_version():
    if strtobool(os.environ.get("TEST_UPSTREAM", "false")):
        return "upstream"
    try:
        return os.environ["COILED_RUNTIME_VERSION"]
    except KeyError:
        # Determine software environment from local `coiled-runtime` version (if installed)
        out = subprocess.check_output(
            shlex.split("conda list --json coiled-runtime"), text=True
        ).rstrip()
        runtime_info = json.loads(out)
        if runtime_info:
            return runtime_info[0]["version"]
        else:
            return "latest"


def get_coiled_software_name():
    try:
        return os.environ["COILED_SOFTWARE_NAME"]
    except KeyError:
        # Determine software environment from local `coiled-runtime` version (in installed)
        out = subprocess.check_output(
            shlex.split("conda list --json coiled-runtime"), text=True
        ).rstrip()
        runtime_info = json.loads(out)
        if runtime_info:
            version = runtime_info[0]["version"].replace(".", "-")
            py_version = f"{sys.version_info[0]}{sys.version_info[1]}"
            return f"coiled/coiled-runtime-{version}-py{py_version}"
        else:
            raise RuntimeError(
                "Must either specific `COILED_SOFTWARE_NAME` environment variable "
                "or have `coiled-runtime` installed"
            )


dask.config.set(
    {
        "coiled.account": "dask-engineering",
        "coiled.software": get_coiled_software_name(),
    }
)

COILED_RUNTIME_VERSION = get_coiled_runtime_version()


# ############################################### #
#            BENCHMARKING RELATED                 #
# ############################################### #

DB_NAME = os.environ.get("DB_NAME", "benchmark.db")

if os.environ.get("GITHUB_SERVER_URL"):
    WORKFLOW_URL = "/".join(
        [
            os.environ.get("GITHUB_SERVER_URL", ""),
            os.environ.get("GITHUB_REPOSITORY", ""),
            "actions",
            "runs",
            os.environ.get("GITHUB_RUN_ID", ""),
        ]
    )
else:
    WORKFLOW_URL = None


@pytest.fixture(scope="session")
def benchmark_db_engine(pytestconfig, tmp_path_factory):
    """Session-scoped fixture for the SQLAlchemy engine for the benchmark sqlite database.

    You can control the database name with the environment variable ``DB_NAME``,
    which defaults to ``benchmark.db``. Most tests shouldn't need to include this
    fixture directly.

    Yields
    ------
    The SQLAlchemy engine if the ``--benchmark`` option is set, None otherwise.
    """

    if not pytestconfig.getoption("--benchmark"):
        yield
    else:
        engine = sqlalchemy.create_engine(f"sqlite:///{DB_NAME}", future=True)

        # get the temp directory shared by all workers
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock = root_tmp_dir / (DB_NAME + ".lock")

        # Create the db if it does not exist already.
        with filelock.FileLock(lock):
            if not os.path.exists(DB_NAME):
                with engine.connect() as conn:
                    conn.execute(sqlalchemy.text("VACUUM"))

        # Run migrations if we are not up-to-date.
        with filelock.FileLock(lock):
            current = subprocess.check_output(["alembic", "current"], text=True)
            if "(head)" not in current:
                p = subprocess.run(["alembic", "upgrade", "head"])
                p.check_returncode()

        yield engine


@pytest.fixture(scope="function")
def benchmark_db_session(benchmark_db_engine):
    """SQLAlchemy session for a given test.

    Most tests shouldn't need to include this fixture directly.

    Yields
    ------
    SQLAlchemy session for the benchmark database engine if run as part of a benchmark,
    None otherwise.
    """
    if not benchmark_db_engine:
        yield
    else:
        with Session(benchmark_db_engine, future=True) as session, session.begin():
            yield session


@pytest.fixture(scope="function")
def test_run_benchmark(benchmark_db_session, request, testrun_uid):
    """SQLAlchemy ORM object representing a given test run.

    By including this fixture in a test (or another fixture that includes it)
    you trigger the test being written to the benchmark database. This fixture
    includes data common to all tests, including python version, test name,
    and the test outcome.

    Yields
    ------
    SQLAlchemy ORM object representing a given test run if run as part of a benchmark,
    None otherwise.
    """
    if not benchmark_db_session:
        yield
    else:
        node = request.node
        run = TestRun(
            session_id=testrun_uid,
            name=node.name,
            originalname=node.originalname,
            path=str(node.path.relative_to(TEST_DIR)),
            dask_version=dask.__version__,
            distributed_version=distributed.__version__,
            coiled_runtime_version=COILED_RUNTIME_VERSION,
            coiled_software_name=dask.config.get("coiled.software"),
            python_version=".".join(map(str, sys.version_info)),
            platform=sys.platform,
            ci_run_url=WORKFLOW_URL,
        )
        yield run

        rep = getattr(request.node, "rep_setup", None)
        if rep:
            run.setup_outcome = rep.outcome
        rep = getattr(request.node, "rep_call", None)
        if rep:
            run.call_outcome = rep.outcome
        rep = getattr(request.node, "rep_teardown", None)
        if rep:
            run.teardown_outcome = rep.outcome

        benchmark_db_session.add(run)
        benchmark_db_session.commit()


@pytest.fixture(scope="function")
def benchmark_time(test_run_benchmark):
    """Benchmark the wall clock time of executing some code.

    Yields
    ------
    Context manager that records the wall clock time duration of executing
    the ``with`` statement if run as part of a benchmark, or does nothing otherwise.

    Example
    -------
    .. code-block:: python

        def test_something(benchmark_time):
            with benchmark_time:
                do_something()

    See Also
    --------
    auto_benchmark_time
    """

    @contextlib.contextmanager
    def _benchmark_time():
        if not test_run_benchmark:
            yield
        else:
            start = time.time()
            yield
            end = time.time()
            test_run_benchmark.duration = end - start
            test_run_benchmark.start = datetime.datetime.utcfromtimestamp(start)
            test_run_benchmark.end = datetime.datetime.utcfromtimestamp(end)

    return _benchmark_time()


@pytest.fixture(scope="function")
def auto_benchmark_time(benchmark_time):
    """Benchmark the wall clock time of an individual test.

    This fixture automatically records the wall clock time it takes to
    execute an individual test if run as part of a benchmark. Depending on the test's structure, this may
    include setup or teardown code that should not be measured. For more
    control, use the ``benchmark_time`` context manager fixture.


    Example
    -------
    .. code-block:: python

        def test_something(auto_benchmark_time):
            do_something()

    See Also
    --------
    benchmark_time
    """
    with benchmark_time:
        yield


@pytest.fixture(scope="function")
def benchmark_memory(test_run_benchmark):
    """Benchmark the memory usage of executing some code.

    Yields
    ------
    Context manager factory function which takes a ``distributed.Client``
    as input. The context manager records peak and average memory usage of
    executing the ``with`` statement if run as part of a benchmark,
    or does nothing otherwise.

    Example
    -------
    .. code-block:: python

        def test_something(benchmark_memory):
            with Client() as client:
                with benchmark_memory(client):
                    do_something()
    """

    @contextlib.contextmanager
    def _benchmark_memory(client):
        if not test_run_benchmark:
            yield
        else:
            sampler = MemorySampler()
            label = uuid.uuid4().hex[:8]
            with sampler.sample(label, client=client, measure="process"):
                yield

            df = sampler.to_pandas()
            test_run_benchmark.average_memory = df[label].mean()
            test_run_benchmark.peak_memory = df[label].max()

    yield _benchmark_memory


@pytest.fixture(scope="function")
def benchmark_task_durations(test_run_benchmark):
    """Benchmark the time spent computing, transferring data, spilling to disk,
    and deserializing data.

    Yields
    ------
    Context manager factory function which takes a ``distributed.Client``
    as input. The context manager records records time spent computing,
    transferring data, spilling to disk, and deserializing data while
    executing the ``with`` statement if run as part of a benchmark,
    or does nothing otherwise.

    Example
    -------
    .. code-block:: python

        def test_something(benchmark_task_durations):
            with Client() as client:
                with benchmark_task_durations(client):
                    do_something()
    """

    @contextlib.contextmanager
    def _measure_durations(client):
        if not test_run_benchmark:
            yield
        else:
            # TODO: is there a nice way to only register this once? I don't think so,
            # other than idempotent=True
            client.register_scheduler_plugin(Durations(), idempotent=True)

            # Start tracking durations
            client.sync(client.scheduler.start_tracking_durations)
            yield
            client.sync(client.scheduler.stop_tracking_durations)

            # Add duration data to db entry
            durations = client.sync(client.scheduler.get_durations)
            test_run_benchmark.compute_time = durations.get("compute", 0.0)
            test_run_benchmark.disk_spill_time = durations.get(
                "disk-read", 0.0
            ) + durations.get("disk-write", 0.0)
            test_run_benchmark.serializing_time = durations.get("deserialize", 0.0)
            test_run_benchmark.transfer_time = durations.get("transfer", 0.0)

    yield _measure_durations


@pytest.fixture(scope="function")
def benchmark_all(benchmark_memory, benchmark_task_durations, benchmark_time):
    """Benchmark all available metrics.

    Yields
    ------
    Context manager factory function which takes a ``distributed.Client``
    as input. The context manager records records all available metrics while
    executing the ``with`` statement if run as part of a benchmark,
    or does nothing otherwise.

    Example
    -------
    .. code-block:: python

        def test_something(benchmark_all):
            with benchmark_all(client):
                do_something()

    See Also
    --------
    benchmark_memory
    benchmark_task_durations
    benchmark_time
    """

    @contextlib.contextmanager
    def _benchmark_all(client):
        with benchmark_memory(client), benchmark_task_durations(client), benchmark_time:
            yield

    yield _benchmark_all


# ############################################### #
#        END BENCHMARKING RELATED                 #
# ############################################### #


@pytest.fixture
def test_name_uuid(request):
    "Test name, suffixed with a UUID. Useful for resources like cluster names, S3 paths, etc."
    return f"{request.node.originalname}-{uuid.uuid4().hex}"


@pytest.fixture(scope="module")
def small_cluster(request):
    # Extract `backend_options` for cluster from `backend_options` markers
    backend_options = merge(
        m.kwargs for m in request.node.iter_markers(name="backend_options")
    )
    module = os.path.basename(request.fspath).split(".")[0]
    with Cluster(
        name=f"{module}-{uuid.uuid4().hex[:8]}",
        n_workers=10,
        worker_vm_types=["t3.large"],  # 2CPU, 8GiB
        scheduler_vm_types=["t3.large"],
        backend_options=backend_options,
    ) as cluster:
        yield cluster


@pytest.fixture
def small_client(
    small_cluster,
    upload_cluster_dump,
    benchmark_all,
):
    with Client(small_cluster) as client:
        small_cluster.scale(10)
        client.wait_for_workers(10)
        client.restart()

        with upload_cluster_dump(client, small_cluster), benchmark_all(client):
            yield client


S3_REGION = "us-east-2"
S3_BUCKET = "s3://coiled-runtime-ci"


@pytest.fixture(scope="session")
def s3_storage_options():
    return {
        "key": os.environ.get("AWS_ACCESS_KEY_ID"),
        "secret": os.environ.get("AWS_SECRET_ACCESS_KEY"),
    }


@pytest.fixture(scope="session")
def s3():
    return s3fs.S3FileSystem(
        key=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


@pytest.fixture(scope="session")
def s3_scratch(s3):
    # Ensure that the test-scratch directory exists,
    # but do NOT remove it as multiple test runs could be
    # accessing it at the same time
    scratch_url = f"{S3_BUCKET}/test-scratch"
    s3.mkdirs(scratch_url, exist_ok=True)
    return scratch_url


@pytest.fixture(scope="function")
def s3_url(s3, s3_scratch, test_name_uuid):
    url = f"{s3_scratch}/{test_name_uuid}"
    s3.mkdirs(url, exist_ok=False)
    yield url
    s3.rm(url, recursive=True)


@pytest.fixture(scope="session")
def s3_cluster_dump_url(s3, s3_scratch):
    dump_url = f"{s3_scratch}/cluster_dumps"
    s3.mkdirs(dump_url, exist_ok=True)
    return dump_url


# this code was taken from pytest docs
# https://docs.pytest.org/en/latest/example/simple.html#making-test-result-information-available-in-fixtures
@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"

    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture
def upload_cluster_dump(request, s3_cluster_dump_url, s3_storage_options):
    @contextlib.contextmanager
    def _upload_cluster_dump(client, cluster):
        failed = False
        # the code below is a workaround to make cluster dumps work with clients in fixtures
        # and outside fixtures.
        try:
            yield
        except Exception:
            failed = True
            raise
        else:
            # we need this for tests that are not using the client fixture
            # for those cases request.node.rep_call.failed can't be access.
            try:
                failed = request.node.rep_call.failed
            except AttributeError:
                failed = False
        finally:
            cluster_dump = strtobool(os.environ.get("CLUSTER_DUMP", "false"))
            if cluster_dump and failed:
                dump_path = f"{s3_cluster_dump_url}/{cluster.name}/{request.node.name}"
                logger.error(f"Cluster state dump can be found at: {dump_path}")
                client.dump_cluster_state(dump_path, **s3_storage_options)

    yield _upload_cluster_dump
