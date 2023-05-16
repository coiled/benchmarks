import contextlib
import datetime
import json
import logging
import os
import pathlib
import pickle
import shlex
import subprocess
import sys
import threading
import time
import uuid
from functools import lru_cache

import dask
import dask.array as da
import distributed
import filelock
import pandas
import pytest
import s3fs
import sqlalchemy
import yaml
from coiled import Cluster
from distributed import Client, WorkerPlugin
from distributed.diagnostics.memory_sampler import MemorySampler
from distributed.scheduler import logger as scheduler_logger
from packaging.version import Version
from sqlalchemy.orm import Session

from benchmark_schema import TestRun
from plugins import Durations

logger = logging.getLogger("coiled-runtime")
logger.setLevel(logging.INFO)

coiled_logger = logging.getLogger("coiled")
# So coiled logs can be displayed on test failure
coiled_logger.setLevel(logging.INFO)
# Timestamps are useful for debugging
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
coiled_logger.addHandler(handler)

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
    parser.addoption("--run-workflows", action="store_true", help="Run workflow tests")


def pytest_collection_modifyitems(config, items):
    skip_latest = pytest.mark.skip(reason="need --run-latest option to run")
    skip_workflows = pytest.mark.skip(reason="need --run-workflows option to run")
    for item in items:
        if not config.getoption("--run-latest") and "latest_runtime" in item.keywords:
            item.add_marker(skip_latest)
        if not config.getoption("--run-workflows") and (
            (TEST_DIR / "workflows") in item.path.parents
        ):
            item.add_marker(skip_workflows)


def get_coiled_runtime_version():
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
            return "unknown"


dask.config.set(
    {
        "coiled.account": "dask-benchmarks",
        "distributed.admin.system-monitor.gil.enabled": True,
    }
)

COILED_RUNTIME_VERSION = get_coiled_runtime_version()
COILED_SOFTWARE_NAME = "package_sync"


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
        run = TestRun(
            session_id=testrun_uid,
            name=request.node.name,
            originalname=request.node.originalname,
            path=str(request.node.path.relative_to(TEST_DIR)),
            dask_version=dask.__version__,
            distributed_version=distributed.__version__,
            coiled_runtime_version=COILED_RUNTIME_VERSION,
            coiled_software_name=COILED_SOFTWARE_NAME,
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
def get_cluster_info(test_run_benchmark):
    """
    Gets cluster.name , cluster.cluster_id and cluster.cluster.details_url
    """

    @contextlib.contextmanager
    def _get_cluster_info(cluster):
        if not test_run_benchmark:
            yield
        else:
            yield
            test_run_benchmark.cluster_name = cluster.name
            test_run_benchmark.cluster_id = cluster.cluster_id
            test_run_benchmark.cluster_details_url = cluster.details_url

    yield _get_cluster_info


@pytest.fixture(scope="function")
def benchmark_all(
    benchmark_memory,
    benchmark_task_durations,
    get_cluster_info,
    benchmark_time,
):
    """Benchmark all available metrics and extracts cluster information

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
    get_cluster_info
    """

    @contextlib.contextmanager
    def _benchmark_all(client):
        with benchmark_memory(client), benchmark_task_durations(
            client
        ), get_cluster_info(client.cluster), benchmark_time:
            yield

    yield _benchmark_all


# ############################################### #
#        END BENCHMARKING RELATED                 #
# ############################################### #


@pytest.fixture(scope="session")
def github_cluster_tags():
    tag_names = [
        "GITHUB_JOB",
        "GITHUB_REF",
        "GITHUB_RUN_ATTEMPT",
        "GITHUB_RUN_ID",
        "GITHUB_RUN_NUMBER",
        "GITHUB_SHA",
    ]

    return {tag: os.environ.get(tag, "") for tag in tag_names}


@pytest.fixture
def test_name_uuid(request):
    """Test name, suffixed with a UUID.
    Useful for resources like cluster names, S3 paths, etc.
    """
    return f"{request.node.originalname}-{uuid.uuid4().hex}"


@pytest.fixture(scope="session")
def dask_env_variables():
    return {k: v for k, v in os.environ.items() if k.startswith("DASK_")}


@lru_cache(None)
def load_cluster_kwargs() -> dict:
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    base_fname = os.path.join(base_dir, "cluster_kwargs.yaml")
    with open(base_fname) as fh:
        config = yaml.safe_load(fh)

    override_fname = os.getenv("CLUSTER_KWARGS")
    if override_fname:
        override_fname = os.path.join(base_dir, override_fname)
        with open(override_fname) as fh:
            override_config = yaml.safe_load(fh)
        if override_config:
            dask.config.update(config, override_config)

    default = config.pop("default")
    config = {k: dask.config.merge(default, v) for k, v in config.items()}
    dump_cluster_kwargs(config, "merged")
    return config


def dump_cluster_kwargs(kwargs: dict, name: str) -> None:
    """Dump Cluster **kwargs to disk for forensic analysis"""
    if "DASK_COILED__TOKEN" in kwargs.get("environ", {}):
        kwargs = kwargs.copy()
        kwargs["environ"] = kwargs["environ"].copy()
        kwargs["environ"]["DASK_COILED__TOKEN"] = "<redacted for dump>"

    base_dir = os.path.join(os.path.dirname(__file__), "..")
    prefix = os.path.join(base_dir, "cluster_kwargs")
    with open(f"{prefix}.{name}.yaml", "w") as fh:
        yaml.dump(kwargs, fh)
    with open(f"{prefix}.{name}.pickle", "wb") as fh:
        pickle.dump(kwargs, fh)


@pytest.fixture(scope="session")
def cluster_kwargs():
    return load_cluster_kwargs()


@pytest.fixture(scope="module")
def small_cluster(request, dask_env_variables, cluster_kwargs, github_cluster_tags):
    module = os.path.basename(request.fspath).split(".")[0]
    module = module.replace("test_", "")
    kwargs = dict(
        name=f"{module}-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **cluster_kwargs["small_cluster"],
    )
    dump_cluster_kwargs(kwargs, f"small_cluster.{module}")
    with Cluster(**kwargs) as cluster:
        yield cluster


def log_on_scheduler(
    client: Client, msg: str, *args, level: int = logging.INFO
) -> None:
    client.run_on_scheduler(scheduler_logger.log, level, msg, *args)


@pytest.fixture
def small_client(
    request,
    testrun_uid,
    small_cluster,
    cluster_kwargs,
    upload_cluster_dump,
    benchmark_all,
):
    n_workers = cluster_kwargs["small_cluster"]["n_workers"]
    test_label = f"{request.node.name}, session_id={testrun_uid}"
    with Client(small_cluster) as client:
        log_on_scheduler(client, "Starting client setup of %s", test_label)
        client.restart()
        small_cluster.scale(n_workers)
        client.wait_for_workers(n_workers)

        with upload_cluster_dump(client):
            log_on_scheduler(client, "Finished client setup of %s", test_label)

            with benchmark_all(client):
                yield client

            # Note: normally, this RPC call is almost instantaneous. However, in the
            # case where the scheduler is still very busy when the fixtured test returns
            # (e.g. test_futures.py::test_large_map_first_work), it can be delayed into
            # several seconds. We don't want to capture this extra delay with
            # benchmark_time, as it's beyond the scope of the test.
            log_on_scheduler(client, "Starting client teardown of %s", test_label)

        client.restart()
        # Run connects to all workers once and to ensure they're up before we do
        # something else. With another call of restart when entering this
        # fixture again, this can trigger a race condition that kills workers
        # See https://github.com/dask/distributed/issues/7312 Can be removed
        # after this issue is fixed.
        client.run(lambda: None)


@pytest.fixture
def client(
    request,
    dask_env_variables,
    cluster_kwargs,
    github_cluster_tags,
    upload_cluster_dump,
    benchmark_all,
):
    name = request.param
    with Cluster(
        f"{name}-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **cluster_kwargs[name],
    ) as cluster:
        with Client(cluster) as client:
            with upload_cluster_dump(client), benchmark_all(client):
                yield client


def _mark_client(name):
    return pytest.mark.parametrize("client", [name], indirect=True)


pytest.mark.client = _mark_client


S3_REGION = "us-east-2"
S3_BUCKET = "s3://coiled-runtime-ci"


@pytest.fixture(scope="session")
def s3_storage_options():
    return {
        "key": os.environ.get("AWS_ACCESS_KEY_ID"),
        "secret": os.environ.get("AWS_SECRET_ACCESS_KEY"),
    }


@pytest.fixture(scope="session")
def s3(s3_storage_options):
    return s3fs.S3FileSystem(**s3_storage_options)


@pytest.fixture
def s3_factory(s3_storage_options):
    def _(**exta_options):
        kwargs = {**s3_storage_options, **exta_options}
        return s3fs.S3FileSystem(**kwargs)

    return _


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
def upload_cluster_dump(
    request, s3_cluster_dump_url, s3_storage_options, test_run_benchmark
):
    @contextlib.contextmanager
    def _upload_cluster_dump(client):
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
            cluster_dump = os.environ.get("CLUSTER_DUMP", "false")
            if cluster_dump == "always" or (cluster_dump == "fail" and failed):
                dump_path = (
                    f"{s3_cluster_dump_url}/{client.cluster.name}/"
                    f"{test_run_benchmark.path.replace('/', '.')}.{request.node.name}"
                )
                test_run_benchmark.cluster_dump_url = dump_path + ".msgpack.gz"
                logger.info(
                    f"Cluster state dump can be found at: {dump_path}.msgpack.gz"
                )
                client.dump_cluster_state(dump_path, **s3_storage_options)

    yield _upload_cluster_dump


# Include https://github.com/dask/distributed/pull/7410 for categorical support
P2P_SHUFFLE_AVAILABLE = Version(distributed.__version__) >= Version("2023.1.0")


@pytest.fixture(
    params=[
        "tasks",
        pytest.param(
            "p2p",
            marks=pytest.mark.skipif(
                not P2P_SHUFFLE_AVAILABLE, reason="p2p shuffle not available"
            ),
        ),
    ]
)
def shuffle_method(request):
    return request.param


@pytest.fixture
def configure_shuffling(shuffle_method):
    with dask.config.set({"dataframe.shuffle.method": shuffle_method}):
        yield


# Include https://github.com/dask/distributed/pull/7534
P2P_RECHUNK_AVAILABLE = Version(distributed.__version__) >= Version("2023.2.1")


@pytest.fixture(
    params=[
        "tasks",
        pytest.param(
            "p2p",
            marks=pytest.mark.skipif(
                not P2P_RECHUNK_AVAILABLE, reason="p2p rechunk not available"
            ),
        ),
    ]
)
def rechunk_method(request):
    return request.param


@pytest.fixture
def configure_rechunking(rechunk_method):
    config = {"optimization.fuse.active": False} if rechunk_method == "p2p" else {}
    config["array.rechunk.method"] = rechunk_method
    with dask.config.set(config):
        yield


@pytest.fixture
def read_parquet_with_pyarrow():
    """Force dask.dataframe.read_parquet() to return strings with dtype=string[pyarrow]
    FIXME https://github.com/dask/dask/issues/9840
    """
    client = distributed.get_client()

    # Set option on the workers
    class SetPandasStringsToPyArrow(WorkerPlugin):
        name = "set_pandas_strings_to_pyarrow"

        def setup(self, worker):
            pandas.set_option("string_storage", "pyarrow")

    client.register_worker_plugin(SetPandasStringsToPyArrow())

    # Set option on the client
    bak = pandas.get_option("string_storage")
    pandas.set_option("string_storage", "pyarrow")

    yield

    pandas.set_option("string_storage", bak)
    # Workers will lose the setting at the next call to client.restart()
    client.unregister_worker_plugin("set_pandas_strings_to_pyarrow")


@pytest.fixture(params=["uncompressible", "compressible"])
def new_array(request):
    """Constructor function for a new dask array.
    This fixture causes the test to run twice, first with uncompressible data and then
    when compressible data.
    """
    if request.param == "uncompressible":
        rng = da.random.default_rng()
        return rng.random
    assert request.param == "compressible"

    def compressible(x):
        """Convert a random array, that is uncompressible, to one that is
        compressible to 42% to its original size and takes 570 MiB/s to compress
        (both measured on lz4 4.0). This exhibits a fundamentally different
        performance profile from e.g. numpy.zeros_like, which would compress to 1%
        of its original size in just 140ms/GiB.

        Note
        ----
        This function must be defined inside a local scope, so that it is pickled with
        cloudpickle, or it will fail to unpickle on the workers.
        """
        y = x.reshape(-1)
        y[::2] = 0
        return y.reshape(x.shape)

    def _(*args, **kwargs):
        rng = da.random.default_rng()
        a = rng.random(*args, **kwargs)
        return a.map_blocks(compressible, dtype=a.dtype)

    return _
