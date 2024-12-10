import contextlib
import os
import tarfile
import time
import uuid

import coiled
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--scale",
        action="store",
        default="small",
        help="Scale to run. Either 'small' or 'large'",
    )

    parser.addoption(
        "--memray",
        action="store",
        default="scheduler",
        help="Memray profiles to collect: scheduler or none",
        choices=("scheduler", "none"),
    )

    parser.addoption(
        "--py-spy",
        action="store",
        default="none",
        help="py-spy profiles to collect: scheduler, workers, all, or none",
        choices=("scheduler", "workers", "all", "none"),
    )


@pytest.fixture(scope="session")
def scale(request):
    return request.config.getoption("scale")


@pytest.fixture(scope="module")
def cluster_name(request, scale):
    module = os.path.basename(request.fspath).split(".")[0]
    module = module.replace("test_", "")
    return f"geospatial-{module}-{scale}-{uuid.uuid4().hex[:8]}"


@pytest.fixture(
    params=[
        pytest.param("execution", marks=pytest.mark.geo_execution),
        pytest.param("submission", marks=pytest.mark.geo_submission),
    ]
)
def benchmark_type(request):
    return request.param


@pytest.fixture
def memray_profile(
    pytestconfig,
    s3,
    s3_performance_url,
    s3_storage_options,
    test_run_benchmark,
    tmp_path,
):
    if not test_run_benchmark:
        yield contextlib.nullcontext
    else:
        memray_option = pytestconfig.getoption("--memray")

        if memray_option == "none":
            yield contextlib.nullcontext
            return

        if memray_option != "scheduler":
            raise ValueError(f"Unhandled value for --memray: {memray_option}")

        try:
            from dask.distributed.diagnostics import memray
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                "memray profiling benchmarks requires memray to be installed."
            ) from e

        @contextlib.contextmanager
        def _memray_profile(client):
            local_directory = tmp_path / "profiles" / "memray"
            local_directory.mkdir(parents=True)
            try:
                with memray.memray_scheduler(directory=local_directory):
                    yield
            finally:
                archive_name = "memray.tar.gz"
                archive = tmp_path / archive_name
                with tarfile.open(archive, mode="w:gz") as tar:
                    for item in local_directory.iterdir():
                        tar.add(item, arcname=item.name)
                destination = f"{s3_performance_url}/{archive_name}"
                test_run_benchmark.memray_profiles_url = destination
                s3.put_file(archive, destination)

        yield _memray_profile


@pytest.fixture
def py_spy_profile(
    pytestconfig,
    s3,
    s3_performance_url,
    s3_storage_options,
    test_run_benchmark,
    tmp_path,
):
    if not test_run_benchmark:
        yield contextlib.nullcontext
        return

    py_spy_option = pytestconfig.getoption("--py-spy")
    if py_spy_option == "none":
        yield contextlib.nullcontext
        return

    profile_scheduler = False
    profile_workers = False

    if py_spy_option == "scheduler":
        profile_scheduler = True
    elif py_spy_option == "workers":
        profile_workers = True
    elif py_spy_option == "all":
        profile_scheduler = True
        profile_workers = True
    else:
        raise ValueError(f"Unhandled value for --py-spy: {py_spy_option}")

    try:
        from dask_pyspy import pyspy, pyspy_on_scheduler
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "py-spy profiling benchmarks requires dask-pyspy to be installed."
        ) from e

    @contextlib.contextmanager
    def _py_spy_profile(client):
        local_directory = tmp_path / "profiles" / "py-spy"
        local_directory.mkdir(parents=True)

        worker_ctx = contextlib.nullcontext()
        if profile_workers:
            worker_ctx = pyspy(local_directory, client=client)

        scheduler_ctx = contextlib.nullcontext()
        if profile_scheduler:
            scheduler_ctx = pyspy_on_scheduler(
                local_directory / "scheduler.json", client=client
            )

        try:
            with worker_ctx, scheduler_ctx:
                yield
        finally:
            archive_name = "py-spy.tar.gz"
            archive = tmp_path / archive_name
            with tarfile.open(archive, mode="w:gz") as tar:
                for item in local_directory.iterdir():
                    tar.add(item, arcname=item.name)
            destination = f"{s3_performance_url}/{archive_name}"
            test_run_benchmark.py_spy_profiles_url = destination
            s3.put_file(archive, destination)

    yield _py_spy_profile


@pytest.fixture
def setup_benchmark(
    benchmark_type,
    cluster_name,
    github_cluster_tags,
    benchmark_all,
    memray_profile,
    py_spy_profile,
    performance_report,
):
    should_execute = benchmark_type == "execution"
    import contextlib

    @contextlib.contextmanager
    def _(n_workers, env=None, **cluster_kwargs):
        n_workers = n_workers if should_execute else 0
        with coiled.Cluster(
            name=cluster_name,
            tags=github_cluster_tags,
            n_workers=n_workers,
            **cluster_kwargs,
        ) as cluster:
            if env:
                cluster.send_private_envs(env=env)
            with cluster.get_client() as client:
                if should_execute:
                    # FIXME https://github.com/coiled/platform/issues/103
                    client.wait_for_workers(n_workers)
                    with performance_report(), py_spy_profile(client), memray_profile(
                        client
                    ), benchmark_all(client):

                        def benchmark_execution(func, *args, **kwargs):
                            func(*args, **kwargs).compute()

                        yield benchmark_execution
                else:
                    submitted = False

                    def track_submission(event):
                        nonlocal submitted
                        ts, msg = event
                        if not isinstance(msg, dict):
                            return

                        if not msg.get("action", None) == "update-graph":
                            return

                        submitted = True

                    def benchmark_submission(func, *args, **kwargs):
                        _ = client.compute(func(*args, **kwargs), sync=False)
                        while submitted is False:
                            time.sleep(0.1)

                    client.subscribe_topic("scheduler", track_submission)
                    with performance_report(), py_spy_profile(client), memray_profile(
                        client
                    ), benchmark_all(client):
                        yield benchmark_submission

    return _
