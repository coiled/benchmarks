from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TestRun(Base):
    __tablename__ = "test_run"

    # unique run ID
    id = Column(Integer, primary_key=True)

    # pytest data
    session_id = Column(String, nullable=False)
    name = Column(String, nullable=False)
    originalname = Column(String, nullable=False)
    path = Column(String, nullable=True)
    setup_outcome = Column(String, nullable=True)
    call_outcome = Column(String, nullable=True)
    teardown_outcome = Column(String, nullable=True)

    # Runtime data
    coiled_runtime_version = Column(String, nullable=True)
    coiled_software_name = Column(String, nullable=True)
    dask_version = Column(String, nullable=True)
    dask_expr_version = Column(String, nullable=True)
    distributed_version = Column(String, nullable=True)
    python_version = Column(String, nullable=True)
    platform = Column(String, nullable=True)

    # CI runner data
    ci_run_url = Column(String, nullable=True)

    # Wall clock data
    start = Column(DateTime, nullable=True)
    end = Column(DateTime, nullable=True)
    duration = Column(Float, nullable=True)

    # Memory data
    average_memory = Column(Float, nullable=True)
    peak_memory = Column(Float, nullable=True)

    # Durations data
    compute_time = Column(Float, nullable=True)
    disk_spill_time = Column(Float, nullable=True)
    serializing_time = Column(Float, nullable=True)
    transfer_time = Column(Float, nullable=True)

    # Scheduler
    scheduler_cpu_avg = Column(Float, nullable=True)
    scheduler_memory_max = Column(Float, nullable=True)

    # Event Loop
    worker_max_tick = Column(Float, nullable=True)
    scheduler_max_tick = Column(Float, nullable=True)

    # Cluster name/id/details_url
    cluster_name = Column(String, nullable=True)
    cluster_id = Column(Integer, nullable=True)
    cluster_details_url = Column(String, nullable=True)

    # Artifacts
    performance_report_url = Column(String, nullable=True)  # Not yet collected
    cluster_dump_url = Column(String, nullable=True)
    memray_profiles_url = Column(String, nullable=True)


class TPCHRun(Base):
    __tablename__ = "tpch_run"

    # unique run ID
    id = Column(Integer, primary_key=True)

    # pytest data
    session_id = Column(String, nullable=False)
    name = Column(String, nullable=False)
    originalname = Column(String, nullable=False)
    path = Column(String, nullable=True)
    setup_outcome = Column(String, nullable=True)
    call_outcome = Column(String, nullable=True)
    teardown_outcome = Column(String, nullable=True)

    # Runtime data
    dask_version = Column(String, nullable=True)
    dask_expr_version = Column(String, nullable=True)
    distributed_version = Column(String, nullable=True)
    duckdb_version = Column(String, nullable=True)
    pyspark_version = Column(String, nullable=True)
    polars_version = Column(String, nullable=True)

    python_version = Column(String, nullable=True)
    platform = Column(String, nullable=True)

    # CI runner data
    ci_run_url = Column(String, nullable=True)

    # Wall clock data
    start = Column(DateTime, nullable=True)
    end = Column(DateTime, nullable=True)
    duration = Column(Float, nullable=True)

    # Memory data
    average_memory = Column(Float, nullable=True)
    peak_memory = Column(Float, nullable=True)

    # Cluster name/id/details_url
    cluster_name = Column(String, nullable=True)
    cluster_id = Column(Integer, nullable=True)
    cluster_details_url = Column(String, nullable=True)

    scale = Column(Integer, nullable=False)
    query = Column(Integer, nullable=False)
    local = Column(Boolean, nullable=False)

    compression = Column(String, nullable=True)
    partition_size = Column(String, nullable=True)
    partition_size = Column(String, nullable=True)

    n_workers = Column(Integer, nullable=True)
    worker_vm_type = Column(String, nullable=True)
    cluster_disk_size = Column(Integer, nullable=True)
