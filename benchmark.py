from sqlalchemy import Column, DateTime, Float, Integer, String
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
    setup_outcome = Column(String, nullable=True)
    call_outcome = Column(String, nullable=True)
    teardown_outcome = Column(String, nullable=True)

    # Wall clock data
    start = Column(DateTime, nullable=True)
    end = Column(DateTime, nullable=True)
    duration = Column(Float, nullable=True)

    # Memory data
    average_memory = Column(Float, nullable=True)
    peak_memory = Column(Float, nullable=True)

    # Timing data
    compute_time = Column(Float, nullable=True)
    disk_spill_time = Column(Float, nullable=True)
    serializing_time = Column(Float, nullable=True)
    transfer_time = Column(Float, nullable=True)
