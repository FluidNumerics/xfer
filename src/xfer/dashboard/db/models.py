"""SQLAlchemy ORM models for the dashboard."""

from datetime import datetime
from enum import Enum
from typing import List, Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class JobStatus(str, Enum):
    """Job status enumeration."""

    PENDING = "pending"
    MANIFEST_QUEUED = "manifest_queued"
    MANIFEST_RUNNING = "manifest_running"
    MANIFEST_FAILED = "manifest_failed"
    MANIFEST_DONE = "manifest_done"
    TRANSFER_QUEUED = "transfer_queued"
    TRANSFER_RUNNING = "transfer_running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class User(Base):
    """User model for Google SSO authenticated users."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    picture_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationships
    jobs: Mapped[List["Job"]] = relationship("Job", back_populates="created_by")

    def __repr__(self) -> str:
        return f"<User(id={self.id}, email={self.email!r})>"


class Job(Base):
    """Transfer job model."""

    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # User-provided fields
    tag: Mapped[str] = mapped_column(String(100), nullable=False)
    source_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    dest_path: Mapped[str] = mapped_column(String(1024), nullable=False)

    # System-generated fields
    run_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    run_dir: Mapped[str] = mapped_column(String(1024), nullable=False)

    # Admin config snapshot (frozen at job creation)
    config_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)

    # Status tracking
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=JobStatus.PENDING.value
    )

    # Slurm job IDs
    manifest_slurm_job_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    transfer_slurm_job_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    # User relationship
    created_by_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=True
    )
    created_by: Mapped[Optional["User"]] = relationship("User", back_populates="jobs")

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Cached progress (updated by background task)
    num_shards: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    shards_done: Mapped[int] = mapped_column(Integer, default=0)
    shards_failed: Mapped[int] = mapped_column(Integer, default=0)
    shards_running: Mapped[int] = mapped_column(Integer, default=0)
    total_bytes: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    manifest_object_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Error info
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    status_history: Mapped[List["JobStatusHistory"]] = relationship(
        "JobStatusHistory", back_populates="job", cascade="all, delete-orphan"
    )

    @property
    def job_name(self) -> str:
        """Get the Slurm job name."""
        return f"xfer_{self.tag}"

    @property
    def manifest_job_name(self) -> str:
        """Get the manifest Slurm job name."""
        return f"xfer_{self.tag}_manifest"

    @property
    def progress_percent(self) -> float:
        """Get completion percentage."""
        if not self.num_shards or self.num_shards == 0:
            return 0.0
        return (self.shards_done / self.num_shards) * 100

    def __repr__(self) -> str:
        return f"<Job(id={self.id}, tag={self.tag!r}, status={self.status!r})>"


class JobStatusHistory(Base):
    """Audit log for job status changes."""

    __tablename__ = "job_status_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    old_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    new_status: Mapped[str] = mapped_column(String(32), nullable=False)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    job: Mapped["Job"] = relationship("Job", back_populates="status_history")

    def __repr__(self) -> str:
        return f"<JobStatusHistory(job_id={self.job_id}, {self.old_status} -> {self.new_status})>"
