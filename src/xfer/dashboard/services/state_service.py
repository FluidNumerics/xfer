"""State directory monitoring service."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.models import Job, JobStatus


@dataclass
class ShardState:
    """State of a single shard."""

    shard_id: int
    is_done: bool = False
    is_failed: bool = False
    attempt_count: int = 0
    exit_code: Optional[int] = None


@dataclass
class JobProgress:
    """Progress information for a job."""

    total_shards: int
    done: int
    failed: int
    running: int
    pending: int

    done_ids: List[int]
    failed_ids: List[int]
    running_ids: List[int]

    @property
    def percent_complete(self) -> float:
        """Get completion percentage."""
        if self.total_shards == 0:
            return 0.0
        return (self.done / self.total_shards) * 100

    @property
    def percent_failed(self) -> float:
        """Get failure percentage."""
        if self.total_shards == 0:
            return 0.0
        return (self.failed / self.total_shards) * 100


class StateService:
    """Service for monitoring job state directories."""

    def get_shard_state(self, run_dir: Path, shard_id: int) -> ShardState:
        """Get state of a specific shard.

        Args:
            run_dir: Path to run directory.
            shard_id: Shard ID.

        Returns:
            ShardState instance.
        """
        state_dir = run_dir / "state"
        state = ShardState(shard_id=shard_id)

        # Check done file
        done_file = state_dir / f"shard_{shard_id}.done"
        state.is_done = done_file.exists()

        # Check fail file
        fail_file = state_dir / f"shard_{shard_id}.fail"
        if fail_file.exists():
            state.is_failed = True
            try:
                content = fail_file.read_text().strip()
                state.exit_code = int(content)
            except (ValueError, OSError):
                pass

        # Check attempt file
        attempt_file = state_dir / f"shard_{shard_id}.attempt"
        if attempt_file.exists():
            try:
                content = attempt_file.read_text().strip()
                state.attempt_count = int(content)
            except (ValueError, OSError):
                pass

        return state

    def get_job_progress(self, run_dir: Path, num_shards: int) -> JobProgress:
        """Get progress for a job from its state directory.

        Args:
            run_dir: Path to run directory.
            num_shards: Total number of shards.

        Returns:
            JobProgress instance.

        State directory structure:
        - state/shard_{id}.done  - empty file, indicates completion
        - state/shard_{id}.fail  - contains exit code
        - state/shard_{id}.attempt - contains attempt count
        """
        state_dir = Path(run_dir) / "state"

        if not state_dir.exists():
            return JobProgress(
                total_shards=num_shards,
                done=0,
                failed=0,
                running=0,
                pending=num_shards,
                done_ids=[],
                failed_ids=[],
                running_ids=[],
            )

        # Collect state file information
        done_ids: Set[int] = set()
        fail_ids: Set[int] = set()
        attempt_ids: Set[int] = set()

        for f in state_dir.glob("shard_*.done"):
            shard_id = self._extract_shard_id(f)
            if shard_id is not None:
                done_ids.add(shard_id)

        for f in state_dir.glob("shard_*.fail"):
            shard_id = self._extract_shard_id(f)
            if shard_id is not None:
                fail_ids.add(shard_id)

        for f in state_dir.glob("shard_*.attempt"):
            shard_id = self._extract_shard_id(f)
            if shard_id is not None:
                attempt_ids.add(shard_id)

        # Calculate states
        # Failed = has .fail but no .done (could have retried to success)
        failed_ids = fail_ids - done_ids

        # Running = has .attempt but no .done or .fail
        running_ids = attempt_ids - done_ids - fail_ids

        # Pending = total - done - failed - running
        done_count = len(done_ids)
        failed_count = len(failed_ids)
        running_count = len(running_ids)
        pending_count = max(0, num_shards - done_count - failed_count - running_count)

        return JobProgress(
            total_shards=num_shards,
            done=done_count,
            failed=failed_count,
            running=running_count,
            pending=pending_count,
            done_ids=sorted(done_ids),
            failed_ids=sorted(failed_ids),
            running_ids=sorted(running_ids),
        )

    def _extract_shard_id(self, path: Path) -> Optional[int]:
        """Extract shard ID from state file path.

        Args:
            path: Path like "shard_123.done".

        Returns:
            Shard ID or None if parsing fails.
        """
        try:
            # shard_123.done -> shard_123 -> 123
            name = path.stem  # shard_123
            parts = name.split("_")
            if len(parts) >= 2:
                return int(parts[1])
        except (ValueError, IndexError):
            pass
        return None

    def get_manifest_stats(self, run_dir: Path) -> Optional[Dict]:
        """Get manifest statistics from shards.meta.json.

        Args:
            run_dir: Path to run directory.

        Returns:
            Dictionary with manifest stats or None.
        """
        meta_file = Path(run_dir) / "shards" / "shards.meta.json"
        if not meta_file.exists():
            return None

        try:
            with open(meta_file) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None


async def update_all_active_jobs(db: AsyncSession) -> None:
    """Update progress for all active jobs.

    Args:
        db: Database session.
    """
    from .job_service import JobService

    # Get all active jobs
    active_statuses = [
        JobStatus.MANIFEST_QUEUED.value,
        JobStatus.MANIFEST_RUNNING.value,
        JobStatus.MANIFEST_DONE.value,
        JobStatus.TRANSFER_QUEUED.value,
        JobStatus.TRANSFER_RUNNING.value,
    ]

    result = await db.execute(select(Job).where(Job.status.in_(active_statuses)))
    jobs = result.scalars().all()

    state_service = StateService()

    for job in jobs:
        run_dir = Path(job.run_dir)

        # Update shard progress from state directory
        if job.num_shards and run_dir.exists():
            progress = state_service.get_job_progress(run_dir, job.num_shards)
            job.shards_done = progress.done
            job.shards_failed = progress.failed
            job.shards_running = progress.running

            # Get manifest stats if available
            stats = state_service.get_manifest_stats(run_dir)
            if stats:
                job.manifest_object_count = stats.get("num_records")
                job.total_bytes = stats.get("bytes_total")

        # Update job status from Slurm (this is imported here to avoid circular imports)
        # Note: We need the config to create JobService, which we don't have here
        # So we'll just update the progress and let the status update happen elsewhere

    await db.commit()
