"""Job orchestration service."""

import os
import re
import shlex
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import DashboardConfig
from ..db.models import Job, JobStatus, JobStatusHistory
from .rclone_service import validate_path
from .slurm_service import cancel_jobs, get_job_state, submit_job


def generate_run_id() -> str:
    """Generate a unique run ID.

    Returns:
        Run ID in format "YYYY-MM-DDTHH:MM:SSZ_hexrandom".
    """
    ts = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rnd = os.urandom(3).hex()
    return f"{ts}_{rnd}"


def validate_tag(tag: str) -> bool:
    """Validate job tag format.

    Args:
        tag: Tag to validate.

    Returns:
        True if tag is valid.
    """
    # Allow alphanumeric, hyphens, and underscores
    # 1-50 characters
    pattern = r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,49}$"
    return bool(re.match(pattern, tag))


# Manifest job script template
MANIFEST_JOB_SCRIPT = r"""#!/usr/bin/env bash
#SBATCH --job-name={manifest_job_name}
#SBATCH --output={run_dir}/manifest.out
#SBATCH --error={run_dir}/manifest.err
#SBATCH --partition={partition}
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=4:00:00

set -euo pipefail

cd "{run_dir}"

echo "== $(date -Is) Starting manifest build =="

# Build manifest
xfer manifest build \
    --source {source_path} \
    --dest {dest_path} \
    --out manifest.jsonl \
    --rclone-image {rclone_image} \
    --rclone-config {rclone_config} \
    --recursive --fast-list

echo "== $(date -Is) Manifest build complete =="

# Count objects in manifest
OBJECT_COUNT=$(wc -l < manifest.jsonl)
echo "Objects in manifest: ${{OBJECT_COUNT}}"

# Shard manifest
xfer manifest shard \
    --in manifest.jsonl \
    --outdir shards \
    --num-shards {num_shards}

echo "== $(date -Is) Sharding complete =="

# Render Slurm scripts
xfer slurm render \
    --run-dir . \
    --num-shards {num_shards} \
    --array-concurrency {array_concurrency} \
    --job-name {transfer_job_name} \
    --partition {partition} \
    --cpus-per-task {cpus_per_task} \
    --mem {mem} \
    --time-limit {time_limit} \
    --rclone-image {rclone_image} \
    --rclone-config {rclone_config} \
    --rclone-flags {rclone_flags} \
    --max-attempts {max_attempts}

echo "== $(date -Is) Render complete =="

# Submit transfer array job and capture job ID
TRANSFER_JOB_ID=$(sbatch --parsable sbatch_array.sh)
echo "TRANSFER_JOB_ID=${{TRANSFER_JOB_ID}}" > transfer_job_id.txt
echo "Submitted transfer array job: ${{TRANSFER_JOB_ID}}"

echo "== $(date -Is) Complete =="
"""


class JobService:
    """Service for managing transfer jobs."""

    def __init__(self, config: DashboardConfig, db: AsyncSession):
        """Initialize job service.

        Args:
            config: Dashboard configuration.
            db: Database session.
        """
        self.config = config
        self.db = db

    async def create_job(
        self,
        tag: str,
        source_path: str,
        dest_path: str,
        user_id: Optional[int] = None,
    ) -> Job:
        """Create a new transfer job and submit manifest job to Slurm.

        Args:
            tag: User-provided job tag.
            source_path: Source rclone path.
            dest_path: Destination rclone path.
            user_id: ID of user creating the job.

        Returns:
            Created Job instance.

        Raises:
            ValueError: If validation fails.
        """
        # Validate tag
        if not validate_tag(tag):
            raise ValueError(
                "Invalid tag. Must be 1-50 characters, alphanumeric with hyphens/underscores, "
                "starting with alphanumeric."
            )

        # Validate paths
        if not validate_path(source_path, self.config.paths.allowed_prefixes):
            raise ValueError(f"Source path '{source_path}' is not in allowed prefixes.")

        if not validate_path(dest_path, self.config.paths.allowed_prefixes):
            raise ValueError(f"Destination path '{dest_path}' is not in allowed prefixes.")

        # Generate run ID and directory
        run_id = generate_run_id()
        run_dir = self.config.paths.run_base_dir / f"xfer_{tag}_{run_id.replace(':', '-')}"

        # Create job record
        job = Job(
            tag=tag,
            source_path=source_path,
            dest_path=dest_path,
            run_id=run_id,
            run_dir=str(run_dir),
            config_snapshot=self.config.slurm.model_dump(mode="json"),
            status=JobStatus.PENDING.value,
            created_by_id=user_id,
            num_shards=self.config.slurm.num_shards,
        )

        self.db.add(job)
        await self.db.flush()

        # Create run directory
        run_dir.mkdir(parents=True, exist_ok=True)

        # Generate manifest job script
        script_content = self._generate_manifest_script(job)
        script_path = run_dir / "manifest_job.sh"
        script_path.write_text(script_content)
        script_path.chmod(0o755)

        # Submit manifest job to Slurm
        try:
            manifest_job_id = await submit_job(str(script_path))
            job.manifest_slurm_job_id = manifest_job_id
            job.status = JobStatus.MANIFEST_QUEUED.value
            job.started_at = datetime.utcnow()
        except RuntimeError as e:
            job.status = JobStatus.FAILED.value
            job.error_message = str(e)

        await self._log_status_change(job, JobStatus.PENDING.value, job.status)
        await self.db.commit()

        return job

    def _generate_manifest_script(self, job: Job) -> str:
        """Generate the manifest job sbatch script.

        Args:
            job: Job instance.

        Returns:
            Script content.
        """
        config = self.config.slurm

        return MANIFEST_JOB_SCRIPT.format(
            manifest_job_name=job.manifest_job_name,
            run_dir=job.run_dir,
            partition=config.partitions[0],
            source_path=shlex.quote(job.source_path),
            dest_path=shlex.quote(job.dest_path),
            rclone_image=shlex.quote(config.rclone_image),
            rclone_config=shlex.quote(str(config.rclone_config)),
            num_shards=config.num_shards,
            array_concurrency=config.array_concurrency,
            transfer_job_name=shlex.quote(job.job_name),
            cpus_per_task=config.cpus_per_task,
            mem=shlex.quote(config.mem),
            time_limit=shlex.quote(config.time_limit),
            rclone_flags=shlex.quote(config.rclone_flags),
            max_attempts=config.max_attempts,
        )

    async def cancel_job(self, job_id: int) -> Job:
        """Cancel a running job.

        Args:
            job_id: Job ID to cancel.

        Returns:
            Updated Job instance.

        Raises:
            ValueError: If job not found or cannot be cancelled.
        """
        job = await self.get_job(job_id)
        if job is None:
            raise ValueError(f"Job {job_id} not found.")

        # Collect Slurm job IDs to cancel
        slurm_jobs = []
        if job.manifest_slurm_job_id:
            slurm_jobs.append(job.manifest_slurm_job_id)
        if job.transfer_slurm_job_id:
            slurm_jobs.append(job.transfer_slurm_job_id)

        if slurm_jobs:
            await cancel_jobs(slurm_jobs)

        old_status = job.status
        job.status = JobStatus.CANCELLED.value
        job.completed_at = datetime.utcnow()

        await self._log_status_change(job, old_status, job.status, "Cancelled by user")
        await self.db.commit()

        return job

    async def get_job(self, job_id: int) -> Optional[Job]:
        """Get a job by ID.

        Args:
            job_id: Job ID.

        Returns:
            Job instance or None.
        """
        result = await self.db.execute(select(Job).where(Job.id == job_id))
        return result.scalar_one_or_none()

    async def list_jobs(self, limit: int = 100) -> list[Job]:
        """List recent jobs.

        Args:
            limit: Maximum number of jobs to return.

        Returns:
            List of Job instances.
        """
        result = await self.db.execute(
            select(Job).order_by(Job.created_at.desc()).limit(limit)
        )
        return list(result.scalars().all())

    async def update_job_status(self, job: Job) -> None:
        """Update job status based on Slurm job states.

        Args:
            job: Job to update.
        """
        old_status = job.status

        # Check manifest job status
        if job.status in (JobStatus.MANIFEST_QUEUED.value, JobStatus.MANIFEST_RUNNING.value):
            if job.manifest_slurm_job_id:
                state = await get_job_state(job.manifest_slurm_job_id)
                if state:
                    if state.upper() == "RUNNING":
                        job.status = JobStatus.MANIFEST_RUNNING.value
                    elif state.upper() == "COMPLETED":
                        # Manifest done, check for transfer job ID
                        await self._check_transfer_job_submitted(job)
                    elif state.upper() in ("FAILED", "CANCELLED", "TIMEOUT"):
                        job.status = JobStatus.MANIFEST_FAILED.value
                        job.completed_at = datetime.utcnow()

        # Check transfer job status
        elif job.status in (
            JobStatus.MANIFEST_DONE.value,
            JobStatus.TRANSFER_QUEUED.value,
            JobStatus.TRANSFER_RUNNING.value,
        ):
            if job.transfer_slurm_job_id:
                state = await get_job_state(job.transfer_slurm_job_id)
                if state:
                    if state.upper() in ("PENDING", "CONFIGURING"):
                        job.status = JobStatus.TRANSFER_QUEUED.value
                    elif state.upper() == "RUNNING":
                        job.status = JobStatus.TRANSFER_RUNNING.value
                    elif state.upper() == "COMPLETED":
                        job.status = JobStatus.COMPLETED.value
                        job.completed_at = datetime.utcnow()
                    elif state.upper() in ("FAILED", "CANCELLED", "TIMEOUT"):
                        job.status = JobStatus.FAILED.value
                        job.completed_at = datetime.utcnow()

        if job.status != old_status:
            await self._log_status_change(job, old_status, job.status)

    async def _check_transfer_job_submitted(self, job: Job) -> None:
        """Check if transfer job was submitted and update job accordingly.

        Args:
            job: Job to check.
        """
        run_dir = Path(job.run_dir)
        job_id_file = run_dir / "transfer_job_id.txt"

        if job_id_file.exists():
            content = job_id_file.read_text().strip()
            # Parse "TRANSFER_JOB_ID=12345"
            for line in content.split("\n"):
                if line.startswith("TRANSFER_JOB_ID="):
                    job.transfer_slurm_job_id = line.split("=", 1)[1].strip()
                    job.status = JobStatus.TRANSFER_QUEUED.value
                    break
            else:
                job.status = JobStatus.MANIFEST_DONE.value
        else:
            # File not yet written, manifest might have just finished
            job.status = JobStatus.MANIFEST_DONE.value

    async def _log_status_change(
        self,
        job: Job,
        old_status: str,
        new_status: str,
        message: Optional[str] = None,
    ) -> None:
        """Log a job status change.

        Args:
            job: Job instance.
            old_status: Previous status.
            new_status: New status.
            message: Optional message.
        """
        history = JobStatusHistory(
            job_id=job.id,
            old_status=old_status,
            new_status=new_status,
            message=message,
        )
        self.db.add(history)
