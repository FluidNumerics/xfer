"""Slurm interaction service."""

import asyncio
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class SlurmJobInfo:
    """Information about a Slurm job."""

    job_id: str
    name: str
    state: str
    elapsed: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    exit_code: Optional[int] = None


async def submit_job(script_path: str) -> str:
    """Submit a job to Slurm via sbatch.

    Args:
        script_path: Path to the sbatch script.

    Returns:
        Slurm job ID.

    Raises:
        RuntimeError: If sbatch fails.
    """
    result = await asyncio.to_thread(
        subprocess.run,
        ["sbatch", "--parsable", script_path],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"sbatch failed: {result.stderr}")

    # sbatch --parsable returns job_id or job_id;cluster
    job_id = result.stdout.strip().split(";")[0]
    return job_id


async def cancel_jobs(job_ids: List[str]) -> None:
    """Cancel one or more Slurm jobs.

    Args:
        job_ids: List of Slurm job IDs to cancel.
    """
    if not job_ids:
        return

    await asyncio.to_thread(
        subprocess.run,
        ["scancel"] + job_ids,
        capture_output=True,
        check=False,
    )


async def get_job_info(job_id: str) -> Optional[SlurmJobInfo]:
    """Get information about a Slurm job.

    Args:
        job_id: Slurm job ID.

    Returns:
        SlurmJobInfo if found, None otherwise.
    """
    result = await asyncio.to_thread(
        subprocess.run,
        [
            "sacct",
            "-j",
            job_id,
            "--format=JobID,JobName,State,Elapsed,Start,End,ExitCode",
            "--noheader",
            "--parsable2",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0 or not result.stdout.strip():
        return None

    # Parse first line (main job, not steps)
    lines = result.stdout.strip().split("\n")
    for line in lines:
        parts = line.split("|")
        if len(parts) >= 7:
            # Skip job steps (contain '.')
            if "." in parts[0]:
                continue

            exit_code = None
            if parts[6] and ":" in parts[6]:
                # Format is "exit_code:signal"
                try:
                    exit_code = int(parts[6].split(":")[0])
                except ValueError:
                    pass

            return SlurmJobInfo(
                job_id=parts[0],
                name=parts[1],
                state=parts[2],
                elapsed=parts[3] if parts[3] else None,
                start_time=_parse_slurm_time(parts[4]),
                end_time=_parse_slurm_time(parts[5]),
                exit_code=exit_code,
            )

    return None


async def get_job_state(job_id: str) -> Optional[str]:
    """Get the state of a Slurm job.

    Args:
        job_id: Slurm job ID.

    Returns:
        Job state string (e.g., "PENDING", "RUNNING", "COMPLETED"), or None.
    """
    info = await get_job_info(job_id)
    return info.state if info else None


async def is_job_running(job_id: str) -> bool:
    """Check if a Slurm job is still running or pending.

    Args:
        job_id: Slurm job ID.

    Returns:
        True if job is running or pending.
    """
    state = await get_job_state(job_id)
    if state is None:
        return False
    return state.upper() in ("PENDING", "RUNNING", "CONFIGURING", "COMPLETING")


async def is_job_completed(job_id: str) -> bool:
    """Check if a Slurm job has completed successfully.

    Args:
        job_id: Slurm job ID.

    Returns:
        True if job completed successfully.
    """
    state = await get_job_state(job_id)
    if state is None:
        return False
    return state.upper() == "COMPLETED"


async def is_job_failed(job_id: str) -> bool:
    """Check if a Slurm job has failed.

    Args:
        job_id: Slurm job ID.

    Returns:
        True if job failed.
    """
    state = await get_job_state(job_id)
    if state is None:
        return False
    return state.upper() in ("FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "PREEMPTED")


def _parse_slurm_time(time_str: str) -> Optional[datetime]:
    """Parse Slurm timestamp format.

    Args:
        time_str: Slurm timestamp (e.g., "2024-01-15T10:30:00").

    Returns:
        datetime object or None if parsing fails.
    """
    if not time_str or time_str == "Unknown":
        return None

    try:
        return datetime.fromisoformat(time_str)
    except ValueError:
        return None
