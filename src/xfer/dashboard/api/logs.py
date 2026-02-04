"""API endpoints for log streaming."""

import asyncio
from pathlib import Path
from typing import Optional

import aiofiles
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth import User, require_auth
from ..db.session import get_db
from ..services.job_service import JobService

router = APIRouter(prefix="/api/logs", tags=["logs"])


def _validate_log_path(job_run_dir: str, log_path: Path) -> bool:
    """Validate that a log path is within the job's run directory.

    Prevents directory traversal attacks.

    Args:
        job_run_dir: Job's run directory.
        log_path: Requested log path.

    Returns:
        True if path is valid and safe.
    """
    try:
        run_dir = Path(job_run_dir).resolve()
        resolved_path = log_path.resolve()
        return str(resolved_path).startswith(str(run_dir))
    except (ValueError, OSError):
        return False


@router.get("/{job_id}/manifest")
async def stream_manifest_log(
    request: Request,
    job_id: int,
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
):
    """Stream manifest job log via Server-Sent Events.

    Args:
        job_id: Job ID.

    Returns:
        SSE stream of log content.
    """
    config = request.app.state.config
    job_service = JobService(config, db)
    job = await job_service.get_job(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    log_path = Path(job.run_dir) / "manifest.out"

    if not log_path.exists():
        raise HTTPException(status_code=404, detail="Manifest log not found")

    if not _validate_log_path(job.run_dir, log_path):
        raise HTTPException(status_code=403, detail="Invalid log path")

    async def generate():
        """Generate SSE events from log file."""
        try:
            async with aiofiles.open(log_path, mode="r") as f:
                # Send existing content
                content = await f.read()
                if content:
                    for line in content.split("\n"):
                        yield f"data: {line}\n\n"

                # Follow for new content (tail -f style)
                while True:
                    line = await f.readline()
                    if line:
                        yield f"data: {line.rstrip()}\n\n"
                    else:
                        # Send heartbeat to keep connection alive
                        yield ": heartbeat\n\n"
                        await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


@router.get("/{job_id}/manifest/content")
async def get_manifest_log_content(
    request: Request,
    job_id: int,
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
):
    """Get manifest log content as plain text.

    Args:
        job_id: Job ID.

    Returns:
        Log content.
    """
    config = request.app.state.config
    job_service = JobService(config, db)
    job = await job_service.get_job(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    log_path = Path(job.run_dir) / "manifest.out"

    if not log_path.exists():
        return {"content": "", "exists": False}

    if not _validate_log_path(job.run_dir, log_path):
        raise HTTPException(status_code=403, detail="Invalid log path")

    try:
        content = log_path.read_text()
        return {"content": content, "exists": True}
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Error reading log: {e}")


@router.get("/{job_id}/shard/{shard_id}")
async def get_shard_log(
    request: Request,
    job_id: int,
    shard_id: int,
    attempt: int = 1,
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
):
    """Get shard log content.

    Args:
        job_id: Job ID.
        shard_id: Shard ID.
        attempt: Attempt number (default 1).

    Returns:
        Log content.
    """
    config = request.app.state.config
    job_service = JobService(config, db)
    job = await job_service.get_job(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    log_path = Path(job.run_dir) / "logs" / f"shard_{shard_id}_attempt_{attempt}.log"

    if not _validate_log_path(job.run_dir, log_path):
        raise HTTPException(status_code=403, detail="Invalid log path")

    if not log_path.exists():
        # Try to find any attempt
        logs_dir = Path(job.run_dir) / "logs"
        available = []
        if logs_dir.exists():
            for f in logs_dir.glob(f"shard_{shard_id}_attempt_*.log"):
                try:
                    att = int(f.stem.split("_")[-1])
                    available.append(att)
                except ValueError:
                    pass

        return {
            "content": "",
            "exists": False,
            "shard_id": shard_id,
            "attempt": attempt,
            "available_attempts": sorted(available),
        }

    try:
        content = log_path.read_text()
        return {
            "content": content,
            "exists": True,
            "shard_id": shard_id,
            "attempt": attempt,
        }
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Error reading log: {e}")


@router.get("/{job_id}/shards")
async def list_shard_logs(
    request: Request,
    job_id: int,
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
):
    """List available shard logs for a job.

    Args:
        job_id: Job ID.

    Returns:
        List of available shard logs.
    """
    config = request.app.state.config
    job_service = JobService(config, db)
    job = await job_service.get_job(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    logs_dir = Path(job.run_dir) / "logs"

    if not logs_dir.exists():
        return {"shards": []}

    # Parse log filenames to get shard info
    shards = {}
    for f in logs_dir.glob("shard_*_attempt_*.log"):
        try:
            parts = f.stem.split("_")
            shard_id = int(parts[1])
            attempt = int(parts[3])

            if shard_id not in shards:
                shards[shard_id] = {"shard_id": shard_id, "attempts": []}
            shards[shard_id]["attempts"].append(attempt)
        except (ValueError, IndexError):
            continue

    # Sort attempts
    for shard in shards.values():
        shard["attempts"].sort()

    return {"shards": sorted(shards.values(), key=lambda x: x["shard_id"])}
