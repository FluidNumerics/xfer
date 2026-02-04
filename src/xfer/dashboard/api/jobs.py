"""API endpoints for job management."""

from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth import User, require_auth
from ..db.models import Job
from ..db.session import get_db
from ..services.job_service import JobService
from ..services.rclone_service import get_allowed_local_prefixes, get_allowed_remotes
from ..services.state_service import StateService

router = APIRouter(tags=["jobs"])


@router.get("/jobs", response_class=HTMLResponse)
async def list_jobs_page(
    request: Request,
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
):
    """List all jobs page."""
    config = request.app.state.config
    job_service = JobService(config, db)
    jobs = await job_service.list_jobs(limit=100)

    return request.app.state.templates.TemplateResponse(
        "jobs/list.html",
        {"request": request, "user": user, "jobs": jobs},
    )


@router.get("/jobs/new", response_class=HTMLResponse)
async def new_job_page(
    request: Request,
    user: User = Depends(require_auth),
):
    """Show job creation form."""
    config = request.app.state.config

    # Get available remotes and local prefixes
    remotes = get_allowed_remotes(
        config.slurm.rclone_config,
        config.paths.allowed_prefixes,
    )
    local_prefixes = get_allowed_local_prefixes(config.paths.allowed_prefixes)

    return request.app.state.templates.TemplateResponse(
        "jobs/create.html",
        {
            "request": request,
            "user": user,
            "config": config,
            "remotes": remotes,
            "local_prefixes": local_prefixes,
        },
    )


@router.post("/jobs")
async def create_job(
    request: Request,
    tag: str = Form(...),
    source_path: str = Form(...),
    dest_path: str = Form(...),
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
):
    """Create a new transfer job."""
    config = request.app.state.config
    job_service = JobService(config, db)

    try:
        job = await job_service.create_job(
            tag=tag,
            source_path=source_path,
            dest_path=dest_path,
            user_id=user.id,
        )
        return RedirectResponse(url=f"/jobs/{job.id}", status_code=303)
    except ValueError as e:
        # Re-render form with error
        remotes = get_allowed_remotes(
            config.slurm.rclone_config,
            config.paths.allowed_prefixes,
        )
        local_prefixes = get_allowed_local_prefixes(config.paths.allowed_prefixes)

        return request.app.state.templates.TemplateResponse(
            "jobs/create.html",
            {
                "request": request,
                "user": user,
                "config": config,
                "remotes": remotes,
                "local_prefixes": local_prefixes,
                "error": str(e),
                "form_data": {"tag": tag, "source_path": source_path, "dest_path": dest_path},
            },
            status_code=400,
        )


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job_detail_page(
    request: Request,
    job_id: int,
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
):
    """Show job detail page."""
    config = request.app.state.config
    job_service = JobService(config, db)
    job = await job_service.get_job(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    # Get progress from state directory
    state_service = StateService()
    progress = None
    if job.num_shards:
        progress = state_service.get_job_progress(Path(job.run_dir), job.num_shards)

    return request.app.state.templates.TemplateResponse(
        "jobs/detail.html",
        {
            "request": request,
            "user": user,
            "job": job,
            "progress": progress,
        },
    )


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(
    request: Request,
    job_id: int,
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
):
    """Cancel a running job."""
    config = request.app.state.config
    job_service = JobService(config, db)

    try:
        await job_service.cancel_job(job_id)
        return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# HTMX partials


@router.get("/htmx/jobs/table", response_class=HTMLResponse)
async def jobs_table_partial(
    request: Request,
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
):
    """Partial: jobs table for htmx polling."""
    config = request.app.state.config
    job_service = JobService(config, db)
    jobs = await job_service.list_jobs(limit=100)

    return request.app.state.templates.TemplateResponse(
        "partials/job_table.html",
        {"request": request, "jobs": jobs},
    )


@router.get("/htmx/jobs/{job_id}/progress", response_class=HTMLResponse)
async def job_progress_partial(
    request: Request,
    job_id: int,
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
):
    """Partial: job progress for htmx polling."""
    config = request.app.state.config
    job_service = JobService(config, db)
    job = await job_service.get_job(job_id)

    if job is None:
        return HTMLResponse("<div>Job not found</div>", status_code=404)

    # Get progress from state directory
    state_service = StateService()
    progress = None
    if job.num_shards:
        progress = state_service.get_job_progress(Path(job.run_dir), job.num_shards)

    return request.app.state.templates.TemplateResponse(
        "partials/progress_bar.html",
        {"request": request, "job": job, "progress": progress},
    )


# JSON API endpoints


@router.get("/api/jobs")
async def list_jobs_api(
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
):
    """List jobs as JSON."""
    config = request.app.state.config
    job_service = JobService(config, db)
    jobs = await job_service.list_jobs(limit=100)

    return [
        {
            "id": job.id,
            "tag": job.tag,
            "status": job.status,
            "source_path": job.source_path,
            "dest_path": job.dest_path,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "num_shards": job.num_shards,
            "shards_done": job.shards_done,
            "shards_failed": job.shards_failed,
            "progress_percent": job.progress_percent,
        }
        for job in jobs
    ]


@router.get("/api/jobs/{job_id}")
async def get_job_api(
    job_id: int,
    user: User = Depends(require_auth),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
):
    """Get job details as JSON."""
    config = request.app.state.config
    job_service = JobService(config, db)
    job = await job_service.get_job(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    # Get progress
    state_service = StateService()
    progress = None
    if job.num_shards:
        progress = state_service.get_job_progress(Path(job.run_dir), job.num_shards)

    return {
        "id": job.id,
        "tag": job.tag,
        "status": job.status,
        "source_path": job.source_path,
        "dest_path": job.dest_path,
        "run_id": job.run_id,
        "run_dir": job.run_dir,
        "manifest_slurm_job_id": job.manifest_slurm_job_id,
        "transfer_slurm_job_id": job.transfer_slurm_job_id,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "num_shards": job.num_shards,
        "shards_done": job.shards_done,
        "shards_failed": job.shards_failed,
        "shards_running": job.shards_running,
        "total_bytes": job.total_bytes,
        "manifest_object_count": job.manifest_object_count,
        "progress_percent": job.progress_percent,
        "error_message": job.error_message,
        "progress": {
            "total": progress.total_shards,
            "done": progress.done,
            "failed": progress.failed,
            "running": progress.running,
            "pending": progress.pending,
        }
        if progress
        else None,
    }
