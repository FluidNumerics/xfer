"""API router aggregation."""

from fastapi import APIRouter

from . import jobs, logs, remotes

# Create main API router
api_router = APIRouter()

# Include sub-routers
api_router.include_router(jobs.router)
api_router.include_router(logs.router)
api_router.include_router(remotes.router)
