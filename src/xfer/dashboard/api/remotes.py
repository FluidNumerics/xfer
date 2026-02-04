"""API endpoints for rclone remote discovery."""

from typing import List

from fastapi import APIRouter, Depends, Request

from ..auth import User, require_auth
from ..services.rclone_service import (
    get_allowed_local_prefixes,
    get_allowed_remotes,
)

router = APIRouter(prefix="/api", tags=["remotes"])


@router.get("/remotes")
async def list_remotes(
    request: Request,
    user: User = Depends(require_auth),
) -> dict:
    """List available rclone remotes and local path prefixes.

    Returns remotes parsed from the admin-configured rclone.conf that
    match the allowed_prefixes configuration.

    Returns:
        Dictionary with 'remotes' and 'local_prefixes' lists.
    """
    config = request.app.state.config

    # Get allowed remotes from rclone.conf
    remotes = get_allowed_remotes(
        config.slurm.rclone_config,
        config.paths.allowed_prefixes,
    )

    # Get allowed local prefixes
    local_prefixes = get_allowed_local_prefixes(config.paths.allowed_prefixes)

    return {
        "remotes": [r.to_dict() for r in remotes],
        "local_prefixes": local_prefixes,
    }


@router.get("/remotes/sources")
async def list_source_options(
    request: Request,
    user: User = Depends(require_auth),
) -> List[dict]:
    """List all available source options for the job creation form.

    Combines remotes and local prefixes into a unified list for UI dropdowns.

    Returns:
        List of source options with 'value', 'label', and 'type' fields.
    """
    config = request.app.state.config

    options = []

    # Add remotes
    remotes = get_allowed_remotes(
        config.slurm.rclone_config,
        config.paths.allowed_prefixes,
    )
    for remote in remotes:
        options.append(
            {
                "value": remote.prefix,
                "label": remote.display_name,
                "type": "remote",
                "remote_type": remote.type,
            }
        )

    # Add local prefixes
    local_prefixes = get_allowed_local_prefixes(config.paths.allowed_prefixes)
    for prefix in local_prefixes:
        options.append(
            {
                "value": prefix,
                "label": f"Local: {prefix}",
                "type": "local",
            }
        )

    return options
