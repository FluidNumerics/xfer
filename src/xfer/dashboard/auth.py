"""Google SSO authentication and session handling."""

from datetime import datetime
from typing import Optional

from fastapi import Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi_sso.sso.google import GoogleSSO
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import DashboardConfig
from .db.models import User
from .db.session import get_db


def create_google_sso(config: DashboardConfig) -> GoogleSSO:
    """Create Google SSO instance from config.

    Args:
        config: Dashboard configuration.

    Returns:
        Configured GoogleSSO instance.
    """
    return GoogleSSO(
        client_id=config.auth.google.client_id,
        client_secret=config.auth.google.client_secret,
        redirect_uri=f"{config.server.base_url}/auth/callback",
        allow_insecure_http=config.server.debug,
    )


async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> Optional[User]:
    """Get current user from session.

    Args:
        request: FastAPI request.
        db: Database session.

    Returns:
        User instance if authenticated, None otherwise.
    """
    user_id = request.session.get("user_id")
    if not user_id:
        return None

    result = await db.execute(select(User).where(User.id == user_id))
    return result.scalar_one_or_none()


async def require_auth(
    request: Request,
    user: Optional[User] = Depends(get_current_user),
) -> User:
    """Require authenticated user, raise 401 if not.

    Args:
        request: FastAPI request.
        user: Current user (from dependency).

    Returns:
        Authenticated user.

    Raises:
        HTTPException: If user is not authenticated.
    """
    if not user:
        # For API requests, return 401
        accept = request.headers.get("accept", "")
        if "application/json" in accept:
            raise HTTPException(status_code=401, detail="Not authenticated")
        # For browser requests, redirect to login
        raise HTTPException(
            status_code=302,
            headers={"Location": "/login"},
        )
    return user


async def login_redirect(request: Request) -> RedirectResponse:
    """Initiate Google OAuth login flow.

    Args:
        request: FastAPI request.

    Returns:
        Redirect to Google OAuth consent page.
    """
    config: DashboardConfig = request.app.state.config
    google_sso = create_google_sso(config)

    with google_sso:
        return await google_sso.get_login_redirect()


async def auth_callback(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    """Handle Google OAuth callback.

    Args:
        request: FastAPI request.
        db: Database session.

    Returns:
        Redirect to dashboard on success.

    Raises:
        HTTPException: If domain is not allowed or auth fails.
    """
    config: DashboardConfig = request.app.state.config
    google_sso = create_google_sso(config)

    with google_sso:
        user_info = await google_sso.verify_and_process(request)

    if not user_info or not user_info.email:
        raise HTTPException(status_code=400, detail="Failed to get user info from Google")

    # Check domain restriction
    email_domain = user_info.email.split("@")[1]
    if email_domain not in config.auth.google.allowed_domains:
        raise HTTPException(
            status_code=403,
            detail=f"Domain '{email_domain}' is not allowed. "
            f"Allowed domains: {', '.join(config.auth.google.allowed_domains)}",
        )

    # Get or create user
    result = await db.execute(select(User).where(User.email == user_info.email))
    user = result.scalar_one_or_none()

    if not user:
        user = User(
            email=user_info.email,
            name=user_info.display_name,
            picture_url=user_info.picture,
        )
        db.add(user)
        await db.flush()

    user.last_login_at = datetime.utcnow()
    await db.commit()

    # Set session
    request.session["user_id"] = user.id

    return RedirectResponse(url="/", status_code=302)


async def logout(request: Request) -> RedirectResponse:
    """Log out user by clearing session.

    Args:
        request: FastAPI request.

    Returns:
        Redirect to login page.
    """
    request.session.clear()
    return RedirectResponse(url="/login", status_code=302)
