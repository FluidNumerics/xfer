"""FastAPI application factory and entry point."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from . import auth
from .config import DashboardConfig, load_config
from .db.models import User
from .db.session import close_db, init_db

# Template directory
TEMPLATE_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"


def create_app(config: Optional[DashboardConfig] = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        config: Dashboard configuration. If None, loads from file.

    Returns:
        Configured FastAPI application.
    """
    if config is None:
        config = load_config()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Application lifespan handler."""
        # Startup
        await init_db(config.database.url)

        # Start background tasks
        poll_task = asyncio.create_task(progress_polling_task(app))

        yield

        # Shutdown
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass
        await close_db()

    app = FastAPI(
        title="xfer Dashboard",
        description="Web interface for managing xfer data transfer jobs",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Store config in app state
    app.state.config = config

    # Add session middleware
    app.add_middleware(
        SessionMiddleware,
        secret_key=config.auth.session.secret_key,
        session_cookie=config.auth.session.cookie_name,
        max_age=config.auth.session.max_age_seconds,
        same_site="lax",
        https_only=not config.server.debug,
    )

    # Mount static files
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    # Set up templates
    templates = Jinja2Templates(directory=TEMPLATE_DIR)
    app.state.templates = templates

    # Register routes
    register_routes(app, templates)

    # Include API routers
    from .api.routes import api_router
    app.include_router(api_router)

    return app


def register_routes(app: FastAPI, templates: Jinja2Templates) -> None:
    """Register all routes on the application.

    Args:
        app: FastAPI application.
        templates: Jinja2 templates instance.
    """
    # Auth routes
    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request):
        """Show login page."""
        return templates.TemplateResponse(
            "login.html",
            {"request": request},
        )

    @app.get("/auth/login")
    async def login_redirect_route(request: Request):
        """Redirect to Google OAuth."""
        return await auth.login_redirect(request)

    @app.get("/auth/callback")
    async def auth_callback_route(
        request: Request,
        db=Depends(auth.get_db),
    ):
        """Handle Google OAuth callback."""
        return await auth.auth_callback(request, db)

    @app.get("/auth/logout")
    async def logout_route(request: Request):
        """Log out user."""
        return await auth.logout(request)

    # Dashboard routes
    @app.get("/", response_class=HTMLResponse)
    async def dashboard(
        request: Request,
        user: User = Depends(auth.require_auth),
    ):
        """Show main dashboard."""
        return templates.TemplateResponse(
            "dashboard.html",
            {"request": request, "user": user},
        )

    # Health check
    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {"status": "ok"}

    # API info
    @app.get("/api")
    async def api_info():
        """API information endpoint."""
        return {
            "name": "xfer Dashboard API",
            "version": "0.1.0",
            "docs": "/docs",
        }


async def progress_polling_task(app: FastAPI) -> None:
    """Background task to poll job progress from state directories.

    Args:
        app: FastAPI application.
    """
    from .db.session import get_db_session
    from .services.state_service import update_all_active_jobs

    while True:
        try:
            async with get_db_session() as db:
                await update_all_active_jobs(db)
        except Exception as e:
            # Log error but keep polling
            print(f"Error in progress polling: {e}")

        await asyncio.sleep(30)


def run() -> None:
    """Entry point for running the dashboard server."""
    import uvicorn

    config = load_config()
    app = create_app(config)

    uvicorn.run(
        app,
        host=config.server.host,
        port=config.server.port,
        log_level="info" if not config.server.debug else "debug",
    )


if __name__ == "__main__":
    run()
