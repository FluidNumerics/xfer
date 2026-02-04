"""Admin configuration loading and validation."""

from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class GoogleAuthConfig(BaseModel):
    """Google OAuth2 configuration."""

    client_id: str
    client_secret: str
    allowed_domains: List[str]


class SessionConfig(BaseModel):
    """Session configuration."""

    secret_key: str
    cookie_name: str = "xfer_session"
    max_age_seconds: int = 86400


class AuthConfig(BaseModel):
    """Authentication configuration."""

    google: GoogleAuthConfig
    session: SessionConfig


class DatabaseConfig(BaseModel):
    """Database configuration."""

    url: str = "sqlite+aiosqlite:///./xfer_dashboard.db"


class SlurmConfig(BaseModel):
    """Slurm job configuration (admin-controlled)."""

    rclone_image: str = "rclone/rclone:latest"
    rclone_config: Path
    num_shards: int = 256
    array_concurrency: int = 64
    partitions: List[str] = ["standard"]
    cpus_per_task: int = 4
    mem: str = "8G"
    time_limit: str = "24:00:00"
    max_attempts: int = 5
    rclone_flags: str = "--transfers 32 --checkers 64 --fast-list --retries 10 --low-level-retries 20"
    pyxis_extra: str = ""

    @field_validator("rclone_config", mode="before")
    @classmethod
    def validate_rclone_config(cls, v):
        return Path(v) if isinstance(v, str) else v


class PathsConfig(BaseModel):
    """Path restrictions configuration."""

    run_base_dir: Path
    allowed_prefixes: List[str]

    @field_validator("run_base_dir", mode="before")
    @classmethod
    def validate_run_base_dir(cls, v):
        return Path(v) if isinstance(v, str) else v


class ServerConfig(BaseModel):
    """Server configuration."""

    host: str = "0.0.0.0"
    port: int = 8000
    base_url: str
    debug: bool = False


class DashboardConfig(BaseModel):
    """Root dashboard configuration."""

    schema_version: str = Field(default="xfer.dashboard.config.v1", alias="schema")
    auth: AuthConfig
    database: DatabaseConfig
    slurm: SlurmConfig
    paths: PathsConfig
    server: ServerConfig

    class Config:
        populate_by_name = True


def find_config_file() -> Optional[Path]:
    """Find the dashboard configuration file.

    Searches in order:
    1. XFER_DASHBOARD_CONFIG environment variable
    2. /etc/xfer/dashboard.yaml
    3. ~/.config/xfer/dashboard.yaml
    4. ./dashboard.yaml (current directory)
    """
    import os

    # Check environment variable
    env_path = os.environ.get("XFER_DASHBOARD_CONFIG")
    if env_path:
        path = Path(env_path)
        if path.exists():
            return path

    # Check standard locations
    candidates = [
        Path("/etc/xfer/dashboard.yaml"),
        Path.home() / ".config" / "xfer" / "dashboard.yaml",
        Path("dashboard.yaml"),
    ]

    for path in candidates:
        if path.exists():
            return path

    return None


def load_config(path: Optional[Path] = None) -> DashboardConfig:
    """Load dashboard configuration from YAML file.

    Args:
        path: Path to config file. If None, searches standard locations.

    Returns:
        Validated DashboardConfig instance.

    Raises:
        FileNotFoundError: If no config file is found.
        ValueError: If config validation fails.
    """
    if path is None:
        path = find_config_file()

    if path is None:
        raise FileNotFoundError(
            "No dashboard configuration file found. "
            "Create one at /etc/xfer/dashboard.yaml, ~/.config/xfer/dashboard.yaml, "
            "or set XFER_DASHBOARD_CONFIG environment variable."
        )

    with open(path) as f:
        data = yaml.safe_load(f)

    return DashboardConfig.model_validate(data)
