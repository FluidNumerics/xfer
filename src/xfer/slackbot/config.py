"""
Configuration and defaults for the xfer Slack bot.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class SlurmDefaults:
    """Default Slurm job parameters."""

    partition: str = "transfer"
    time_limit: str = "24:00:00"
    cpus_per_task: int = 4
    mem: str = "8G"
    array_concurrency: int = 64
    num_shards: int = 256
    max_attempts: int = 5
    qos: Optional[str] = None  # Set via cluster config if needed


@dataclass
class RcloneDefaults:
    """Default rclone parameters."""

    image: str = "rclone/rclone:latest"
    config_path: Path = field(
        default_factory=lambda: Path.home() / ".config/rclone/rclone.conf"
    )
    container_conf_path: str = "/etc/rclone/rclone.conf"
    flags: str = (
        "--transfers 32 --checkers 64 --fast-list --retries 10 --low-level-retries 20 "
        "--stats 600s --progress"
    )


@dataclass
class BotConfig:
    """Configuration for the xfer Slack bot."""

    # Slack
    slack_bot_token: str = field(
        default_factory=lambda: os.environ.get("SLACK_BOT_TOKEN", "")
    )
    slack_app_token: str = field(
        default_factory=lambda: os.environ.get("SLACK_APP_TOKEN", "")
    )
    allowed_channels: list[str] = field(default_factory=list)  # Empty = all channels
    support_channel: Optional[str] = field(
        default_factory=lambda: os.environ.get("XFER_SUPPORT_CHANNEL")
    )  # Channel ID for backend requests, alerts, etc.

    # Claude
    anthropic_api_key: str = field(
        default_factory=lambda: os.environ.get("ANTHROPIC_API_KEY", "")
    )
    claude_model: str = "claude-sonnet-4-20250514"

    # Paths
    runs_base_dir: Path = field(default_factory=lambda: Path.home() / "xfer-runs")
    allowed_backends_file: Optional[Path] = (
        None  # Path to YAML/JSON listing allowed backends
    )

    # Defaults
    slurm: SlurmDefaults = field(default_factory=SlurmDefaults)
    rclone: RcloneDefaults = field(default_factory=RcloneDefaults)

    @classmethod
    def from_env(cls) -> "BotConfig":
        """Load configuration from environment variables."""
        config = cls()

        # Override from env if set
        if channels := os.environ.get("XFER_ALLOWED_CHANNELS"):
            config.allowed_channels = [
                c.strip() for c in channels.split(",") if c.strip()
            ]

        if runs_dir := os.environ.get("XFER_RUNS_DIR"):
            config.runs_base_dir = Path(runs_dir)

        if backends_file := os.environ.get("XFER_ALLOWED_BACKENDS_FILE"):
            config.allowed_backends_file = Path(backends_file)

        if partition := os.environ.get("XFER_SLURM_PARTITION"):
            config.slurm.partition = partition

        if qos := os.environ.get("XFER_SLURM_QOS"):
            config.slurm.qos = qos

        if rclone_image := os.environ.get("XFER_RCLONE_IMAGE"):
            config.rclone.image = rclone_image

        if rclone_config := os.environ.get("XFER_RCLONE_CONFIG"):
            config.rclone.config_path = Path(rclone_config)

        return config


def slack_comment(channel_id: str, thread_ts: str) -> str:
    """Generate a Slurm job comment from Slack thread identifiers."""
    return f"slack:{channel_id}/{thread_ts}"


def parse_slack_comment(comment: str) -> tuple[str, str] | None:
    """Parse a Slurm job comment back to (channel_id, thread_ts)."""
    if not comment.startswith("slack:"):
        return None
    try:
        rest = comment[6:]  # Remove "slack:"
        channel_id, thread_ts = rest.split("/", 1)
        return channel_id, thread_ts
    except ValueError:
        return None
