"""Configuration models and loader for xfer-mcp."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

DEFAULT_CONFIG_PATH = Path.home() / ".config" / "xfer" / "mcp.yaml"


@dataclass
class ClusterConfig:
    """SSH-accessible HPC cluster running Slurm + pyxis/enroot."""

    host: str
    user: str
    description: str = ""
    # SSH connection
    ssh_key: Optional[str] = None
    ssh_extra_opts: list[str] = field(default_factory=list)
    # xfer binary and rclone config on the cluster
    xfer_path: str = "xfer"
    rclone_config: str = "~/.config/rclone/rclone.conf"
    rclone_image: str = "rclone/rclone:latest"
    rclone_conf_in_container: str = "/etc/rclone/rclone.conf"
    # Slurm defaults
    default_partition: str = "transfer"
    default_cpus_per_task: int = 4
    default_mem: str = "8G"
    default_time_limit: str = "24:00:00"
    default_num_shards: int = 256
    default_array_concurrency: int = 64
    default_max_attempts: int = 5


@dataclass
class EndpointConfig:
    """A data location reachable as a transfer source or destination."""

    type: str  # "s3" | "local"
    description: str = ""
    # S3 / object-storage fields
    rclone_remote: Optional[str] = None
    bucket: Optional[str] = None
    prefix: str = ""
    accessible_from: list[str] = field(default_factory=list)
    # Local-filesystem fields
    cluster: Optional[str] = None
    path: Optional[str] = None

    @property
    def rclone_path(self) -> str:
        """Return the rclone-style path for this endpoint."""
        if self.type == "s3":
            base = f"{self.rclone_remote}:{self.bucket}"
            if self.prefix:
                base += "/" + self.prefix.lstrip("/")
            return base
        if self.type == "local":
            return self.path or ""
        return ""


@dataclass
class XferMcpConfig:
    clusters: dict[str, ClusterConfig]
    endpoints: dict[str, EndpointConfig]


def load_config(path: str | Path | None = None) -> XferMcpConfig:
    """Load xfer-mcp configuration from a YAML file."""
    if yaml is None:
        raise ImportError(
            "pyyaml is required for xfer-mcp. Install with: uv sync --extra mcp"
        )
    if path is None:
        path = DEFAULT_CONFIG_PATH
    path = Path(path).expanduser()
    if not path.exists():
        raise FileNotFoundError(
            f"xfer-mcp config not found: {path}\n"
            f"Create one at {DEFAULT_CONFIG_PATH} "
            "(see examples/mcp.config.example.yaml in the xfer repo)"
        )
    with open(path) as fh:
        data = yaml.safe_load(fh) or {}

    cluster_fields = set(ClusterConfig.__dataclass_fields__.keys())
    endpoint_fields = set(EndpointConfig.__dataclass_fields__.keys())

    clusters: dict[str, ClusterConfig] = {}
    for name, cfg in (data.get("clusters") or {}).items():
        filtered = {k: v for k, v in cfg.items() if k in cluster_fields}
        clusters[name] = ClusterConfig(**filtered)

    endpoints: dict[str, EndpointConfig] = {}
    for name, cfg in (data.get("endpoints") or {}).items():
        filtered = {k: v for k, v in cfg.items() if k in endpoint_fields}
        endpoints[name] = EndpointConfig(**filtered)

    return XferMcpConfig(clusters=clusters, endpoints=endpoints)
