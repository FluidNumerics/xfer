"""rclone configuration parsing and remote discovery."""

import configparser
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class RcloneRemote:
    """Represents an rclone remote configuration."""

    name: str
    type: str
    endpoint: Optional[str] = None
    provider: Optional[str] = None
    region: Optional[str] = None

    @property
    def display_name(self) -> str:
        """Human-readable name for UI display."""
        if self.endpoint:
            return f"{self.name} ({self.endpoint})"
        if self.provider and self.provider != "Other":
            return f"{self.name} ({self.provider})"
        return self.name

    @property
    def prefix(self) -> str:
        """Get the rclone prefix for this remote (e.g., 's3-src:')."""
        return f"{self.name}:"

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "type": self.type,
            "endpoint": self.endpoint,
            "provider": self.provider,
            "region": self.region,
            "display_name": self.display_name,
            "prefix": self.prefix,
        }


def parse_rclone_config(config_path: Path) -> List[RcloneRemote]:
    """Parse rclone.conf INI file and extract remote configurations.

    Args:
        config_path: Path to rclone.conf file.

    Returns:
        List of RcloneRemote instances.

    Example rclone.conf format:
        [s3-src]
        type = s3
        provider = Other
        endpoint = https://s3.example.com
        access_key_id = ...
        secret_access_key = ...
    """
    if not config_path.exists():
        return []

    config = configparser.ConfigParser()
    config.read(config_path)

    remotes = []
    for section in config.sections():
        remote_type = config.get(section, "type", fallback="")
        remotes.append(
            RcloneRemote(
                name=section,
                type=remote_type,
                endpoint=config.get(section, "endpoint", fallback=None),
                provider=config.get(section, "provider", fallback=None),
                region=config.get(section, "region", fallback=None),
            )
        )

    return remotes


def get_allowed_remotes(
    config_path: Path,
    allowed_prefixes: List[str],
) -> List[RcloneRemote]:
    """Get remotes that match the admin-allowed prefixes.

    Args:
        config_path: Path to rclone.conf file.
        allowed_prefixes: List of allowed path prefixes from admin config.

    Returns:
        List of allowed RcloneRemote instances.
    """
    all_remotes = parse_rclone_config(config_path)
    allowed = []

    for remote in all_remotes:
        remote_prefix = remote.prefix
        for prefix in allowed_prefixes:
            # Match if the allowed prefix starts with the remote name
            # e.g., "s3-src:" matches allowed prefix "s3-src:"
            # e.g., "s3-src:" matches allowed prefix "s3-src:bucket/"
            if prefix.startswith(remote_prefix) or remote_prefix == prefix:
                allowed.append(remote)
                break

    return allowed


def get_allowed_local_prefixes(allowed_prefixes: List[str]) -> List[str]:
    """Extract local filesystem prefixes from allowed prefixes.

    Args:
        allowed_prefixes: List of allowed path prefixes from admin config.

    Returns:
        List of local filesystem path prefixes (those starting with '/').
    """
    return [p for p in allowed_prefixes if p.startswith("/")]


def validate_path(path: str, allowed_prefixes: List[str]) -> bool:
    """Validate that a path matches at least one allowed prefix.

    Args:
        path: Path to validate.
        allowed_prefixes: List of allowed path prefixes.

    Returns:
        True if path is allowed, False otherwise.
    """
    for prefix in allowed_prefixes:
        if path.startswith(prefix):
            return True
    return False


def is_remote_path(path: str) -> bool:
    """Check if a path is an rclone remote path (contains ':').

    Args:
        path: Path to check.

    Returns:
        True if path is a remote path, False otherwise.
    """
    # Remote paths have format "remote:path" but we need to exclude
    # Windows absolute paths like "C:\path" (though unlikely on Linux)
    if ":" not in path:
        return False
    # Check if it looks like a remote (letters/digits/hyphens before colon)
    colon_idx = path.index(":")
    if colon_idx == 0:
        return False
    remote_name = path[:colon_idx]
    return all(c.isalnum() or c in "-_" for c in remote_name)


def split_remote_path(path: str) -> tuple[Optional[str], str]:
    """Split a path into remote name and path components.

    Args:
        path: Full path (e.g., "s3-src:bucket/prefix" or "/scratch/data").

    Returns:
        Tuple of (remote_name, path). remote_name is None for local paths.
    """
    if not is_remote_path(path):
        return None, path

    colon_idx = path.index(":")
    remote_name = path[:colon_idx]
    remaining_path = path[colon_idx + 1 :]
    return remote_name, remaining_path
