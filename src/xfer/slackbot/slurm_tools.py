"""
Slurm interaction tools for the xfer Slack bot.

These functions are called by Claude via tool use to execute actual operations.
"""

from __future__ import annotations

import json
import re
import shlex
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from .config import BotConfig, slack_comment

from ..est import (
    compute_file_size_stats,
    compute_totals_and_sizes,
    extract_size_bytes,
    format_histogram_data,
    format_histogram_text,
    human_bytes,
    suggest_rclone_flags_from_sizes,
)


@dataclass
class JobInfo:
    """Information about a Slurm job."""

    job_id: str
    array_job_id: Optional[str]
    state: str
    name: str
    comment: str
    work_dir: Optional[str]  # From --chdir, lets us find run directory
    submit_time: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    partition: str
    # Progress info (from state files)
    total_tasks: Optional[int] = None
    completed_tasks: Optional[int] = None
    failed_tasks: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "array_job_id": self.array_job_id,
            "state": self.state,
            "name": self.name,
            "comment": self.comment,
            "work_dir": self.work_dir,
            "submit_time": self.submit_time,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "partition": self.partition,
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
        }


@dataclass
class TransferRequest:
    """Tracks a multi-phase transfer request."""

    source: str
    dest: str
    run_dir: Path
    channel_id: str
    thread_ts: str
    # Job IDs for each phase
    manifest_job_id: Optional[str] = None
    transfer_job_id: Optional[str] = None
    # Config overrides
    num_shards: Optional[int] = None
    time_limit: Optional[str] = None
    job_name: Optional[str] = None


@dataclass
class TransferResult:
    """Result of submitting a transfer job."""

    success: bool
    job_id: Optional[str]
    run_dir: Optional[Path]
    message: str
    error: Optional[str] = None


def run_cmd(
    cmd: list[str],
    *,
    check: bool = True,
    capture: bool = True,
) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        capture_output=capture,
    )


def get_allowed_backends(config: BotConfig) -> list[str]:
    """
    Get list of allowed rclone backends.

    If allowed_backends_file is set, reads from that file.
    Otherwise, parses rclone.conf for configured remotes.
    """
    # If explicit allowed list exists, use it
    if config.allowed_backends_file and config.allowed_backends_file.exists():
        content = config.allowed_backends_file.read_text()
        if config.allowed_backends_file.suffix in (".yaml", ".yml"):
            import yaml

            data = yaml.safe_load(content)
            return data.get("allowed_backends", [])
        else:
            data = json.loads(content)
            return data.get("allowed_backends", [])

    # Otherwise, parse rclone.conf for section headers
    backends = []
    if config.rclone.config_path.exists():
        content = config.rclone.config_path.read_text()
        # rclone.conf uses INI format: [remote_name]
        for match in re.finditer(r"^\[([^\]]+)\]", content, re.MULTILINE):
            backends.append(match.group(1))

    return backends


def validate_backend(backend: str, config: BotConfig) -> tuple[bool, str]:
    """
    Validate that a backend is allowed.

    Returns (is_valid, message).
    """
    # Extract remote name from "remote:path" format
    if ":" in backend:
        remote_name = backend.split(":")[0]
    else:
        remote_name = backend

    allowed = get_allowed_backends(config)
    if not allowed:
        return False, "No backends configured. Please contact support."

    if remote_name in allowed:
        return True, f"Backend '{remote_name}' is allowed."
    else:
        return False, (
            f"Backend '{remote_name}' is not in the allowed list. "
            f"Allowed backends: {', '.join(allowed)}. "
            "Contact support if you need access to additional backends."
        )


def _write_prepare_script(
    run_dir: Path,
    source: str,
    dest: str,
    config: BotConfig,
    num_shards: int,
    array_concurrency: int,
    time_limit: str,
    job_name: str,
    comment: str,
    rclone_flags: Optional[str] = None,
) -> Path:
    """
    Write a batch script that prepares the transfer (manifest + shard + render).

    This script runs as a single Slurm job. When complete, it submits the
    transfer array job.
    """
    script_path = run_dir / "prepare.sh"

    # Build sbatch extras for the transfer job
    sbatch_extras_lines = [f"#SBATCH --comment={shlex.quote(comment)}"]
    sbatch_extras_lines.append(f"#SBATCH --chdir={run_dir}")
    if config.slurm.qos:
        sbatch_extras_lines.append(f"#SBATCH --qos={config.slurm.qos}")
    sbatch_extras = "\\n".join(sbatch_extras_lines)

    # User flags to append after analysis-suggested flags
    user_rclone_flags = rclone_flags or ""

    # Get xfer install directory for uv
    xfer_dir = config.xfer_install_dir or Path(__file__).parent.parent.parent.parent.resolve()

    script_content = f"""#!/usr/bin/env bash
#SBATCH --job-name={job_name}-prepare
#SBATCH --output={run_dir}/prepare-%j.out
#SBATCH --error={run_dir}/prepare-%j.err
#SBATCH --time=04:00:00
#SBATCH --partition={config.slurm.partition}
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --comment={shlex.quote(comment)}
#SBATCH --chdir={run_dir}
{f"#SBATCH --qos={config.slurm.qos}" if config.slurm.qos else ""}

set -euo pipefail

# Unset conflicting Slurm memory variables to allow srun to work
# (cluster DefMemPerCPU conflicts with job --mem setting)
unset SLURM_MEM_PER_CPU SLURM_MEM_PER_GPU SLURM_MEM_PER_NODE

# Setup uv environment
XFER_DIR="{xfer_dir}"
echo "=== Setting up uv environment at $(date -Is) ==="
echo "XFER_DIR: $XFER_DIR"
cd "$XFER_DIR"
uv sync
echo "=== uv sync complete ==="

echo "=== Starting manifest build at $(date -Is) ==="
echo "Source: {source}"
echo "Dest: {dest}"
echo "Run dir: {run_dir}"

# Phase 1: Build manifest
uv run xfer manifest build \\
    --source {shlex.quote(source)} \\
    --dest {shlex.quote(dest)} \\
    --out {run_dir}/manifest.jsonl \\
    --rclone-image {shlex.quote(config.rclone.image)} \\
    --rclone-config {shlex.quote(str(config.rclone.config_path))}

echo "=== Manifest build complete at $(date -Is) ==="

# Phase 2: Analyze manifest to determine optimal rclone flags
echo "=== Analyzing file size distribution ==="
uv run xfer manifest analyze \\
    --in {run_dir}/manifest.jsonl \\
    --out {run_dir}/analysis.json

# Extract suggested flags from analysis
SUGGESTED_FLAGS=$(python3 -c "import json; print(json.load(open('{run_dir}/analysis.json'))['suggested_flags'])")
echo "Profile-based flags: $SUGGESTED_FLAGS"

# Append any user-specified flags
USER_FLAGS={shlex.quote(user_rclone_flags)}
if [ -n "$USER_FLAGS" ]; then
    RCLONE_FLAGS="$SUGGESTED_FLAGS $USER_FLAGS"
    echo "User flags appended: $USER_FLAGS"
else
    RCLONE_FLAGS="$SUGGESTED_FLAGS"
fi
echo "Final rclone flags: $RCLONE_FLAGS"

echo "=== Analysis complete at $(date -Is) ==="

# Phase 3: Shard manifest
uv run xfer manifest shard \\
    --in {run_dir}/manifest.jsonl \\
    --outdir {run_dir}/shards \\
    --num-shards {num_shards}

echo "=== Sharding complete at $(date -Is) ==="

# Phase 4: Render Slurm scripts
uv run xfer slurm render \\
    --run-dir {run_dir} \\
    --num-shards {num_shards} \\
    --array-concurrency {array_concurrency} \\
    --job-name {shlex.quote(job_name)} \\
    --time-limit {time_limit} \\
    --partition {config.slurm.partition} \\
    --cpus-per-task {config.slurm.cpus_per_task} \\
    --mem {config.slurm.mem} \\
    --rclone-image {shlex.quote(config.rclone.image)} \\
    --rclone-config {shlex.quote(str(config.rclone.config_path))} \\
    --rclone-flags "$RCLONE_FLAGS" \\
    --max-attempts {config.slurm.max_attempts} \\
    --sbatch-extras {shlex.quote(sbatch_extras)}

echo "=== Render complete at $(date -Is) ==="

# Phase 5: Submit transfer array job
uv run xfer slurm submit --run-dir {run_dir}

echo "=== Transfer job submitted at $(date -Is) ==="
"""

    script_path.write_text(script_content)
    script_path.chmod(0o755)
    return script_path


def submit_transfer(
    source: str,
    dest: str,
    config: BotConfig,
    channel_id: str,
    thread_ts: str,
    *,
    num_shards: Optional[int] = None,
    array_concurrency: Optional[int] = None,
    time_limit: Optional[str] = None,
    job_name: Optional[str] = None,
    rclone_flags: Optional[str] = None,
) -> TransferResult:
    """
    Submit a data transfer job via xfer.

    This submits a two-phase job:
    1. Prepare job: builds manifest, shards, renders scripts, submits transfer job
    2. Transfer job: the actual array job doing the data movement

    The prepare job runs first and submits the transfer job when complete.
    """
    # Validate backends
    for backend, label in [(source, "source"), (dest, "destination")]:
        valid, msg = validate_backend(backend, config)
        if not valid:
            return TransferResult(
                success=False,
                job_id=None,
                run_dir=None,
                message=f"Invalid {label}: {msg}",
            )

    # Generate run directory
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_name = f"slack_{channel_id}_{timestamp}"
    run_dir = config.runs_base_dir / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    # Resolve defaults
    num_shards = num_shards or config.slurm.num_shards
    array_concurrency = array_concurrency or config.slurm.array_concurrency
    time_limit = time_limit or config.slurm.time_limit
    job_name = job_name or "xfer-slack"

    # Build comment for tracking
    comment = slack_comment(channel_id, thread_ts)

    # Write the prepare script
    prepare_script = _write_prepare_script(
        run_dir=run_dir,
        source=source,
        dest=dest,
        config=config,
        num_shards=num_shards,
        array_concurrency=array_concurrency,
        time_limit=time_limit,
        job_name=job_name,
        comment=comment,
        rclone_flags=rclone_flags,
    )

    # Submit the prepare job
    # Use --export=NONE to prevent inheriting SLURM_* env vars from the bot's job
    try:
        result = run_cmd(["sbatch", "--export=NONE", str(prepare_script)], check=True)

        # Parse job ID from output
        job_id = None
        for line in result.stdout.splitlines():
            if "Submitted batch job" in line:
                parts = line.split()
                job_id = parts[-1]
                break

        # Save request metadata for later reference
        request_meta = {
            "source": source,
            "dest": dest,
            "channel_id": channel_id,
            "thread_ts": thread_ts,
            "prepare_job_id": job_id,
            "num_shards": num_shards,
            "submitted_at": datetime.utcnow().isoformat() + "Z",
        }
        (run_dir / "request.json").write_text(json.dumps(request_meta, indent=2))

        return TransferResult(
            success=True,
            job_id=job_id,
            run_dir=run_dir,
            message=(
                f"Transfer preparation job submitted. Job ID: {job_id}\n"
                f"Run directory: {run_dir}\n"
                "The manifest will be built and the transfer will start automatically."
            ),
        )

    except subprocess.CalledProcessError as e:
        return TransferResult(
            success=False,
            job_id=None,
            run_dir=run_dir,
            message="Failed to submit transfer preparation job.",
            error=e.stderr or str(e),
        )


def _parse_job_from_sacct_json(job_data: dict) -> Optional[JobInfo]:
    """Parse a single job entry from sacct --json output."""
    job_id = str(job_data.get("job_id", ""))
    if not job_id:
        return None

    # Handle array jobs
    array_job_id = None
    array_task_id = job_data.get("array", {}).get("task_id", {}).get("number")
    if array_task_id is not None:
        array_job_id = job_id
        job_id = f"{job_id}_{array_task_id}"

    # Extract state - sacct JSON uses state.current
    state_info = job_data.get("state", {})
    state = state_info.get("current", ["UNKNOWN"])
    if isinstance(state, list):
        state = state[0] if state else "UNKNOWN"

    # Extract times
    time_info = job_data.get("time", {})
    submit_time = time_info.get("submission")
    start_time = time_info.get("start")
    end_time = time_info.get("end")

    # Work directory from --chdir
    work_dir = job_data.get("working_directory")

    return JobInfo(
        job_id=job_id,
        array_job_id=array_job_id,
        state=state,
        name=job_data.get("name", ""),
        comment=job_data.get("comment", {}).get("job", ""),
        work_dir=work_dir,
        submit_time=str(submit_time) if submit_time else None,
        start_time=str(start_time) if start_time else None,
        end_time=str(end_time) if end_time else None,
        partition=job_data.get("partition", ""),
    )


def get_jobs_by_user(user: Optional[str] = None) -> list[JobInfo]:
    """
    Get all jobs for a user using sacct --json.

    If user is None, queries current user's jobs.
    """
    cmd = ["sacct", "--json", "-X"]  # -X = no job steps
    if user:
        cmd.extend(["-u", user])

    try:
        result = run_cmd(cmd, check=True)
    except subprocess.CalledProcessError:
        return []

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return []

    jobs = []
    for job_data in data.get("jobs", []):
        job_info = _parse_job_from_sacct_json(job_data)
        if job_info:
            jobs.append(job_info)

    return jobs


def get_jobs_by_thread(channel_id: str, thread_ts: str) -> list[JobInfo]:
    """
    Find all Slurm jobs associated with a Slack thread.

    Uses sacct --json and filters by comment.
    """
    comment_pattern = slack_comment(channel_id, thread_ts)
    all_jobs = get_jobs_by_user()

    return [j for j in all_jobs if comment_pattern in (j.comment or "")]


def get_job_status(job_id: str) -> Optional[JobInfo]:
    """Get detailed status for a specific job ID."""
    cmd = ["sacct", "--json", "-j", job_id, "-X"]

    try:
        result = run_cmd(cmd, check=True)
    except subprocess.CalledProcessError:
        return None

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None

    for job_data in data.get("jobs", []):
        job_info = _parse_job_from_sacct_json(job_data)
        if job_info:
            return job_info

    return None


def get_transfer_progress(run_dir: Path) -> dict:
    """
    Get detailed progress for a transfer by examining state files.

    Returns dict with total_tasks, completed, failed, pending counts,
    plus metadata about the transfer.
    """
    state_dir = run_dir / "state"
    shards_meta = run_dir / "shards" / "shards.meta.json"
    request_meta = run_dir / "request.json"
    manifest_file = run_dir / "manifest.jsonl"

    result = {
        "run_dir": str(run_dir),
        "phase": "unknown",
        "total_tasks": 0,
        "completed": 0,
        "failed": 0,
        "pending": 0,
        "in_progress": 0,
        "total_bytes": 0,
        "total_files": 0,
    }

    # Load request metadata if available
    if request_meta.exists():
        try:
            meta = json.loads(request_meta.read_text())
            result["source"] = meta.get("source")
            result["dest"] = meta.get("dest")
            result["prepare_job_id"] = meta.get("prepare_job_id")
        except json.JSONDecodeError:
            pass

    # Determine phase based on what files exist
    if not manifest_file.exists():
        result["phase"] = "building_manifest"
        return result

    if not shards_meta.exists():
        result["phase"] = "sharding"
        return result

    # Get total from shards metadata
    try:
        meta = json.loads(shards_meta.read_text())
        result["total_tasks"] = meta.get("num_shards", 0)
        result["total_bytes"] = meta.get("bytes_total", 0)
        result["total_files"] = meta.get("num_records", 0)
    except json.JSONDecodeError:
        pass

    if not state_dir.exists():
        result["phase"] = "waiting_to_start"
        result["pending"] = result["total_tasks"]
        return result

    result["phase"] = "transferring"

    # Count state files
    done_files = list(state_dir.glob("shard_*.done"))
    fail_files = list(state_dir.glob("shard_*.fail"))

    result["completed"] = len(done_files)
    result["failed"] = len(fail_files)

    # In progress = has attempt file but no done/fail
    attempt_files = list(state_dir.glob("shard_*.attempt"))
    done_ids = {f.stem.replace("shard_", "").replace(".done", "") for f in done_files}
    fail_ids = {f.stem.replace("shard_", "").replace(".fail", "") for f in fail_files}

    in_progress = 0
    for af in attempt_files:
        task_id = af.stem.replace("shard_", "").replace(".attempt", "")
        if task_id not in done_ids and task_id not in fail_ids:
            in_progress += 1

    result["in_progress"] = in_progress
    result["pending"] = (
        result["total_tasks"] - result["completed"] - result["failed"] - in_progress
    )

    # Check if complete
    if result["completed"] == result["total_tasks"]:
        result["phase"] = "complete"
    elif result["failed"] > 0 and result["pending"] == 0 and result["in_progress"] == 0:
        result["phase"] = "failed"

    return result


def get_transfer_progress_by_job(job_id: str) -> Optional[dict]:
    """
    Get transfer progress by looking up the job's working directory.
    """
    job = get_job_status(job_id)
    if not job or not job.work_dir:
        return None

    run_dir = Path(job.work_dir)
    if not run_dir.exists():
        return None

    progress = get_transfer_progress(run_dir)
    progress["job_state"] = job.state
    return progress


def cancel_job(job_id: str, channel_id: str, thread_ts: str) -> tuple[bool, str]:
    """
    Cancel a Slurm job.

    Validates that the job belongs to the requesting thread via comment.
    """
    # First, verify the job belongs to this thread
    job_info = get_job_status(job_id)
    if not job_info:
        return False, f"Job {job_id} not found."

    expected_comment = slack_comment(channel_id, thread_ts)
    if expected_comment not in job_info.comment:
        return False, f"Job {job_id} does not belong to this thread. Cannot cancel."

    # Cancel the job
    try:
        run_cmd(["scancel", job_id], check=True)
        return True, f"Job {job_id} has been cancelled."
    except subprocess.CalledProcessError as e:
        return False, f"Failed to cancel job {job_id}: {e.stderr or str(e)}"


@dataclass
class SourceStats:
    """Statistics about a source path."""

    source: str
    total_files: int
    total_bytes: int
    total_bytes_human: str
    file_size_stats: dict
    suggested_flags: dict
    histogram: list
    histogram_text: str
    error: Optional[str] = None


def get_source_stats(source: str, config: BotConfig) -> SourceStats:
    """
    Scan a source path and return file statistics without starting a transfer.

    Runs rclone lsjson via container and computes statistics.
    """
    # Validate backend first
    valid, msg = validate_backend(source, config)
    if not valid:
        return SourceStats(
            source=source,
            total_files=0,
            total_bytes=0,
            total_bytes_human="0 B",
            file_size_stats={},
            suggested_flags={},
            histogram=[],
            histogram_text="",
            error=msg,
        )

    # Build rclone lsjson command
    rclone_cmd = [
        "rclone",
        "lsjson",
        source,
        "--recursive",
        "--fast-list",
        "--files-only",
        "--config",
        config.rclone.container_conf_path,
    ]

    # Build srun command with container
    mounts = f"{config.rclone.config_path}:{config.rclone.container_conf_path}:ro"

    srun_cmd = [
        "srun",
        "-n",
        "1",
        "-c",
        "8",
        "--container-image",
        config.rclone.image,
        "--container-mounts",
        mounts,
        "--no-container-remap-root",
    ] + rclone_cmd

    try:
        result = run_cmd(srun_cmd, capture=True, check=True)
        lsjson_output = result.stdout
    except subprocess.CalledProcessError as e:
        return SourceStats(
            source=source,
            total_files=0,
            total_bytes=0,
            total_bytes_human="0 B",
            file_size_stats={},
            suggested_flags={},
            histogram=[],
            histogram_text="",
            error=f"Failed to list source: {e.stderr or str(e)}",
        )

    # Parse JSON output
    try:
        items = json.loads(lsjson_output)
        if not isinstance(items, list):
            raise ValueError("Expected JSON array from rclone lsjson")
    except (json.JSONDecodeError, ValueError) as e:
        return SourceStats(
            source=source,
            total_files=0,
            total_bytes=0,
            total_bytes_human="0 B",
            file_size_stats={},
            suggested_flags={},
            histogram=[],
            histogram_text="",
            error=f"Failed to parse rclone output: {e}",
        )

    # Extract sizes
    sizes = []
    for item in items:
        size = extract_size_bytes(item)
        if size is not None:
            sizes.append(size)

    if not sizes:
        return SourceStats(
            source=source,
            total_files=0,
            total_bytes=0,
            total_bytes_human="0 B",
            file_size_stats={},
            suggested_flags={},
            histogram=[],
            histogram_text="No files found",
            error=None,
        )

    # Compute statistics
    stats = compute_file_size_stats(sizes)
    flags_suggestion = suggest_rclone_flags_from_sizes(sizes)
    histogram = format_histogram_data(sizes)
    histogram_text = format_histogram_text(sizes)

    return SourceStats(
        source=source,
        total_files=stats.total_files,
        total_bytes=stats.total_bytes,
        total_bytes_human=human_bytes(stats.total_bytes),
        file_size_stats={
            "min_size": stats.min_size,
            "min_size_human": human_bytes(stats.min_size),
            "max_size": stats.max_size,
            "max_size_human": human_bytes(stats.max_size),
            "median_size": stats.median_size,
            "median_size_human": human_bytes(stats.median_size),
            "mean_size": int(stats.mean_size),
            "mean_size_human": human_bytes(int(stats.mean_size)),
            "p10_size": stats.p10_size,
            "p90_size": stats.p90_size,
            "small_files_pct": round(stats.small_files_pct, 1),
            "medium_files_pct": round(stats.medium_files_pct, 1),
            "large_files_pct": round(stats.large_files_pct, 1),
        },
        suggested_flags={
            "profile": flags_suggestion.profile,
            "flags": flags_suggestion.flags,
            "explanation": flags_suggestion.explanation,
        },
        histogram=histogram,
        histogram_text=histogram_text,
        error=None,
    )


@dataclass
class PathCheckResult:
    """Result of checking if a path exists."""

    path: str
    exists: bool
    error: Optional[str] = None
    details: Optional[str] = None


def check_path_exists(path: str, config: BotConfig) -> PathCheckResult:
    """
    Check if a bucket/path exists at a remote endpoint.

    Uses rclone lsf with --max-depth 0 to check if the path is accessible.
    """
    # Validate backend first
    valid, msg = validate_backend(path, config)
    if not valid:
        return PathCheckResult(
            path=path,
            exists=False,
            error=msg,
            details="Backend not in allowed list",
        )

    # Build rclone lsf command to check if path exists
    # Using lsf with --max-depth 0 and --dirs-only is a quick way to check
    rclone_cmd = [
        "rclone",
        "lsf",
        path,
        "--max-depth",
        "0",
        "--config",
        config.rclone.container_conf_path,
    ]

    # Build srun command with container
    mounts = f"{config.rclone.config_path}:{config.rclone.container_conf_path}:ro"

    srun_cmd = [
        "srun",
        "-n",
        "1",
        "-c",
        "2",
        "--container-image",
        config.rclone.image,
        "--container-mounts",
        mounts,
        "--no-container-remap-root",
    ] + rclone_cmd

    try:
        result = run_cmd(srun_cmd, capture=True, check=True)
        # If command succeeded, path exists
        return PathCheckResult(
            path=path,
            exists=True,
            details="Path is accessible",
        )
    except subprocess.CalledProcessError as e:
        error_output = e.stderr or str(e)

        # Check for common error patterns
        if "NoSuchBucket" in error_output or "bucket does not exist" in error_output.lower():
            return PathCheckResult(
                path=path,
                exists=False,
                error="Bucket does not exist",
                details=error_output,
            )
        elif "AccessDenied" in error_output or "access denied" in error_output.lower():
            return PathCheckResult(
                path=path,
                exists=False,
                error="Access denied - credentials may not have permission",
                details=error_output,
            )
        elif "NoSuchKey" in error_output or "not found" in error_output.lower():
            return PathCheckResult(
                path=path,
                exists=False,
                error="Path does not exist within the bucket",
                details=error_output,
            )
        else:
            return PathCheckResult(
                path=path,
                exists=False,
                error=f"Failed to access path: {error_output[:200]}",
                details=error_output,
            )
