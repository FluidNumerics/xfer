"""xfer MCP server – gives Claude tools to orchestrate xfer data transfers."""
from __future__ import annotations

import json
import os
import shlex
import subprocess
from typing import Optional

from mcp.server.fastmcp import FastMCP

from .config import ClusterConfig, XferMcpConfig, load_config
from xfer.est import ascii_bar, human_bytes

# ---------------------------------------------------------------------------
# Config loading (once at import time)
# ---------------------------------------------------------------------------

_config_path = os.environ.get("XFER_MCP_CONFIG")
_config: XferMcpConfig | None = None
_config_error: str | None = None

try:
    _config = load_config(_config_path)
except Exception as exc:
    _config_error = str(exc)


def _cfg() -> XferMcpConfig:
    if _config is None:
        raise RuntimeError(
            f"xfer-mcp config could not be loaded: {_config_error}\n"
            "Set XFER_MCP_CONFIG env var or create ~/.config/xfer/mcp.yaml"
        )
    return _config


def _cluster(name: str) -> ClusterConfig:
    cfg = _cfg()
    if name not in cfg.clusters:
        available = ", ".join(cfg.clusters)
        raise ValueError(f"Unknown cluster {name!r}. Configured: {available}")
    return cfg.clusters[name]


def _resolve(name_or_path: str) -> str:
    """Return a rclone path: resolve endpoint names, pass-through everything else."""
    try:
        cfg = _cfg()
        if name_or_path in cfg.endpoints:
            return cfg.endpoints[name_or_path].rclone_path
    except Exception:
        pass
    return name_or_path


def _rq(s: str) -> str:
    """Shell-quote a value for a remote command; preserve leading ~ for tilde-expansion."""
    if s.startswith("~/") or s == "~":
        return s
    return shlex.quote(s)


def _ssh(cluster: ClusterConfig, command: str, timeout: int = 120) -> tuple[int, str, str]:
    """Run *command* on *cluster* over SSH. Returns (returncode, stdout, stderr)."""
    args = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
    if cluster.ssh_key:
        args += ["-i", os.path.expanduser(cluster.ssh_key)]
    for opt in cluster.ssh_extra_opts or []:
        args += ["-o", opt]
    args.append(f"{cluster.user}@{cluster.host}")
    args.append(command)
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        return -1, "", f"SSH command timed out after {timeout}s"
    except Exception as exc:
        return -1, "", str(exc)


def _result(rc: int, stdout: str, stderr: str) -> str:
    parts: list[str] = []
    if stdout.strip():
        parts.append(f"stdout:\n{stdout.strip()}")
    if stderr.strip():
        parts.append(f"stderr:\n{stderr.strip()}")
    parts.append(f"exit_code: {rc}")
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Manifest analysis helpers
# ---------------------------------------------------------------------------

_KiB = 1024
_MiB = _KiB * 1024
_GiB = _MiB * 1024

# Compact Python 3 script (no external deps, no single quotes) run on the
# cluster via `cat manifest.jsonl | python3 -c '<script>'`.
# Outputs a single JSON line with size stats and a pow2 histogram.
_ANALYZE_SCRIPT = """\
import sys, json, math
sizes = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        obj = json.loads(line)
    except Exception:
        continue
    if obj.get("IsDir") or obj.get("isdir"):
        continue
    sz = obj.get("size", obj.get("Size"))
    if sz is None:
        continue
    try:
        sizes.append(int(sz))
    except Exception:
        pass
sizes.sort()
n = len(sizes)
total = sum(sizes)
def pct(p):
    if not n:
        return 0
    idx = min(n - 1, max(0, int(math.ceil(n * p / 100.0)) - 1))
    return sizes[idx]
def pow2edges(lo, hi):
    lo = max(lo, 1)
    hi = max(hi, lo + 1)
    lp = 2 ** max(0, int(math.floor(math.log2(lo))))
    hp = 2 ** int(math.ceil(math.log2(hi)))
    if hp <= lp:
        hp = lp * 2
    edges, x = [], lp
    while x < hp:
        edges.append(x)
        x *= 2
    edges.append(hp)
    return edges
if sizes:
    edges = pow2edges(min(sizes), max(sizes))
    bins = [{"lo": edges[i], "hi": edges[i+1], "count": 0, "bytes": 0} for i in range(len(edges)-1)]
    for sz in sizes:
        for i, b in enumerate(bins):
            if i < len(bins) - 1:
                if b["lo"] <= sz < b["hi"]:
                    b["count"] += 1; b["bytes"] += sz; break
            else:
                b["count"] += 1; b["bytes"] += sz; break
    bins = [b for b in bins if b["count"] > 0]
else:
    bins = []
print(json.dumps({"objects": n, "bytes_total": total, "mean_bytes": int(total/n) if n else 0, "percentiles": {"p10": pct(10), "p25": pct(25), "p50": pct(50), "p75": pct(75), "p90": pct(90), "p95": pct(95), "p99": pct(99)}, "histogram": bins}))
"""


def _suggest_rclone_flags(stats: dict) -> tuple[list[tuple[str, str | None]], list[str]]:
    """Derive rclone flag recommendations from manifest size statistics.

    Returns ([(flag, value_or_None), ...], [reasoning lines]).
    """
    p50 = stats["percentiles"]["p50"]
    p90 = stats["percentiles"]["p90"]

    flags: list[tuple[str, str | None]] = []
    notes: list[str] = []

    # --- Base parallelism driven by median file size ---
    if p50 < _MiB:
        transfers, checkers, buf = 128, 256, "8M"
        notes.append(
            f"Median {human_bytes(p50)} < 1 MiB: maximise --transfers for small-file throughput"
        )
    elif p50 < 64 * _MiB:
        transfers, checkers, buf = 32, 64, "32M"
        notes.append(
            f"Median {human_bytes(p50)} (1–64 MiB): balanced parallelism"
        )
    elif p50 < 512 * _MiB:
        transfers, checkers, buf = 16, 32, "64M"
        notes.append(
            f"Median {human_bytes(p50)} (64–512 MiB): fewer concurrent transfers, larger buffer"
        )
    else:
        transfers, checkers, buf = 8, 16, "128M"
        notes.append(
            f"Median {human_bytes(p50)} > 512 MiB: minimal parallelism, large per-transfer buffer"
        )

    flags += [
        ("--transfers", str(transfers)),
        ("--checkers", str(checkers)),
        ("--buffer-size", buf),
    ]

    # --- Multipart / multi-thread tuning driven by P90 ---
    if p90 > _GiB:
        flags += [
            ("--multi-thread-streams", "8"),
            ("--s3-upload-concurrency", "16"),
            ("--s3-chunk-size", "128M"),
        ]
        notes.append(
            f"P90 {human_bytes(p90)} > 1 GiB: large S3 chunk size + high multi-thread concurrency"
        )
    elif p90 > 200 * _MiB:
        flags += [
            ("--multi-thread-streams", "4"),
            ("--s3-upload-concurrency", "8"),
            ("--s3-chunk-size", "64M"),
        ]
        notes.append(
            f"P90 {human_bytes(p90)} > 200 MiB: enable multi-thread streams + S3 multipart tuning"
        )
    else:
        notes.append(
            f"P90 {human_bytes(p90)} ≤ 200 MiB: multipart flags not needed"
        )

    # Always-on reliability flags
    flags += [
        ("--fast-list", None),
        ("--retries", "10"),
        ("--low-level-retries", "20"),
    ]

    return flags, notes


def _format_analysis(stats: dict, run_dir: str) -> str:
    """Format remote stats JSON into a human-readable histogram + flag suggestions."""
    n = stats["objects"]
    total = stats["bytes_total"]
    mean = stats["mean_bytes"]
    pct = stats["percentiles"]
    bins = stats["histogram"]

    lines = [
        f"## Manifest analysis: {run_dir}/manifest.jsonl",
        "",
        f"  Objects : {n:,}",
        f"  Total   : {human_bytes(total)}  ({total:,} bytes)",
        f"  Mean    : {human_bytes(mean)}",
        "",
        "### File size percentiles",
        f"  P10={human_bytes(pct['p10'])}  "
        f"P25={human_bytes(pct['p25'])}  "
        f"P50={human_bytes(pct['p50'])}  "
        f"P75={human_bytes(pct['p75'])}",
        f"  P90={human_bytes(pct['p90'])}  "
        f"P95={human_bytes(pct['p95'])}  "
        f"P99={human_bytes(pct['p99'])}",
        "",
        "### File size histogram (pow2 bins)",
    ]

    if bins:
        max_count = max(b["count"] for b in bins)
        lines.append("| Size range | Files | %files | Bytes | %bytes | Bar |")
        lines.append("|---|---:|---:|---:|---:|---|")
        for b in bins:
            lo, hi, count, bts = b["lo"], b["hi"], b["count"], b["bytes"]
            pf = 100.0 * count / n if n else 0.0
            pb = 100.0 * bts / total if total else 0.0
            bar = ascii_bar(count, max_count, width=30)
            lines.append(
                f"| [{human_bytes(lo)}, {human_bytes(hi)}) "
                f"| {count:,} | {pf:.1f}% "
                f"| {human_bytes(bts)} | {pb:.1f}% | {bar} |"
            )
    else:
        lines.append("(no files found in manifest)")

    flags, notes = _suggest_rclone_flags(stats)

    lines += [
        "",
        "### Suggested rclone flags",
        "",
        "**Reasoning:**",
    ]
    for note in notes:
        lines.append(f"- {note}")

    # Build the flag string two ways: multi-line for readability, single-line for xfer
    flag_parts = [f"{f} {v}" if v is not None else f for f, v in flags]
    multiline = " \\\n  ".join(flag_parts)
    singleline = " ".join(flag_parts)

    lines += [
        "",
        "**Flags** (formatted for copy-paste):",
        "```",
        multiline,
        "```",
        "",
        "**Pass to xfer** via `--rclone-flags`:",
        "```",
        f'--rclone-flags "{singleline}"',
        "```",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "xfer-mcp",
    instructions="""
You help users manage large-scale data transfers between HPC clusters and S3/object storage
using xfer (https://github.com/FluidNumerics/xfer).

xfer orchestrates S3↔S3 and filesystem→S3 transfers via rclone running inside Slurm+pyxis
array jobs. It is designed for datasets with millions of objects of highly variable sizes.

## Transfer workflow
1. **Build manifest** – lists source objects with containerized rclone lsjson → manifest.jsonl
2. **Shard manifest** – splits manifest into byte-balanced chunks → one shard per array task
3. **Render Slurm scripts** – generates worker.sh, sbatch_array.sh, submit.sh
4. **Submit job** – submits the Slurm array job

Or use `run_transfer_pipeline` which wraps all four steps in one call.

## Source / destination formats
- S3 endpoint name (from list_endpoints): e.g. `coreweave-bridge-data`
- Raw rclone path: e.g. `coreweave-obj:bridge-data/prefix`
- Local cluster filesystem path: e.g. `/scratch/mydata` or `~/datasets`

## When a user asks to transfer data
1. Call list_clusters() and list_endpoints() to understand what is configured.
2. Identify source cluster, source path/endpoint, and destination endpoint.
3. Determine which cluster to run the transfer from (must have access to both endpoints).
4. Confirm run_dir and Slurm settings with the user if uncertain.
5. Call run_transfer_pipeline() (or step-by-step tools) to execute.
6. After submission, call check_job_status() to confirm the job is queued.
""",
)


@mcp.resource("xfer://workflow")
def workflow_doc() -> str:
    """Full xfer workflow and state-tracking documentation."""
    return """\
# xfer Transfer Workflow

xfer orchestrates large-scale S3↔S3 and filesystem→S3 transfers via rclone inside
Slurm+pyxis array jobs. Designed for datasets with millions of objects.

## Steps

### 1. Build Manifest
Lists the source with containerized `rclone lsjson` via `srun -n1 -c8`.
Output: `{run_dir}/manifest.jsonl` – one JSON record per file.
Fields: path, size, mtime, hashes, etag, storage_class, source_root, dest_root, run_id.
Can be slow for large datasets (millions of objects).

### 2. Shard Manifest
Splits manifest into N shards using greedy byte bin-packing.
Output: `{run_dir}/shards/shard_XXXXXX.jsonl`
Each shard holds roughly equal total bytes → balanced task runtime.

### 3. Render Slurm Scripts
Generates:
- `{run_dir}/worker.sh`       – per-task: checks .done, runs rclone copy, handles retries
- `{run_dir}/sbatch_array.sh` – array job: `#SBATCH --array=0-{N-1}%{concurrency}`
- `{run_dir}/submit.sh`       – convenience sbatch wrapper
- `{run_dir}/config.resolved.json` – frozen config for auditability

### 4. Submit Job
Runs `sbatch {run_dir}/sbatch_array.sh`.
Slurm launches up to `array_concurrency` tasks concurrently.
Each task: `srun rclone copy --files-from shard_XXXXXX.jsonl SOURCE DEST`

## State Tracking
`{run_dir}/state/`:
- `shard_XXXXXX.done`    – task completed successfully
- `shard_XXXXXX.fail`    – task failed (contains exit code)
- `shard_XXXXXX.attempt` – attempt counter

On failure, tasks are requeued via `scontrol requeue` up to `max_attempts`.

## Logs
`{run_dir}/logs/shard_XXXXXX_attempt_N.log`
"""


# ---------------------------------------------------------------------------
# Discovery tools
# ---------------------------------------------------------------------------


@mcp.tool()
def list_clusters() -> str:
    """List all HPC clusters configured for xfer transfers."""
    cfg = _cfg()
    if not cfg.clusters:
        return "No clusters configured. Add clusters to ~/.config/xfer/mcp.yaml"
    lines = ["Configured clusters:\n"]
    for name, c in cfg.clusters.items():
        lines.append(f"  {name}")
        lines.append(f"    host: {c.user}@{c.host}")
        if c.description:
            lines.append(f"    description: {c.description}")
        lines.append(f"    default_partition: {c.default_partition}")
        lines.append(f"    default_num_shards: {c.default_num_shards}")
        lines.append(f"    default_array_concurrency: {c.default_array_concurrency}")
        lines.append(f"    rclone_image: {c.rclone_image}")
        lines.append("")
    return "\n".join(lines)


@mcp.tool()
def list_endpoints() -> str:
    """List all configured data endpoints (S3 buckets, local filesystems) for transfers."""
    cfg = _cfg()
    if not cfg.endpoints:
        return "No endpoints configured. Add endpoints to ~/.config/xfer/mcp.yaml"
    lines = ["Configured endpoints:\n"]
    for name, e in cfg.endpoints.items():
        lines.append(f"  {name}  [{e.type}]")
        if e.description:
            lines.append(f"    description: {e.description}")
        if e.type == "s3":
            lines.append(f"    rclone_path: {e.rclone_path}")
            if e.accessible_from:
                lines.append(f"    accessible_from: {', '.join(e.accessible_from)}")
        elif e.type == "local":
            lines.append(f"    cluster: {e.cluster}")
            lines.append(f"    path: {e.path}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Pipeline tools
# ---------------------------------------------------------------------------


@mcp.tool()
def run_transfer_pipeline(
    cluster: str,
    source: str,
    dest: str,
    run_dir: str,
    num_shards: int = 0,
    submit: bool = True,
    partition: str = "",
    cpus_per_task: int = 0,
    mem: str = "",
    time_limit: str = "",
    array_concurrency: int = 0,
    max_attempts: int = 0,
    job_name: str = "xfer",
    rclone_flags: str = "",
    extra_lsjson_flags: str = "",
    pyxis_extra: str = "",
    background: bool = False,
) -> str:
    """Run the full xfer pipeline on a cluster: build manifest → shard → render → submit.

    This is the primary tool for starting a data transfer. It calls `xfer run` which
    wraps all four steps. For very large datasets the manifest-build step can take tens of
    minutes; set background=True to launch it asynchronously.

    Args:
        cluster: Cluster name from list_clusters() (e.g. 'lambda')
        source: Source rclone path, endpoint name, or local path (e.g. '/scratch/data')
        dest: Destination rclone path or endpoint name (e.g. 'coreweave-bridge-data')
        run_dir: Directory on the cluster to store all run artifacts
        num_shards: Number of shards / Slurm array tasks (0 = cluster default)
        submit: Submit the Slurm job after rendering scripts (default: True)
        partition: Slurm partition (empty = cluster default)
        cpus_per_task: CPUs per task (0 = cluster default)
        mem: Memory per task e.g. '16G' (empty = cluster default)
        time_limit: Slurm time limit e.g. '12:00:00' (empty = cluster default)
        array_concurrency: Max concurrent array tasks (0 = cluster default)
        max_attempts: Max retry attempts per shard (0 = cluster default)
        job_name: Slurm job name (default: 'xfer')
        rclone_flags: Extra rclone copy flags e.g. '--transfers 32 --checkers 16'
        extra_lsjson_flags: Extra flags for rclone lsjson during manifest build
        pyxis_extra: Extra pyxis/srun flags for container configuration
        background: Run in background (nohup); returns immediately with PID and log path
    """
    c = _cluster(cluster)
    src = _resolve(source)
    dst = _resolve(dest)

    n = num_shards or c.default_num_shards
    p = partition or c.default_partition
    cpus = cpus_per_task or c.default_cpus_per_task
    m = mem or c.default_mem
    t = time_limit or c.default_time_limit
    conc = array_concurrency or c.default_array_concurrency
    attempts = max_attempts or c.default_max_attempts

    parts = [
        _rq(c.xfer_path), "run",
        "--source", _rq(src),
        "--dest", _rq(dst),
        "--run-dir", _rq(run_dir),
        "--num-shards", str(n),
        "--rclone-image", _rq(c.rclone_image),
        "--rclone-config", _rq(c.rclone_config),
        "--container-conf-path", _rq(c.rclone_conf_in_container),
        "--partition", _rq(p),
        "--cpus-per-task", str(cpus),
        "--mem", _rq(m),
        "--time-limit", _rq(t),
        "--array-concurrency", str(conc),
        "--max-attempts", str(attempts),
        "--job-name", _rq(job_name),
    ]
    if submit:
        parts.append("--submit")
    if rclone_flags:
        parts += ["--rclone-flags", _rq(rclone_flags)]
    if extra_lsjson_flags:
        parts += ["--extra-lsjson-flags", _rq(extra_lsjson_flags)]
    if pyxis_extra:
        parts += ["--pyxis-extra", _rq(pyxis_extra)]

    command = " ".join(parts)

    if background:
        log_path = f"{run_dir}/xfer-pipeline.log"
        command = (
            "mkdir -p " + _rq(run_dir) + " && "
            "nohup " + command + " > " + _rq(log_path) + ' 2>&1 & echo "PID: $!"'
        )
        rc, stdout, stderr = _ssh(c, command, timeout=30)
        if rc == 0:
            return (
                f"xfer pipeline started in background.\n{stdout.strip()}\n"
                f"Log: {log_path}\n"
                "Use check_transfer_progress() or ssh_exec() to monitor."
            )
        return _result(rc, stdout, stderr)

    rc, stdout, stderr = _ssh(c, command, timeout=900)
    return _result(rc, stdout, stderr)


@mcp.tool()
def build_manifest(
    cluster: str,
    source: str,
    dest: str,
    run_dir: str,
    extra_lsjson_flags: str = "",
    pyxis_extra: str = "",
    background: bool = False,
) -> str:
    """Build a transfer manifest by listing source objects with containerized rclone lsjson.

    Writes {run_dir}/manifest.jsonl (schema xfer.manifest.v1). For large datasets this
    can take many minutes; set background=True to run asynchronously.

    Args:
        cluster: Cluster name from list_clusters()
        source: Source rclone path, endpoint name, or local path
        dest: Destination path or endpoint name (stored in manifest for traceability)
        run_dir: Run directory; manifest written to {run_dir}/manifest.jsonl
        extra_lsjson_flags: Extra flags passed to rclone lsjson
        pyxis_extra: Extra pyxis/srun flags for container configuration
        background: Run in background; returns PID and log path immediately
    """
    c = _cluster(cluster)
    src = _resolve(source)
    dst = _resolve(dest)

    parts = [
        _rq(c.xfer_path), "manifest", "build",
        "--source", _rq(src),
        "--dest", _rq(dst),
        "--out", _rq(run_dir) + "/manifest.jsonl",
        "--rclone-image", _rq(c.rclone_image),
        "--rclone-config", _rq(c.rclone_config),
        "--container-conf-path", _rq(c.rclone_conf_in_container),
    ]
    if extra_lsjson_flags:
        parts += ["--extra-lsjson-flags", _rq(extra_lsjson_flags)]
    if pyxis_extra:
        parts += ["--pyxis-extra", _rq(pyxis_extra)]

    command = " ".join(parts)

    if background:
        log_path = f"{run_dir}/manifest-build.log"
        command = (
            "mkdir -p " + _rq(run_dir) + " && "
            "nohup " + command + " > " + _rq(log_path) + ' 2>&1 & echo "PID: $!"'
        )
        rc, stdout, stderr = _ssh(c, command, timeout=30)
        if rc == 0:
            return (
                f"Manifest build started in background.\n{stdout.strip()}\n"
                f"Log: {log_path}"
            )
        return _result(rc, stdout, stderr)

    rc, stdout, stderr = _ssh(c, command, timeout=900)
    return _result(rc, stdout, stderr)


@mcp.tool()
def shard_manifest(
    cluster: str,
    run_dir: str,
    num_shards: int = 0,
    strategy: str = "bytes",
) -> str:
    """Shard a manifest into balanced chunks for parallel Slurm array tasks.

    Reads {run_dir}/manifest.jsonl, writes shards to {run_dir}/shards/.
    Run after build_manifest().

    Args:
        cluster: Cluster name from list_clusters()
        run_dir: Run directory containing manifest.jsonl
        num_shards: Number of shards to create (0 = cluster default)
        strategy: 'bytes' (greedy bin-packing by size, default), 'count', or 'hash'
    """
    c = _cluster(cluster)
    n = num_shards or c.default_num_shards
    command = (
        _rq(c.xfer_path) + " manifest shard"
        " --in " + _rq(run_dir) + "/manifest.jsonl"
        " --outdir " + _rq(run_dir) + "/shards"
        " --num-shards " + str(n) +
        " --strategy " + shlex.quote(strategy)
    )
    rc, stdout, stderr = _ssh(c, command, timeout=120)
    return _result(rc, stdout, stderr)


@mcp.tool()
def render_slurm_scripts(
    cluster: str,
    run_dir: str,
    num_shards: int = 0,
    partition: str = "",
    cpus_per_task: int = 0,
    mem: str = "",
    time_limit: str = "",
    array_concurrency: int = 0,
    max_attempts: int = 0,
    job_name: str = "xfer",
    rclone_flags: str = "",
    sbatch_extras: str = "",
    pyxis_extra: str = "",
) -> str:
    """Render Slurm job scripts into run_dir.

    Generates worker.sh, sbatch_array.sh, submit.sh, and config.resolved.json.
    Source and destination roots are read from the manifest already in run_dir.
    Run after shard_manifest().

    Args:
        cluster: Cluster name from list_clusters()
        run_dir: Run directory containing shards/
        num_shards: Must match the count used in shard_manifest (0 = cluster default)
        partition: Slurm partition (empty = cluster default)
        cpus_per_task: CPUs per task (0 = cluster default)
        mem: Memory per task e.g. '16G' (empty = cluster default)
        time_limit: Slurm time limit e.g. '12:00:00' (empty = cluster default)
        array_concurrency: Max concurrent tasks (0 = cluster default)
        max_attempts: Max retry attempts per shard (0 = cluster default)
        job_name: Slurm job name
        rclone_flags: Extra rclone copy flags e.g. '--transfers 32'
        sbatch_extras: Extra SBATCH directives e.g. '#SBATCH --account=myproject'
        pyxis_extra: Extra pyxis/srun flags
    """
    c = _cluster(cluster)
    n = num_shards or c.default_num_shards

    parts = [
        _rq(c.xfer_path), "slurm", "render",
        "--run-dir", _rq(run_dir),
        "--num-shards", str(n),
        "--partition", _rq(partition or c.default_partition),
        "--cpus-per-task", str(cpus_per_task or c.default_cpus_per_task),
        "--mem", _rq(mem or c.default_mem),
        "--time-limit", _rq(time_limit or c.default_time_limit),
        "--array-concurrency", str(array_concurrency or c.default_array_concurrency),
        "--max-attempts", str(max_attempts or c.default_max_attempts),
        "--job-name", _rq(job_name),
        "--rclone-image", _rq(c.rclone_image),
        "--rclone-config", _rq(c.rclone_config),
        "--rclone-conf-in-container", _rq(c.rclone_conf_in_container),
    ]
    if rclone_flags:
        parts += ["--rclone-flags", _rq(rclone_flags)]
    if sbatch_extras:
        parts += ["--sbatch-extras", _rq(sbatch_extras)]
    if pyxis_extra:
        parts += ["--pyxis-extra", _rq(pyxis_extra)]

    rc, stdout, stderr = _ssh(c, " ".join(parts), timeout=60)
    return _result(rc, stdout, stderr)


@mcp.tool()
def submit_transfer_job(cluster: str, run_dir: str) -> str:
    """Submit the rendered Slurm array job for a transfer run.

    Requires render_slurm_scripts() (or run_transfer_pipeline with submit=False) first.
    Returns the Slurm job ID on success.

    Args:
        cluster: Cluster name from list_clusters()
        run_dir: Run directory containing sbatch_array.sh
    """
    c = _cluster(cluster)
    command = _rq(c.xfer_path) + " slurm submit --run-dir " + _rq(run_dir)
    rc, stdout, stderr = _ssh(c, command, timeout=30)
    return _result(rc, stdout, stderr)


# ---------------------------------------------------------------------------
# Monitoring tools
# ---------------------------------------------------------------------------


@mcp.tool()
def check_job_status(
    cluster: str,
    job_id: Optional[str] = None,
    job_name: Optional[str] = None,
    user: Optional[str] = None,
) -> str:
    """Check the status of Slurm jobs on a cluster using squeue.

    Args:
        cluster: Cluster name from list_clusters()
        job_id: Specific Slurm job ID to inspect (optional)
        job_name: Filter by job name e.g. 'xfer' (optional)
        user: Filter by username (default: cluster user from config)
    """
    c = _cluster(cluster)
    u = user or c.user
    parts = [
        "squeue",
        "--user", shlex.quote(u),
        "--format", shlex.quote("%.18i %.9P %.30j %.8u %.8T %.10M %.6D %R"),
    ]
    if job_id:
        parts += ["--job", shlex.quote(job_id)]
    if job_name:
        parts += ["--name", shlex.quote(job_name)]
    rc, stdout, stderr = _ssh(c, " ".join(parts), timeout=30)
    if rc != 0:
        return _result(rc, stdout, stderr)
    return stdout.strip() or "No matching jobs found in the queue."


@mcp.tool()
def check_transfer_progress(cluster: str, run_dir: str) -> str:
    """Check progress of a running or completed xfer transfer.

    Counts .done / .fail state files and reports how many shards are finished,
    failed, or still in-progress.

    Args:
        cluster: Cluster name from list_clusters()
        run_dir: Run directory (contains state/ and shards/ subdirectories)
    """
    c = _cluster(cluster)
    command = (
        "d=" + _rq(run_dir) + "; "
        'done=$(ls "$d/state"/*.done 2>/dev/null | wc -l); '
        'fail=$(ls "$d/state"/*.fail 2>/dev/null | wc -l); '
        'total=$(ls "$d/shards"/shard_*.jsonl 2>/dev/null | wc -l); '
        'echo "Run dir: $d"; '
        'echo "Shards : total=$total  done=$done  failed=$fail  in_progress=$((total - done - fail))"'
    )
    rc, stdout, stderr = _ssh(c, command, timeout=30)
    if rc != 0:
        return _result(rc, stdout, stderr)
    return stdout.strip()


@mcp.tool()
def list_transfer_runs(cluster: str, base_dir: str = "~") -> str:
    """List xfer transfer run directories on a cluster by finding config.resolved.json files.

    Args:
        cluster: Cluster name from list_clusters()
        base_dir: Directory to search under (default: home directory)
    """
    c = _cluster(cluster)
    command = (
        "find " + _rq(base_dir) +
        " -maxdepth 4 -name config.resolved.json 2>/dev/null | head -20"
    )
    rc, stdout, stderr = _ssh(c, command, timeout=30)
    if rc != 0:
        return _result(rc, stdout, stderr)
    if not stdout.strip():
        return f"No xfer run directories found under {base_dir}"
    dirs = [os.path.dirname(p) for p in stdout.strip().splitlines() if p.strip()]
    return "Found xfer run directories:\n" + "\n".join(f"  {d}" for d in dirs)


@mcp.tool()
def get_run_config(cluster: str, run_dir: str) -> str:
    """Read the resolved configuration (config.resolved.json) for an xfer transfer run.

    Args:
        cluster: Cluster name from list_clusters()
        run_dir: Run directory containing config.resolved.json
    """
    c = _cluster(cluster)
    command = "cat " + _rq(run_dir) + "/config.resolved.json"
    rc, stdout, stderr = _ssh(c, command, timeout=15)
    return _result(rc, stdout, stderr)


# ---------------------------------------------------------------------------
# Manifest analysis
# ---------------------------------------------------------------------------


@mcp.tool()
def analyze_manifest(cluster: str, run_dir: str) -> str:
    """Analyse the file size distribution in a manifest and suggest rclone flags.

    Streams {run_dir}/manifest.jsonl through a lightweight Python script on the
    cluster to compute object count, total bytes, size percentiles, and a
    pow2-binned file size histogram. Returns the histogram and recommends
    --transfers, --checkers, --buffer-size, --multi-thread-streams, and S3
    multipart flags tuned to the observed distribution.

    Run after build_manifest() and before render_slurm_scripts() so the
    suggested flags can be fed into --rclone-flags.

    Args:
        cluster: Cluster name from list_clusters()
        run_dir: Run directory containing manifest.jsonl
    """
    c = _cluster(cluster)
    command = (
        "cat " + _rq(run_dir) + "/manifest.jsonl"
        " | python3 -c " + shlex.quote(_ANALYZE_SCRIPT)
    )
    rc, stdout, stderr = _ssh(c, command, timeout=300)
    if rc != 0:
        return _result(rc, stdout, stderr)
    try:
        stats = json.loads(stdout.strip())
    except json.JSONDecodeError as exc:
        return f"Failed to parse analysis output: {exc}\n\nRaw output:\n{stdout[:2000]}"
    return _format_analysis(stats, run_dir)


# ---------------------------------------------------------------------------
# General SSH escape hatch
# ---------------------------------------------------------------------------


@mcp.tool()
def ssh_exec(cluster: str, command: str) -> str:
    """Execute an arbitrary command on a cluster via SSH.

    Use for checking files, reading logs, running diagnostics, or any cluster
    operation not covered by the dedicated tools.

    Args:
        cluster: Cluster name from list_clusters()
        command: Shell command to run on the cluster
    """
    c = _cluster(cluster)
    rc, stdout, stderr = _ssh(c, command, timeout=60)
    return _result(rc, stdout, stderr)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
