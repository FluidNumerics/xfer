#!/usr/bin/env python3
"""
xfer.py — rclone S3↔S3 transfer orchestrator for Slurm + enroot/pyxis

Features
- Build a stable JSONL manifest via `rclone lsjson` (containerized under pyxis)
- Shard the manifest (balanced by bytes, greedy bin-pack)
- Generate Slurm scripts (submit + worker) and optionally submit an array job
- Robust worker: retries, skip-if-done, per-shard logs, per-shard state

Assumptions
- Both source and destination are rclone remotes (S3-compatible)
- rclone runs inside an enroot/pyxis container (via `srun --container-*`)
- You provide an rclone.conf accessible on the submit host and mount it into the container

Typical usage
  python xfer.py manifest build --source s3src:bucket/pfx --dest s3dst:bucket/pfx \
      --out run/manifest.jsonl --rclone-image networkstatic/rclone:latest \
      --rclone-config ~/.config/rclone/rclone.conf

  python xfer.py manifest shard --in run/manifest.jsonl --outdir run/shards --num-shards 256

  python xfer.py slurm render --run-dir run --num-shards 256 --array-concurrency 64 \
      --rclone-image networkstatic/rclone:latest --rclone-config ~/.config/rclone/rclone.conf

  python xfer.py slurm submit --run-dir run
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import typer

app = typer.Typer(add_completion=False, no_args_is_help=True)
manifest_app = typer.Typer(no_args_is_help=False)
slurm_app = typer.Typer(no_args_is_help=True)
app.add_typer(manifest_app, name="manifest")
app.add_typer(slurm_app, name="slurm")

SCHEMA = "xfer.manifest.v1"


# -----------------------------
# Helpers
# -----------------------------
def now_run_id() -> str:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rnd = os.urandom(3).hex()
    return f"{ts}_{rnd}"


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def mkdirp(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, content: str, mode: int = 0o755) -> None:
    path.write_text(content, encoding="utf-8")
    try:
        os.chmod(path, mode)
    except PermissionError:
        pass


def run_cmd(
    cmd: List[str],
    *,
    check: bool = True,
    capture: bool = False,
    env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    eprint(">", " ".join(shlex.quote(c) for c in cmd))
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        env=env,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )


def default_mounts_for_rclone_config(
    rclone_config: Path, container_conf_path: str
) -> List[str]:
    """
    Returns pyxis mounts for rclone config.
    Example: /home/user/.config/rclone/rclone.conf:/etc/rclone/rclone.conf:ro
    """
    return [f"{str(rclone_config)}:{container_conf_path}:ro"]


def pyxis_container_args(
    image: str,
    mounts: List[str],
    workdir: Optional[str] = None,
    extra: Optional[List[str]] = None,
) -> List[str]:
    args = [
        "--container-image",
        image,
    ]
    for m in mounts:
        args += ["--container-mounts", m]  # pyxis expects repeated flag
    if workdir:
        args += ["--container-workdir", workdir]
    if extra:
        args += extra
    return args


def rclone_env_args(
    container_conf_path: str, extra_env: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    env = dict(os.environ)
    env["RCLONE_CONFIG"] = container_conf_path
    if extra_env:
        env.update(extra_env)
    return env


def stable_dest_for_source(source_root: str, dest_root: str, object_path: str) -> str:
    """
    rclone lsjson returns Path relative to the listing root.
    We map: source_root + object_path -> dest_root + object_path.
    Ensure both roots end with '/' for clean join semantics.
    """
    src = source_root.rstrip("/") + "/"
    dst = dest_root.rstrip("/") + "/"
    # object_path is relative, no leading slash
    return dst + object_path


def parse_lsjson_items(lsjson: str) -> Iterable[Dict[str, Any]]:
    data = json.loads(lsjson)
    if not isinstance(data, list):
        raise ValueError("Expected rclone lsjson output to be a JSON array")
    for item in data:
        if not isinstance(item, dict):
            continue
        yield item


def greedy_binpack_by_bytes(
    items: List[Tuple[int, int]], n_bins: int
) -> List[List[int]]:
    """
    items: list of (index, size_bytes)
    Returns bins: list of lists of indices
    """
    bins: List[List[int]] = [[] for _ in range(n_bins)]
    bin_sizes = [0] * n_bins

    # sort by size desc
    items_sorted = sorted(items, key=lambda x: x[1], reverse=True)
    for idx, size in items_sorted:
        j = min(range(n_bins), key=lambda k: bin_sizes[k])
        bins[j].append(idx)
        bin_sizes[j] += int(size)
    return bins


# -----------------------------
# Manifest build/shard
# -----------------------------
@manifest_app.command("build")
def manifest_build(
    source: str = typer.Option(
        ..., help="rclone source root, e.g. s3src:bucket/prefix"
    ),
    dest: str = typer.Option(..., help="rclone dest root, e.g. s3dst:bucket/prefix"),
    out: Path = typer.Option(..., help="Output manifest JSONL path", resolve_path=True),
    rclone_image: str = typer.Option(..., help="Container image containing rclone"),
    rclone_config: Path = typer.Option(
        ...,
        exists=True,
        dir_okay=False,
        help="Local path to rclone.conf",
        resolve_path=True,
    ),
    container_conf_path: str = typer.Option(
        "/etc/rclone/rclone.conf", help="Path inside container for rclone.conf"
    ),
    recursive: bool = typer.Option(True, help="List recursively"),
    # rclone lsjson options
    fast_list: bool = typer.Option(True, help="Use --fast-list for listing"),
    no_modtime: bool = typer.Option(
        False, help="Use --no-modtime if server modtime is unreliable"
    ),
    extra_lsjson_flags: str = typer.Option(
        "", help="Extra flags for `rclone lsjson` (string passed as-is)"
    ),
    pyxis_extra: str = typer.Option(
        "", help="Extra pyxis flags for srun (string passed as-is)"
    ),
    run_id: Optional[str] = typer.Option(
        None, help="Run identifier; default is generated"
    ),
    v: bool = typer.Option(False, "-v", help="Pass -v to rclone for verbose output"),
    vv: bool = typer.Option(
        False, "-vv", help="Pass -vv to rclone for very verbose output"
    ),
) -> None:
    """
    Build manifest.jsonl by running containerized `rclone lsjson` against SOURCE.
    Output is JSONL with stable schema xfer.manifest.v1.
    """
    run_id = run_id or now_run_id()
    mkdirp(out.parent)

    # Ensure error directory exists
    err_dir = Path(out.parent.parent) / "xfer-err"
    mkdirp(err_dir)

    mounts = default_mounts_for_rclone_config(rclone_config, container_conf_path)

    # We run rclone inside a one-task srun, so the tooling works in allocated environments
    # and inherits Slurm settings (modules, env, etc).
    rclone_cmd = ["rclone", "lsjson", source]
    # Handle verbosity flags
    if vv:
        rclone_cmd.append("-vv")
    elif v:
        rclone_cmd.append("-v")
    if recursive:
        rclone_cmd.append("--recursive")
    if fast_list:
        rclone_cmd.append("--fast-list")
    if no_modtime:
        rclone_cmd.append("--no-modtime")
    if extra_lsjson_flags.strip():
        rclone_cmd += shlex.split(extra_lsjson_flags)

    rclone_cmd.append("--files-only")

    srun_cmd = ["srun", "-n", "1", "-c", "8", "--no-container-remap-root"]
    srun_cmd += pyxis_container_args(
        image=rclone_image,
        mounts=mounts,
        workdir="/",
        extra=shlex.split(pyxis_extra) if pyxis_extra.strip() else None,
    )
    srun_cmd += rclone_cmd

    try:
        cp = run_cmd(
            srun_cmd, capture=True, check=True, env=rclone_env_args(container_conf_path)
        )
    except Exception as exc:
        # Write error details to xfer-err/
        err_file = err_dir / f"manifest_build-{run_id}.log"
        with err_file.open("w", encoding="utf-8") as ef:
            ef.write(f"Exception: {exc}\n")
            import traceback

            ef.write(traceback.format_exc())
        # If subprocess.CalledProcessError, try to write stderr
        if hasattr(exc, "stderr") and exc.stderr:
            with err_file.open("a", encoding="utf-8") as ef:
                ef.write("\n--- STDERR ---\n")
                ef.write(str(exc.stderr))
        eprint(f"ERROR: srun/rclone failed, see {err_file}")
        raise

    # Build JSONL
    n = 0
    bytes_total = 0
    with out.open("w", encoding="utf-8") as f:
        for item in parse_lsjson_items(cp.stdout):
            # Skip directories
            if item.get("IsDir") is True:
                continue
            rel_path = item.get("Path")
            if not rel_path or not isinstance(rel_path, str):
                continue

            size = int(item.get("Size") or 0)
            bytes_total += size

            # rclone lsjson fields vary by backend; be defensive
            mtime = item.get("ModTime")  # ISO-ish string if present
            hashes = item.get("Hashes") if isinstance(item.get("Hashes"), dict) else {}
            etag = item.get("ETag") or item.get("etag")  # sometimes present
            storage_class = item.get("StorageClass") or item.get(
                "StorageClass"
            )  # may be absent
            meta = (
                item.get("Metadata") if isinstance(item.get("Metadata"), dict) else {}
            )

            rec = {
                "schema": SCHEMA,
                "run_id": run_id,
                "source_root": source,
                "dest_root": dest,
                "source": source.rstrip("/") + "/" + rel_path,
                "dest": stable_dest_for_source(source, dest, rel_path),
                "path": rel_path,
                "size": size,
                "mtime": mtime,
                "hashes": hashes,
                "etag": etag,
                "storage_class": storage_class,
                "meta": meta,
            }
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
            n += 1

    eprint(f"Wrote {n} items, {bytes_total} bytes -> {out}")


@manifest_app.command("shard")
def manifest_shard(
    infile: Path = typer.Option(
        ...,
        "--in",
        exists=True,
        dir_okay=False,
        help="Input manifest JSONL",
        resolve_path=True,
    ),
    outdir: Path = typer.Option(..., help="Output shard directory", resolve_path=True),
    num_shards: int = typer.Option(..., min=1, help="Number of shards"),
    strategy: str = typer.Option("bytes", help="Sharding strategy: bytes|count|hash"),
) -> None:
    """
    Shard manifest JSONL into shard_XXXXXX.jsonl files.
    Default strategy 'bytes' uses greedy bin-packing by object size.
    """
    mkdirp(outdir)

    # Read all lines (OK up to a few million; if you expect 20M+, we can make this streaming later)
    lines: List[str] = infile.read_text(encoding="utf-8").splitlines()
    recs: List[Dict[str, Any]] = []
    sizes: List[int] = []
    for ln in lines:
        if not ln.strip():
            continue
        r = json.loads(ln)
        recs.append(r)
        sizes.append(int(r.get("size") or 0))

    n = len(recs)
    if n == 0:
        raise typer.Exit(code=2)

    if strategy == "count":
        bins: List[List[int]] = [[] for _ in range(num_shards)]
        for i in range(n):
            bins[i % num_shards].append(i)
    elif strategy == "hash":
        bins = [[] for _ in range(num_shards)]
        for i, r in enumerate(recs):
            key = (r.get("source") or r.get("path") or str(i)).encode("utf-8")
            h = int(hashlib.sha1(key).hexdigest(), 16)
            bins[h % num_shards].append(i)
    elif strategy == "bytes":
        items = [(i, sizes[i]) for i in range(n)]
        bins = greedy_binpack_by_bytes(items, num_shards)
    else:
        raise typer.BadParameter("strategy must be one of: bytes, count, hash")

    # Write shards
    total_bytes = sum(sizes)
    for shard_id, idxs in enumerate(bins):
        shard_path = outdir / f"shard_{shard_id:06d}.jsonl"
        with shard_path.open("w", encoding="utf-8") as f:
            for i in idxs:
                f.write(json.dumps(recs[i], separators=(",", ":")) + "\n")

    # Write shard index metadata
    meta = {
        "schema": "xfer.shards.v1",
        "input": str(infile),
        "outdir": str(outdir),
        "num_shards": num_shards,
        "strategy": strategy,
        "num_records": n,
        "bytes_total": total_bytes,
        "created_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }
    (outdir / "shards.meta.json").write_text(
        json.dumps(meta, indent=2) + "\n", encoding="utf-8"
    )
    eprint(f"Wrote {num_shards} shards to {outdir} (records={n}, bytes={total_bytes})")


# -----------------------------
# Slurm render/submit
# -----------------------------
WORKER_SH = r"""#!/usr/bin/env bash
set -euo pipefail

# Required env:
#   RUN_DIR
#   XFER_SOURCE_ROOT
#   XFER_DEST_ROOT
#   RCLONE_IMAGE
#   RCLONE_CONFIG_HOST   (path on host)
# Optional env:
#   RCLONE_CONF_IN_CONTAINER (default /etc/rclone/rclone.conf)
#   RCLONE_FLAGS
#   MAX_ATTEMPTS (default 5)
#   PYXIS_EXTRA (extra flags for pyxis, optional)

: "${RUN_DIR:?}"
: "${XFER_SOURCE_ROOT:?}"
: "${XFER_DEST_ROOT:?}"
: "${RCLONE_IMAGE:?}"
: "${RCLONE_CONFIG_HOST:?}"

RCLONE_CONF_IN_CONTAINER="${RCLONE_CONF_IN_CONTAINER:-/etc/rclone/rclone.conf}"
RCLONE_FLAGS="${RCLONE_FLAGS:-}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-5}"
PYXIS_EXTRA="${PYXIS_EXTRA:-}"

SHARD_DIR="${RUN_DIR}/shards"
LOG_DIR="${RUN_DIR}/logs"
STATE_DIR="${RUN_DIR}/state"
mkdir -p "${LOG_DIR}" "${STATE_DIR}"

TASK_ID="${SLURM_ARRAY_TASK_ID:?}"
printf -v SHARD_FILE "%s/shard_%06d.jsonl" "${SHARD_DIR}" "${TASK_ID}"

DONE_FILE="${STATE_DIR}/shard_${TASK_ID}.done"
FAIL_FILE="${STATE_DIR}/shard_${TASK_ID}.fail"
ATTEMPT_FILE="${STATE_DIR}/shard_${TASK_ID}.attempt"
FILES_FROM="${STATE_DIR}/shard_${TASK_ID}.files"

if [[ -f "${DONE_FILE}" ]]; then
  echo "Shard ${TASK_ID} already done: ${DONE_FILE}"
  exit 0
fi

attempt=0
if [[ -f "${ATTEMPT_FILE}" ]]; then attempt="$(cat "${ATTEMPT_FILE}" || echo 0)"; fi
attempt=$((attempt+1))
echo "${attempt}" > "${ATTEMPT_FILE}"

LOG="${LOG_DIR}/shard_${TASK_ID}_attempt_${attempt}.log"
exec > >(tee -a "${LOG}") 2>&1

echo "== $(date -Is) shard=${TASK_ID} attempt=${attempt} job=${SLURM_JOB_ID} node=$(hostname) =="

if [[ ! -f "${SHARD_FILE}" ]]; then
  echo "Missing shard file: ${SHARD_FILE}"
  exit 2
fi

# Extract relative paths from JSONL for --files-from.
# We stored "path" as relative to source_root.
awk -F'"path":"' 'NF>1 {split($2,a,"\""); print a[1]}' "${SHARD_FILE}" > "${FILES_FROM}"

COUNT=$(wc -l < "${FILES_FROM}" || echo 0)
echo "Files in shard: ${COUNT}"
if [[ "${COUNT}" -eq 0 ]]; then
  echo "Empty shard. Marking done."
  : > "${DONE_FILE}"
  exit 0
fi

# Mount the rclone.conf into container and run rclone inside container.
# We use srun so each array task runs as its own containerized step.
MOUNTS="${RCLONE_CONFIG_HOST}:${RCLONE_CONF_IN_CONTAINER}:ro,${RUN_DIR}:${RUN_DIR}"

set +e
srun -N1 -n1 \
  --container-image "${RCLONE_IMAGE}" \
  --container-mounts "${MOUNTS}" \
  --no-container-remap-root \
  ${PYXIS_EXTRA} \
  rclone copy \
   --config "${RCLONE_CONF_IN_CONTAINER}" \
    ${RCLONE_FLAGS} \
    --files-from "${FILES_FROM}" \
    "${XFER_SOURCE_ROOT}" \
    "${XFER_DEST_ROOT}"
rc=$?
set -e

if [[ "${rc}" -eq 0 ]]; then
  echo "OK shard=${TASK_ID}"
  : > "${DONE_FILE}"
  rm -f "${FAIL_FILE}"
  exit 0
fi

echo "FAIL shard=${TASK_ID} rc=${rc}"
echo "${rc}" > "${FAIL_FILE}"

if [[ "${attempt}" -lt "${MAX_ATTEMPTS}" ]]; then
  echo "Retrying via requeue (attempt ${attempt}/${MAX_ATTEMPTS})"
  scontrol requeue "${SLURM_JOB_ID}"
  exit 0
fi

echo "Exceeded max attempts (${MAX_ATTEMPTS}). Giving up."
exit "${rc}"
"""

SUBMIT_SH = r"""#!/usr/bin/env bash
set -euo pipefail

: "${RUN_DIR:?}"
cd "${RUN_DIR}"

sbatch "${RUN_DIR}/sbatch_array.sh"
"""

SBATCH_ARRAY_SH = r"""#!/usr/bin/env bash
#SBATCH --job-name={job_name}
#SBATCH --output={run_dir}/slurm-%A_%a.out
#SBATCH --error={run_dir}/slurm-%A_%a.err
#SBATCH --array=0-{array_max}%{array_concurrency}
#SBATCH --time={time_limit}
#SBATCH --partition={partition}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --mem={mem}
{extras}

set -euo pipefail

export RUN_DIR="{run_dir}"
export XFER_SOURCE_ROOT="{source_root}"
export XFER_DEST_ROOT="{dest_root}"

export RCLONE_IMAGE="{rclone_image}"
export RCLONE_CONFIG_HOST="{rclone_config_host}"
export RCLONE_CONF_IN_CONTAINER="{rclone_conf_in_container}"

export RCLONE_FLAGS={rclone_flags}
export MAX_ATTEMPTS="{max_attempts}"
export PYXIS_EXTRA={pyxis_extra}

bash "{run_dir}/worker.sh"
"""


@slurm_app.command("render")
def slurm_render(
    run_dir: Path = typer.Option(
        ..., help="Run directory containing shards/", resolve_path=True
    ),
    num_shards: int = typer.Option(..., min=1, help="Number of shards (array size)"),
    array_concurrency: int = typer.Option(64, min=1, help="Max concurrent array tasks"),
    job_name: str = typer.Option("xfer", help="Slurm job name"),
    time_limit: str = typer.Option("24:00:00", help="Slurm time limit"),
    partition: str = typer.Option("transfer", help="Slurm partition"),
    cpus_per_task: int = typer.Option(4, min=1, help="cpus-per-task"),
    mem: str = typer.Option("8G", help="Slurm --mem"),
    # rclone container config
    rclone_image: str = typer.Option(..., help="Container image containing rclone"),
    rclone_config: Path = typer.Option(
        ...,
        exists=True,
        dir_okay=False,
        help="Host path to rclone.conf",
        resolve_path=True,
    ),
    rclone_conf_in_container: str = typer.Option(
        "/etc/rclone/rclone.conf", help="Path inside container"
    ),
    # these are passed as shell words (quote them properly in CLI)
    rclone_flags: str = typer.Option(
        "--transfers 32 --checkers 64 --fast-list --retries 10 --low-level-retries 20",
        help="rclone copy flags",
    ),
    # required roots (saved during manifest build, but we'll read from manifest if present)
    source_root: Optional[str] = typer.Option(
        None, help="Source root (rclone remote:path). If omitted, read from manifest."
    ),
    dest_root: Optional[str] = typer.Option(
        None, help="Dest root (rclone remote:path). If omitted, read from manifest."
    ),
    max_attempts: int = typer.Option(
        5, min=1, help="Worker max attempts (requeue retries)"
    ),
    sbatch_extras: str = typer.Option(
        "", help="Extra SBATCH lines, e.g. '#SBATCH --account=foo\\n#SBATCH --qos=bar'"
    ),
    pyxis_extra: str = typer.Option(
        "", help="Extra pyxis flags (string placed after --container-mounts...)"
    ),
) -> None:
    """
    Render worker.sh, sbatch_array.sh, and submit.sh under run_dir.
    """
    mkdirp(run_dir)
    mkdirp(run_dir / "logs")
    mkdirp(run_dir / "state")

    # If source/dest not provided, try to read first line of manifest.jsonl (if present)
    if source_root is None or dest_root is None:
        manifest = run_dir / "manifest.jsonl"
        if manifest.exists():
            first = None
            for ln in manifest.read_text(encoding="utf-8").splitlines():
                if ln.strip():
                    first = json.loads(ln)
                    break
            if first:
                source_root = source_root or first.get("source_root")
                dest_root = dest_root or first.get("dest_root")

    if not source_root or not dest_root:
        raise typer.BadParameter(
            "source_root/dest_root not set and could not be read from run_dir/manifest.jsonl"
        )

    # Write scripts
    write_text(run_dir / "worker.sh", WORKER_SH, mode=0o755)
    write_text(run_dir / "submit.sh", SUBMIT_SH, mode=0o755)

    array_max = num_shards - 1

    def shell_words(s: str) -> str:
        # Return a safe shell fragment. If empty, return '""' (so env var is defined).
        if not s.strip():
            return '""'
        return (
            shlex.quote(s)
            if any(ch in s for ch in ['"', "'", " ", "\t", "\n", "$", "`", "\\"])
            else s
        )

    extras = sbatch_extras.rstrip()
    extras = extras if extras else ""

    sbatch_text = SBATCH_ARRAY_SH.format(
        job_name=job_name,
        run_dir=str(run_dir),
        array_max=array_max,
        array_concurrency=array_concurrency,
        time_limit=time_limit,
        partition=partition,
        cpus_per_task=cpus_per_task,
        mem=mem,
        extras=extras,
        source_root=source_root,
        dest_root=dest_root,
        rclone_image=rclone_image,
        rclone_config_host=str(rclone_config),
        rclone_conf_in_container=rclone_conf_in_container,
        rclone_flags=shell_words(rclone_flags),
        max_attempts=max_attempts,
        pyxis_extra=shell_words(pyxis_extra),
    )
    write_text(run_dir / "sbatch_array.sh", sbatch_text, mode=0o755)

    # Persist resolved config
    cfg = {
        "schema": "xfer.runconfig.v1",
        "run_dir": str(run_dir),
        "num_shards": num_shards,
        "array_concurrency": array_concurrency,
        "job_name": job_name,
        "time_limit": time_limit,
        "partition": partition,
        "cpus_per_task": cpus_per_task,
        "mem": mem,
        "source_root": source_root,
        "dest_root": dest_root,
        "rclone_image": rclone_image,
        "rclone_config_host": str(rclone_config),
        "rclone_conf_in_container": rclone_conf_in_container,
        "rclone_flags": rclone_flags,
        "max_attempts": max_attempts,
        "sbatch_extras": sbatch_extras,
        "pyxis_extra": pyxis_extra,
        "created_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }
    (run_dir / "config.resolved.json").write_text(
        json.dumps(cfg, indent=2) + "\n", encoding="utf-8"
    )

    eprint(f"Rendered scripts in {run_dir}: worker.sh, sbatch_array.sh, submit.sh")


@slurm_app.command("submit")
def slurm_submit(
    run_dir: Path = typer.Option(
        ..., help="Run directory containing sbatch_array.sh", resolve_path=True
    ),
) -> None:
    """
    Submit the rendered Slurm array job (sbatch).
    """
    sbatch_script = run_dir / "sbatch_array.sh"
    if not sbatch_script.exists():
        raise typer.BadParameter(f"Missing {sbatch_script}. Run `slurm render` first.")
    cp = run_cmd(["sbatch", str(sbatch_script)], capture=True, check=True)
    print(cp.stdout.strip())


# -----------------------------
# Convenience "pipeline" command
# -----------------------------
@app.command("run")
def run_all(
    run_dir: Path = typer.Option(
        Path("run"), help="Run directory (will be created)", resolve_path=True
    ),
    source: str = typer.Option(..., help="Source root (rclone remote:path)"),
    dest: str = typer.Option(..., help="Dest root (rclone remote:path)"),
    num_shards: int = typer.Option(256, min=1, help="Number of shards"),
    array_concurrency: int = typer.Option(64, min=1, help="Max concurrent array tasks"),
    # container + config
    rclone_image: str = typer.Option(..., help="Container image containing rclone"),
    rclone_config: Path = typer.Option(
        ...,
        exists=True,
        dir_okay=False,
        help="Host path to rclone.conf",
        resolve_path=True,
    ),
    container_conf_path: str = typer.Option(
        "/etc/rclone/rclone.conf", help="Path inside container for rclone.conf"
    ),
    # listing + copy knobs
    extra_lsjson_flags: str = typer.Option("", help="Extra flags for lsjson"),
    rclone_flags: str = typer.Option(
        "--transfers 32 --checkers 64 --fast-list --retries 10 --low-level-retries 20",
        help="rclone copy flags",
    ),
    # slurm knobs
    job_name: str = typer.Option("xfer", help="Slurm job name"),
    time_limit: str = typer.Option("24:00:00", help="Slurm time limit"),
    partition: str = typer.Option("transfer", help="Slurm partition"),
    cpus_per_task: int = typer.Option(4, min=1, help="cpus-per-task"),
    mem: str = typer.Option("8G", help="Slurm mem"),
    max_attempts: int = typer.Option(5, min=1, help="Worker retry attempts"),
    sbatch_extras: str = typer.Option(
        "", help="Extra SBATCH lines, e.g. '#SBATCH --account=foo\\n#SBATCH --qos=bar'"
    ),
    submit: bool = typer.Option(False, help="If set, submit job after rendering"),
    pyxis_extra: str = typer.Option("", help="Extra pyxis flags"),
) -> None:
    """
    One-shot pipeline: build manifest -> shard -> render slurm -> (optional) submit.
    """
    mkdirp(run_dir)
    manifest_path = run_dir / "manifest.jsonl"
    shards_dir = run_dir / "shards"

    manifest_build(
        source=source,
        dest=dest,
        out=manifest_path,
        rclone_image=rclone_image,
        rclone_config=rclone_config,
        container_conf_path=container_conf_path,
        extra_lsjson_flags=extra_lsjson_flags,
        pyxis_extra=pyxis_extra,
    )
    manifest_shard(
        infile=manifest_path, outdir=shards_dir, num_shards=num_shards, strategy="bytes"
    )
    slurm_render(
        run_dir=run_dir,
        num_shards=num_shards,
        array_concurrency=array_concurrency,
        job_name=job_name,
        time_limit=time_limit,
        partition=partition,
        cpus_per_task=cpus_per_task,
        mem=mem,
        rclone_image=rclone_image,
        rclone_config=rclone_config,
        rclone_conf_in_container=container_conf_path,
        rclone_flags=rclone_flags,
        source_root=source,
        dest_root=dest,
        max_attempts=max_attempts,
        sbatch_extras=sbatch_extras,
        pyxis_extra=pyxis_extra,
    )
    if submit:
        slurm_submit(run_dir=run_dir)


def main() -> None:
    app()


if __name__ == "__main__":
    app()
