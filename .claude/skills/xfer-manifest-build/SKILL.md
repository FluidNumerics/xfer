---
name: xfer-manifest-build
description: Build an xfer JSONL manifest for a large S3-to-S3 (or POSIX-to-S3) data transfer. Use when the user wants to list source objects for a transfer, kick off `xfer manifest build`, or start the xfer pipeline from scratch. Prefers running on a Slurm cluster that has a POSIX mount of the source bucket, since listing over POSIX is far faster than listing over S3.
---

# xfer-manifest-build

Drives `xfer manifest build` — the first stage of the xfer pipeline. Produces `run/manifest.jsonl`.

## Operating model

Assume the user is working from a **local workstation** at the root of the `xfer` repo, in a `uv` environment (`uv venv && uv sync` already done). The workstation orchestrates. **`xfer manifest build` itself must run on a Slurm login node** because it invokes `srun` + pyxis internally.

## Step 1 — Pick the build host (POSIX-first)

Ask the user (or infer from prior conversation / CLAUDE.md / a site-config file if one exists):

1. What is the source? (S3 remote like `s3src:bucket/prefix` or a POSIX path like `/mnt/data/dataset`)
2. What is the destination?
3. **Does any Slurm cluster have a POSIX mount equivalent to the source bucket?** (e.g., `/mnt/data/important-files` on cluster `weka` corresponds to `weka-s3:important-files`.) If yes, strongly prefer that cluster as the build host and pass the POSIX path as `--source` — listing over POSIX is latency-bound and much faster than listing over S3.
4. If no POSIX mount exists, pick any Slurm cluster with network access to the source endpoint. Still prefer the side with better network proximity to source.

Record the chosen build host's hostname, username, and the xfer repo path on that host (default `~/xfer`). These are needed for SSH.

## Step 2 — Pre-flight on the login node

Run a single non-destructive SSH probe to discover what's already in place:

```bash
ssh <user>@<login-node> '
  command -v uv || echo "UV_MISSING"
  test -d <xfer-repo-path> && echo "REPO_PRESENT" || echo "REPO_MISSING"
  test -d <xfer-repo-path>/.venv && echo "VENV_PRESENT" || echo "VENV_MISSING"
  test -f <rclone-conf-path> && echo "RCLONE_CONF_PRESENT" || echo "RCLONE_CONF_MISSING"
  sinfo -h -o "%P %a %D %C %G" | head -20
'
```

Parse results and react:

| State                        | Action                                                                              |
| ---------------------------- | ----------------------------------------------------------------------------------- |
| `UV_MISSING`                 | Install per-user: `curl -LsSf https://astral.sh/uv/install.sh \| sh` (confirm first)|
| `REPO_MISSING`               | Rsync the local repo up (see step 2a below)                                         |
| `REPO_PRESENT` but older     | Offer to rsync updates from the workstation (step 2a); never force without asking   |
| `VENV_MISSING`               | Run `uv sync` on the login node after repo is in place (step 2b)                    |
| `RCLONE_CONF_MISSING`        | Invoke `xfer-rclone-config` to create/deploy the config to this cluster — do not `scp` blindly |

Also verify a CPU-only partition is visible in `sinfo` output (the `%G` column should be empty or `(null)` for CPU-only).

### Step 2a — Sync the repo to the login node (if needed)

```bash
rsync -av \
  --exclude='.venv/' --exclude='.git/' --exclude='run*/' \
  --exclude='__pycache__/' --exclude='*.egg-info/' \
  ./ <user>@<login-node>:<xfer-repo-path>/
```

Exclude `.venv/` (platform-specific; must be rebuilt remotely), `.git/` (not needed to run), and any local `run*/` dirs (those stay on the workstation or move separately).

### Step 2b — Bootstrap the uv environment on the login node

```bash
ssh <user>@<login-node> '
  cd <xfer-repo-path> &&
  uv venv &&
  uv sync
'
```

Stream the output so the user sees `uv sync` progress. If `uv sync` fails (e.g., no network from login node, locked-down Python), surface the error and stop — don't try to force-install.

## Step 3 — Run the build

Prefer a **CPU-only partition** with **4–8 cores**. The `srun` inside `xfer manifest build` already requests 8 cores (see `cli.py:251`), so ask the user to confirm the partition and any `--sbatch-extras`-style account/QoS flags their site requires before submission.

Invoke on the login node via SSH:

```bash
ssh <user>@<login-node> '
  cd <xfer-repo-path> &&
  uv run xfer manifest build \
    --source <source> \
    --dest   <dest> \
    --out    run/manifest.jsonl \
    --rclone-image rclone/rclone:latest \
    --rclone-config <rclone-conf-path-on-this-cluster>
'
```

`<rclone-conf-path-on-this-cluster>` is the absolute path on the **build cluster's login node**, not the workstation path. If unsure, ask the user — or run `xfer-rclone-config` to resolve/deploy it for this cluster.

Notes:
- If the source is a POSIX path, the `--source` value is the filesystem path (e.g., `/mnt/data/dataset`), not an rclone remote. The destination remains an rclone remote.
- `--fast-list` is already the default for S3 sources (see `cli.py:200`). Pass `--no-fast-list` when the source is a POSIX path.
- Stream output back so the user sees progress. Don't background it.

## Step 4 — Retrieve the manifest and note the vantage point

Pull the manifest back to the workstation so downstream skills (analyze, shard, render) can run locally:

```bash
rsync -av <user>@<login-node>:<xfer-repo-path>/run/manifest.jsonl ./run/manifest.jsonl
```

Tell the user **the vantage point of the manifest** — i.e., "source was listed as `<source>` from host `<login-node>`." If the transfer will run on a different cluster with a different view (e.g., built from POSIX `/mnt/data/x`, transferred via `weka-s3:x`), they will need to invoke the `xfer-manifest-rebase` skill before render/submit.

## Safety

- Do not delete or overwrite an existing `run/manifest.jsonl` without confirming.
- Do not pick a build partition without the user's confirmation if the cluster has multiple options.
- If `ssh` or `rsync` would touch a shared path on the login node (e.g., a group scratch dir), confirm the path first.

## After this skill

Recommend the user next invoke `xfer-manifest-analyze` to pick rclone flags from the file-size histogram.
