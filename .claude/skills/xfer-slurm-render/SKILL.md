---
name: xfer-slurm-render
description: Render Slurm batch scripts (`worker.sh`, `sbatch_array.sh`, `submit.sh`, `config.resolved.json`) for an xfer transfer. Use after manifest shard — and after rebase, if the vantage point changed — whenever the user wants to generate the runnable job artifacts. Picks partition and resources informed by current cluster load.
---

# xfer-slurm-render

Drives `xfer slurm render` — produces the runnable job artifacts under `run/`.

## Operating model

Runs **locally on the workstation**. No SSH needed to render, but you will SSH to candidate login nodes in step 1 to check cluster load before picking the transfer cluster.

## Step 1 — Pick the transfer cluster (load-aware, CPU-only preferred)

If the user already decided which cluster to transfer on, skip to step 2. Otherwise, help them pick.

1. Ask for the list of candidate Slurm clusters with login-node SSH access.
2. For each candidate, probe load and CPU-only partitions:

```bash
ssh <user>@<login-node> '
  sinfo -h -o "%P %a %l %D %C %G" | head -20
  echo "---"
  squeue -h -o "%T %C" | awk '"'"'{s[$1]+=$2} END{for(k in s) print k, s[k]}'"'"'
'
```

- `%G` is the GRES column — a partition reporting `(null)` or no GPUs is CPU-only. **Prefer CPU-only partitions** — transfers are I/O-bound and GPU nodes are wasted here.
- From `%C` (allocated/idle/other/total cores), compute idle-fraction per partition. Prefer the partition with the highest idle fraction.
- From `squeue` aggregated by state, check how many jobs are queued (`PD`) vs running (`R`). High pending counts mean the next job will wait.

Present a short comparison to the user and recommend one. Let them override.

## Step 2 — Confirm "vantage-unchanged" invariant

Before rendering, verify the manifest and shards reflect the transfer host's view of source/dest:

- If the build host differs from the chosen transfer host **and** their rclone.conf views / POSIX mounts differ, **stop and invoke `xfer-manifest-rebase` first**. Otherwise workers will hit wrong-URI errors.

Don't assume — if unsure, check `run/config.resolved.json` (if a prior render exists) against the chosen cluster's rclone remotes.

## Step 3 — Gather render inputs

Collect from the user (with defaults from `run/analyze.json` and the chosen partition):

| Flag                   | Source                                                  |
| ---------------------- | ------------------------------------------------------- |
| `--run-dir`            | `run` (or whatever the prior skills used)               |
| `--num-shards`         | count of `run/shards/shard_*.jsonl`                     |
| `--array-concurrency`  | ask user; typically 32–256, bounded by partition        |
| `--job-name`           | ask user; default `xfer-<dataset-name>`                 |
| `--time-limit`         | ask user; default 24:00:00                              |
| `--partition`          | from step 1                                             |
| `--cpus-per-task`      | 4 (default)                                             |
| `--mem`                | 8G (default; bump for large-files profile)              |
| `--rclone-image`       | `rclone/rclone:latest`                                  |
| `--rclone-config`      | absolute path to rclone.conf **on the transfer cluster's compute nodes** (see note below) |
| `--rclone-flags`       | `suggested_flags` from `run/analyze.json`               |
| `--max-attempts`       | 3 (default)                                             |
| `--sbatch-extras`      | site-specific `--account=...`, `--qos=...`, etc.        |
| `--pyxis-extra`        | extra `srun --container-*` flags if site requires them  |

The `--rclone-config` path is baked into `sbatch_array.sh` and resolved **on the transfer cluster at job time**, not on the workstation. It must:

- Be an absolute path valid on that cluster's compute nodes (home dirs and shared paths differ between sites).
- Exist with `0600` permissions before the job starts.

If the user doesn't already have the config deployed to this cluster at a known path, **stop and invoke `xfer-rclone-config`** to set it up and record the path. Don't guess the path — a wrong value here means every array task will fail identically at container start.

## Step 4 — Render

```bash
uv run xfer slurm render \
  --run-dir <run-dir> \
  --num-shards <N> \
  --array-concurrency <K> \
  --job-name <name> \
  --time-limit <HH:MM:SS> \
  --partition <part> \
  --cpus-per-task <c> \
  --mem <mem> \
  --rclone-image <image> \
  --rclone-config <path-on-cluster> \
  --rclone-flags "<flags-from-analyze>" \
  --max-attempts 3 \
  --sbatch-extras '<multi-line sbatch directives>' \
  --pyxis-extra '<extra pyxis flags>'
```

## Step 5 — Verify the outputs

After render, show the user:
- `run/sbatch_array.sh` — read and show the `#SBATCH` header so they can eyeball partition/time/mem
- `run/config.resolved.json` — the frozen run config
- `run/worker.sh` exists and is executable

## After this skill

Recommend `xfer-slurm-submit`.
