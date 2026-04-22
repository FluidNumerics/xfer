---
name: xfer-oneshot
description: Run the whole xfer pipeline (build → shard → render → optional submit) in a single `xfer run` invocation. Use this as an escape hatch for small, straightforward transfers where the user doesn't need the knobs that the staged skills (analyze, rebase, etc.) expose. Do NOT use for very large datasets, multi-cluster transfers, or any case where a separate vantage point (POSIX mount on one cluster, S3 on another) is involved — those need the staged pipeline.
---

# xfer-oneshot

Drives `xfer run` — the one-shot pipeline in `cli.py:1030`. It invokes `manifest build`, `manifest shard`, and `slurm render` in order, optionally submitting. No `analyze`, no `rebase`, no load-aware cluster selection.

## When to use this — and when not to

**Good fit:**

- Source and destination are both reachable as rclone remotes from a single Slurm cluster.
- Object count and total bytes are small enough that default shard/concurrency knobs are fine.
- The user has already run through the full pipeline at least once for this dataset and trusts the defaults.
- Quick demos, smoke tests, CI-style small transfers.

**Bad fit — redirect to the staged pipeline:**

- Total bytes ≥ 10 TiB (`xfer-manifest-analyze` will compute shard count properly).
- Source available only via POSIX mount on a different cluster than the transfer cluster (need `xfer-manifest-rebase`).
- The user wants to tune rclone flags to the dataset's size profile (`xfer-manifest-analyze` does this).
- User wants to inspect / edit the manifest, shards, or sbatch script before submitting.

If any of the "bad fit" conditions apply, **do not invoke this skill** — recommend `xfer-manifest-build` and the staged flow instead.

## Operating model

Runs from a **Slurm login node** (not the workstation), same as `xfer-manifest-build`, because `xfer run` calls `xfer manifest build` internally, which requires `srun` + pyxis. Pre-flight (uv installed, repo staged, venv synced, rclone.conf deployed) is identical to `xfer-manifest-build` Step 2 — invoke `xfer-manifest-build`'s pre-flight probe or `xfer-rclone-config` as needed before running this skill.

## Step 1 — Confirm fit

Ask the user explicitly:

- Rough total bytes? (If ≥ 10 TiB, redirect to staged flow.)
- Same cluster for listing and transfer? (If not, redirect.)
- Do you want tuned rclone flags? (If yes, redirect to analyze first.)

Don't proceed silently — users sometimes reach for `xfer run` by habit when they shouldn't.

## Step 2 — Gather inputs

| Flag                   | Notes                                                                                    |
| ---------------------- | ---------------------------------------------------------------------------------------- |
| `--run-dir`            | default `run`; pick a fresh directory                                                    |
| `--source`             | rclone remote or POSIX path                                                              |
| `--dest`               | rclone remote                                                                            |
| `--num-shards`         | default 256; override only if the user has a reason                                      |
| `--array-concurrency`  | default 64                                                                               |
| `--rclone-image`       | e.g. `rclone/rclone:latest`                                                              |
| `--rclone-config`      | absolute path on **this cluster** (the login node = transfer host here, since `xfer run` assumes same cluster) |
| `--rclone-flags`       | sensible default is baked in; override if the user specifies                             |
| `--partition`          | a CPU-only partition on this cluster                                                     |
| `--time-limit`         | default `24:00:00`                                                                       |
| `--cpus-per-task`      | default 4                                                                                |
| `--mem`                | default `8G`                                                                             |
| `--max-attempts`       | default 5                                                                                |
| `--sbatch-extras`      | site-specific `--account=...`, `--qos=...`                                               |
| `--pyxis-extra`        | extra `srun --container-*` flags if needed                                               |
| `--submit`             | boolean; if set, sbatch runs immediately after render                                    |

## Step 3 — Run

Via SSH to the login node, inside the xfer repo:

```bash
ssh <user>@<login-node> '
  cd <xfer-repo-path> &&
  uv run xfer run \
    --run-dir <run-dir> \
    --source <source> \
    --dest <dest> \
    --rclone-image <image> \
    --rclone-config <path-on-this-cluster> \
    --partition <part> \
    --sbatch-extras "<extras>" \
    [--submit]
'
```

Stream output so the user sees manifest progress, sharding stats, and the render summary. If `--submit` was set, capture the `Submitted batch job <id>` line.

## Step 4 — Report

Print a short summary:
- Total objects / total bytes (from the manifest build line).
- Number of shards (from the shard summary).
- Rendered artifacts in `<run-dir>/` on the login node.
- If submitted: job id and the monitoring commands from `xfer-slurm-submit` Step 4 (`squeue`, `sacct`, logs, state markers).

## Safety

- Never overwrite an existing `run-dir` without confirming — `xfer run` will re-run build and clobber `manifest.jsonl`.
- If `--submit` is set, this skill consumes real cluster resources on the spot. Confirm before running.
- If the user wants to inspect the sbatch script before it runs, run **without** `--submit`, show them `<run-dir>/sbatch_array.sh`, then invoke `xfer-slurm-submit` separately.

## After this skill

If `--submit` was used, direct the user to the monitoring commands from `xfer-slurm-submit` Step 4. If not, `xfer-slurm-submit` is next.
