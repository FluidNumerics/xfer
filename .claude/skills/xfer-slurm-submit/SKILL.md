---
name: xfer-slurm-submit
description: Copy a rendered xfer run directory to a Slurm login node and submit the array job via sbatch. Use after `xfer slurm render` whenever the user is ready to kick off the actual transfer. Handles workstation→cluster staging and post-submit monitoring pointers.
---

# xfer-slurm-submit

Submits the rendered Slurm array job. Assumes `xfer-slurm-render` already produced `run/worker.sh`, `run/sbatch_array.sh`, `run/shards/`, etc. on the workstation.

## Operating model

The user renders locally and submits to the **transfer cluster's login node**. This skill:
1. Copies (or syncs) the run directory to the login node.
2. SSHes in and runs `sbatch`.
3. Returns the job id and monitoring pointers.

## Step 1 — Identify the transfer cluster and paths

From `run/config.resolved.json` (or ask the user):
- Transfer cluster login node (hostname, user)
- Destination path for the run dir on the cluster (e.g., `~/xfer-runs/<run-name>`)
- Path to `rclone.conf` on the cluster (referenced from `config.resolved.json`)

Verify the rclone.conf path recorded in `run/config.resolved.json` exists on the transfer cluster — this is the path the compute nodes will see, which may differ from the workstation's path:

```bash
ssh <user>@<login-node> 'test -f <rclone-conf-path-on-cluster> && stat -c "%a" <rclone-conf-path-on-cluster>'
```

Expect `600`. If the file is missing, **stop and invoke `xfer-rclone-config`** to deploy it — that skill handles credential hygiene, 0600 permissions, and recording the per-cluster path. Don't `scp` the file directly from this skill.

If the file exists but isn't `0600`, ask the user whether to fix it (`chmod 600 <path>` via SSH) before proceeding.

## Step 2 — Stage the run directory

Use `rsync -av` so re-submits don't re-send unchanged shards:

```bash
rsync -av --exclude='logs/' --exclude='state/' \
  ./<run-dir>/ \
  <user>@<login-node>:<remote-run-dir>/
```

Exclude `logs/` and `state/` — those are produced by the running job and re-pulling them would clobber live progress.

If this is a re-submit after failure, do **not** `--delete` — preserve any `.done` markers so completed shards are skipped on replay.

## Step 3 — Submit

```bash
ssh <user>@<login-node> '
  cd <remote-run-dir> &&
  sbatch --export=NONE sbatch_array.sh
'
```

`--export=NONE` is important — it prevents the workstation's or parent job's `SLURM_*` env from leaking into the array job (per the existing project convention; see `.claude/settings.local.json`).

Capture the returned job id (e.g., `Submitted batch job 12345`).

## Step 4 — Report monitoring commands

Print back to the user — ready to paste:

```bash
# Live queue state
ssh <user>@<login-node> 'squeue -j <jobid>'

# Completed tasks summary
ssh <user>@<login-node> 'sacct -j <jobid> --format=JobID,State,Elapsed,ExitCode'

# Per-shard logs
ssh <user>@<login-node> 'ls <remote-run-dir>/logs/ | tail -20'

# Back off array concurrency live (if S3 endpoint is rate-limiting)
ssh <user>@<login-node> 'scontrol update ArrayTaskThrottle=<new-K> JobId=<jobid>'

# State markers: .done = success, .fail = last attempt failed
ssh <user>@<login-node> 'ls <remote-run-dir>/state/ | sort | uniq -c | head'
```

## Step 5 — Tell the user about resume semantics

Remind them of xfer's safety net:
- Failed shards auto-requeue up to `--max-attempts` (set at render time).
- Re-submitting the same `sbatch_array.sh` is safe — `.done` markers cause completed shards to skip.
- To fully restart, delete `run/state/` on the cluster before re-submitting.

## Safety

- **Always confirm before copying rclone.conf** — it contains credentials.
- **Always confirm the sbatch command** before running it — this is the moment real cluster resources get consumed.
- Never run `scontrol update` or `scancel` on a running job without explicit user direction.
- Do not use `rsync --delete` against a running job's run dir.

## After this skill

The pipeline is live. Direct the user to monitor via the commands printed in step 4.
