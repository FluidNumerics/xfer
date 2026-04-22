---
name: xfer-manifest-shard
description: Split an xfer manifest into byte-balanced shards for parallel transfer. Use after analyze and before slurm render, whenever the user wants to shard a manifest or asks how many array tasks the transfer should use.
---

# xfer-manifest-shard

Drives `xfer manifest shard` — splits `run/manifest.jsonl` into `run/shards/shard_*.jsonl` + `shards.meta.json`.

## Operating model

Runs **locally on the workstation**. Pure file processing. No Slurm/SSH needed.

## Step 1 — Read the analyze output

Read `run/analyze.json` (from `xfer-manifest-analyze`) and use `suggested_shard_count` directly as the shard count. The analyzer already factors in the 10 TiB/shard cap, the expected array concurrency, and (if supplied) the core budget.

If `run/analyze.json` doesn't exist yet, invoke `xfer-manifest-analyze` first — don't guess shard counts from the raw manifest.

## Step 2 — Decide whether to override

Only override `suggested_shard_count` if one of the inputs that fed it has changed since analyze ran:

- The transfer cluster is different from what analyze assumed (different core budget).
- The array concurrency cap is different from what analyze assumed (defaults: `--assumed-array-concurrency=64`).
- The user wants a different per-shard byte cap (default 10 TiB).

In that case, **re-run `xfer-manifest-analyze`** with updated `--assumed-*` flags rather than hand-picking a new number here. Sharing the reasoning/assumptions via `run/analyze.json` is how downstream skills stay coherent.

Show the user `suggested_shard_count` alongside `shard_count_reasoning` from analyze, then confirm before running.

## Step 3 — Run shard

```bash
uv run xfer manifest shard \
  --in     run/manifest.jsonl \
  --outdir run/shards \
  --num-shards <N> \
  --strategy bytes
```

`bytes` (the default) is almost always right — it minimizes long-tail tasks. Use `count` only if the user explicitly wants equal object counts per shard, or `hash` for deterministic assignment by key.

## Step 4 — Report

After the command completes, read `run/shards/shards.meta.json` and report:
- Number of shards created
- Min / max / median shard bytes (to show balance quality)
- Total object count

If max/min bytes ratio is > 3x, warn the user — the manifest may have very large individual objects that a byte-balanced greedy bin-pack can't split. Suggest bumping shard count or splitting the outlier files out into a separate run.

## After this skill

Recommend `xfer-slurm-render`. If the transfer cluster has a different view of source/dest than the build host, recommend `xfer-manifest-rebase` first.
