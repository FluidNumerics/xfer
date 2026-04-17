---
name: xfer-manifest-shard
description: Split an xfer manifest into byte-balanced shards for parallel transfer. Use after analyze and before slurm render, whenever the user wants to shard a manifest or asks how many array tasks the transfer should use.
---

# xfer-manifest-shard

Drives `xfer manifest shard` — splits `run/manifest.jsonl` into `run/shards/shard_*.jsonl` + `shards.meta.json`.

## Operating model

Runs **locally on the workstation**. Pure file processing. No Slurm/SSH needed.

## Step 1 — Read the analyze output

Read `run/analyze.json` (from `xfer-manifest-analyze`). Use its `suggested_shard_count` / profile as the starting point. If analyze hasn't been run yet, invoke `xfer-manifest-analyze` first — don't guess shard counts from the raw manifest.

## Step 2 — Reconcile shard count with cluster resources

The right shard count depends on **both** rclone settings (from analyze) **and** the transfer cluster's available resources. Ask the user:

1. Which cluster will run the transfer? (Same as build host, or different?)
2. What's the target array concurrency — how many shards should run at once? Typical range: 32–256, capped by the partition's `MaxArraySize` and the throughput both S3 endpoints can handle.
3. What's the partition's per-node core/memory budget?

Rule of thumb:
- Total shards ≈ max(suggested_shard_count_from_analyze, 4 × array_concurrency). This gives the scheduler enough slack to keep the array fully packed even as slow shards trail.
- For small-files profiles (heavy listing, light bytes), bias toward **more shards** of fewer objects each.
- For large-files profiles (heavy bytes, few objects), bias toward **fewer shards** with more bytes each — byte-balancing matters more than object count.

State your recommendation with the reasoning, then confirm before running.

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
