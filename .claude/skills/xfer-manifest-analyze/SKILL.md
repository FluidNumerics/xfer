---
name: xfer-manifest-analyze
description: Analyze an xfer manifest's file-size distribution and suggest rclone flags plus a shard count. Use after `xfer manifest build` and before sharding/rendering, whenever the user asks "how should I tune rclone?", "how many shards?", or wants to understand the dataset shape before transferring.
---

# xfer-manifest-analyze

Drives `xfer manifest analyze` — reads `run/manifest.jsonl` (produced by `xfer-manifest-build`) and writes a histogram + suggested rclone flags to `run/analyze.json`.

## Operating model

Runs **locally on the workstation** — no cluster access needed. Pure file processing over the JSONL manifest.

## Step 1 — Locate the manifest

Default: `run/manifest.jsonl` at the repo root. If the user has a different path or multiple runs under `run_*/`, ask which to analyze.

## Step 2 — Run analyze

```bash
uv run xfer manifest analyze \
  --in  run/manifest.jsonl \
  --out run/analyze.json
```

Optionally, if the user already has a preferred base set of rclone flags they want to layer on top, pass `--base-flags "<flags>"`.

## Step 3 — Report

Read `run/analyze.json` and report to the user:

1. **Dataset shape**: total object count, total bytes, median size, p90/p99 sizes, and the histogram bin counts (power-of-2 edges).
2. **Profile classification**: which profile the analyzer picked (`small_files`, `large_files`, or `mixed`) and the reasoning (e.g., ">70% of objects are under 1 MiB").
3. **Suggested rclone flags**: the concrete string to pass to `--rclone-flags` for render. Typical examples:
   - small_files → `--transfers 64 --checkers 128 --fast-list`
   - large_files → `--transfers 16 --checkers 32 --buffer-size 256M`
   - mixed       → `--transfers 32 --checkers 64 --fast-list`
4. **Suggested shard count**: derive from total object count / target objects-per-shard (aim for ~10k–50k objects per shard for small-file workloads, smaller for large-file). Cap at what the chosen transfer cluster can reasonably host in a job array. If you don't yet know the transfer cluster, give a range and defer to `xfer-manifest-shard`.

## Step 4 — Persist for downstream skills

`run/analyze.json` is the source of truth for flag/shard decisions. `xfer-manifest-shard` and `xfer-slurm-render` both read it. Don't re-derive flags by hand in those skills — point at this file.

## After this skill

Recommend `xfer-manifest-shard` next.
