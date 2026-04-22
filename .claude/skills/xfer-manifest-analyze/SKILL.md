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

Optional flags that tune the **shard-count suggestion** (not the rclone flag suggestion):

| Flag                            | Default | What it does                                                                |
| ------------------------------- | ------- | --------------------------------------------------------------------------- |
| `--assumed-cpus-per-task`       | `4`     | Cores each worker will request. Matches `xfer slurm render` default.        |
| `--assumed-array-concurrency`   | `64`    | Expected Slurm array concurrency. Matches `xfer slurm render` default.      |
| `--assumed-core-budget`         | unset   | Total cores the partition will make available (supply from `sinfo`).         |
| `--max-shard-bytes-tb`          | `10`    | Per-shard byte cap. No single shard should carry more than this.             |
| `--base-flags "<flags>"`        | —       | Prepend the user's preferred rclone flags to the suggested ones.             |

If the user already knows the transfer cluster's available core budget, pass it — the shard-count suggestion will be sharper. Otherwise the default (concurrency + bytes-only) is fine.

## Step 3 — Report

Read `run/analyze.json` and report to the user:

1. **Dataset shape**: total object count, total bytes, median size, p10/p90 sizes, and the histogram bin counts (power-of-2 edges).
2. **Profile classification**: which profile the analyzer picked (`small_files`, `large_files`, or `mixed`) and the reasoning (e.g., ">70% of objects are under 1 MiB").
3. **Suggested rclone flags** (`suggested_flags`): the concrete string to pass to `--rclone-flags` for render. Typical examples:
   - small_files → `--transfers 64 --checkers 128 --fast-list`
   - large_files → `--transfers 16 --checkers 32 --buffer-size 256M`
   - mixed       → `--transfers 32 --checkers 64 --fast-list`
4. **Suggested shard count** (`suggested_shard_count`, plus `shard_count_reasoning` and `shard_count_assumptions`). The heuristic:
   - If `total_bytes` is below the per-shard cap (default 10 TiB), **1 shard** — don't shard small datasets.
   - Otherwise `ceil(total_bytes / cap)` shards, upper-bounded by `4 × array_concurrency` and (if a core budget was supplied) `core_budget // cpus_per_task`.

   Quote `shard_count_reasoning` verbatim back to the user so they can see the trade-offs.

## Step 4 — Persist for downstream skills

`run/analyze.json` is the source of truth for flag/shard decisions. `xfer-manifest-shard` reads `suggested_shard_count` and `xfer-slurm-render` reads `suggested_flags` — point at this file, don't re-derive.

If the user's plan changes (different transfer cluster, different concurrency cap), re-run `xfer manifest analyze` with updated `--assumed-*` flags before calling `xfer-manifest-shard`.

## After this skill

Recommend `xfer-manifest-shard` next.
