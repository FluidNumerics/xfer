---
name: xfer-manifest-rebase
description: Remap an xfer manifest's source/dest roots when the transfer host has a different view than the manifest build host (e.g., manifest built over POSIX `/mnt/data/x`, transfer runs via S3 `weka-s3:x`). Use whenever the vantage point changes between manifest build and transfer — this MUST run before render/submit or the transfer will fail.
---

# xfer-manifest-rebase

Drives `xfer manifest rebase` — rewrites the source/dest roots in `run/manifest.jsonl` so the manifest is valid from the transfer host's perspective.

## When to run this

**Trigger condition**: the host that will execute the transfer has a different view of either the source or the destination than the host that built the manifest. Common cases:

- Manifest built on a cluster with POSIX mount (`/mnt/data/dataset`); transfer runs on a different cluster that only sees the bucket as an rclone remote (`weka-s3:dataset`).
- Source built via one rclone remote alias; transfer host's rclone.conf uses a different alias for the same bucket.
- Dest root changed (e.g., adding a subprefix) after build.

If the transfer host sees source and dest identically to the build host, **do not rebase** — it's a no-op that wastes a pass over the manifest.

## Step 1 — Determine the mismatch

Ask the user (or look at prior conversation / `run/manifest.jsonl`'s header):

1. What were the `source_root` and `dest_root` recorded in the manifest? (Peek at the first line of `run/manifest.jsonl`.)
2. What does the transfer host see as the source and dest? (Usually rclone remote names; confirm with `rclone listremotes` on that host.)

Show the user the proposed before/after mapping and confirm before proceeding.

## Step 2 — Run rebase

```bash
uv run xfer manifest rebase \
  --in          run/manifest.jsonl \
  --out         run/manifest.rebased.jsonl \
  --source-root <new-source-root> \
  --dest-root   <new-dest-root>
```

Always write to a new file (don't overwrite `manifest.jsonl`). Keeping the original manifest as a record of the build vantage point is useful for debugging and audits.

## Step 3 — Re-shard

Sharding is derived from the manifest, so **re-shard after rebasing**:

```bash
rm -rf run/shards
uv run xfer manifest shard \
  --in     run/manifest.rebased.jsonl \
  --outdir run/shards \
  --num-shards <same-N-as-before>
```

(Or invoke `xfer-manifest-shard`.) Byte balance won't change meaningfully, but the shard files need to carry the rebased paths or workers will try to copy from the wrong URI.

## Step 4 — Point downstream skills at the rebased manifest

When you invoke `xfer-slurm-render` next, pass `--run-dir run` but ensure `config.resolved.json` references the rebased manifest. If the user plans a fresh `xfer slurm render`, that's automatic (render reads from `run/shards/`).

## Safety

- Never delete the original manifest — always keep `run/manifest.jsonl` as an audit trail alongside `run/manifest.rebased.jsonl`.
- Rebase is a remap, not a content migration. It does not move data. It only relabels what each shard points to.

## After this skill

Recommend `xfer-slurm-render` (or re-shard first if you didn't in step 3).
