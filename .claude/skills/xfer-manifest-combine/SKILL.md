---
name: xfer-manifest-combine
description: Combine multiple `rclone lsjson` part files into a single xfer JSONL manifest. Use this instead of `xfer-manifest-build` when the source is too large for a single `rclone lsjson` call and the user has already produced parallel listings (one JSON-array file per top-level prefix, with a `.prefix` sidecar naming the prefix). Produces the same `xfer.manifest.v1` schema the rest of the pipeline consumes.
---

# xfer-manifest-combine

Drives `xfer manifest combine` — an alternative entry point to the pipeline when `xfer manifest build`'s single-shot listing is too slow or too large to buffer. Combines per-prefix lsjson outputs into one `manifest.jsonl`.

## When to use this instead of `xfer-manifest-build`

Use `xfer-manifest-build` by default. Reach for combine when **all** of the following are true:

- The source has so many objects that a single `rclone lsjson` call would OOM or run for days.
- The user (or a previous job) has already produced per-prefix lsjson outputs in a directory.
- Each part file is a JSON array (rclone's native lsjson format), and each has a sibling `.prefix` file naming the prefix that was listed.

If only the first condition is true and there are no parts yet, `xfer-manifest-build` is simpler — running parallel lsjson jobs just to feed combine is out of scope for this skill; the user should do that with their own orchestration.

## Operating model

Runs **locally on the workstation** if the parts dir is accessible locally, otherwise on whichever host can see the part files. Pure file processing — no Slurm, no SSH.

## Step 1 — Verify the parts directory layout

Each part file must follow this pattern:

```
parts/
├── lsjson-0001.json      # JSON array: `rclone lsjson <remote>:<bucket>/<prefix-1> --recursive`
├── lsjson-0001.prefix    # text file containing the literal prefix, e.g. "prefix-1"
├── lsjson-0002.json
├── lsjson-0002.prefix
...
```

Quick probe:

```bash
ls <parts-dir>/lsjson-*.json | head
ls <parts-dir>/lsjson-*.prefix | head
```

If a `.prefix` sidecar is missing for any part, combine will use an empty prefix for that part — the resulting `path` fields will be bucket-relative, which is usually **not** what you want. Flag this and confirm with the user before running.

## Step 2 — Run combine

```bash
uv run xfer manifest combine \
  --source    <rclone-source-root>   \
  --dest      <rclone-dest-root>     \
  --parts-dir <parts-dir>            \
  --out       run/manifest.jsonl
```

`--source` / `--dest` are the full roots (e.g., `s3src:bucket`) — combine prepends them to each part's prefix + object path to produce the `source`/`dest` URIs in the manifest.

`--run-id <id>` is optional; if omitted, one is generated per run.

## Step 3 — Sanity-check the manifest

```bash
wc -l run/manifest.jsonl
head -1 run/manifest.jsonl | python -m json.tool
```

Confirm:
- Line count matches the sum of non-dir entries across parts (combine's final log line reports this).
- `source_root`, `dest_root`, and the first record's `source`/`path` look right (path should start with a known prefix from the parts).

## Safety

- If `run/manifest.jsonl` already exists, confirm with the user before overwriting — combine writes unconditionally.
- If the parts dir is on a shared filesystem, treat it as read-only.

## After this skill

Continue with `xfer-manifest-analyze` exactly as you would after `xfer-manifest-build`. Downstream skills don't care whether the manifest came from build or combine — the schema is identical.
