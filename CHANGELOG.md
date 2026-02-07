# Changelog

## Unreleased (feature/slack-claude)

### Parallel Manifest Build

- Parallelize manifest generation into up to 4 concurrent `rclone lsjson` workers, reducing listing time for large datasets with many top-level subdirectories (`slurm_tools.py`)
- Add `manifest combine` CLI command to merge parallel lsjson part files (with `.prefix` sidecars) into a unified `manifest.jsonl` (`cli.py`)
- Bump prepare job memory from 16 GB to 250 GB to accommodate large listings (`slurm_tools.py`)
- Add `--max-backlog=1000000` to `rclone lsjson` calls to prevent the walker from stalling on large buckets (`cli.py`, `slurm_tools.py`)
- Report manifest build progress (files listed, bytes listed) via `manifest.jsonl.progress` sidecar file (`cli.py`)
- Track prepare job phases (`listing_source`, `combining_manifest`, `analyzing`, `sharding`, `rendering`, `submitting`) in `progress.json` (`slurm_tools.py`)

### Claude-Powered Slack Bot

- Add Claude-powered Slack bot for interactive data transfer requests via Slack threads
- Add intelligent rclone flag selection based on file size distribution analysis
- Add `check_path_exists` tool to validate source paths before submitting jobs
- Add `list_buckets` tool to enumerate buckets at remote endpoints
- Add `read_job_logs` tool to access job analysis data, prepare logs, and shard transfer logs
- Add lightweight Haiku triage to filter thread messages and skip unrelated chatter
- Add per-user job ownership so only the submitting user can cancel their jobs
- Restore thread context from Slack API after bot restarts
- Report manifest listing progress (files_listed, bytes_listed) in job status during `building_manifest` phase

### Slurm Robustness

- Increase prepare job time limit to 4 days for very large datasets
- Unset conflicting `SLURM_MEM_*` environment variables in prepare.sh
- Add `--export=NONE` to all `sbatch` calls to prevent environment leakage
- Allow users to set lower array concurrency (max 64)
- Verify source path exists before creating run directory and submitting jobs
- Reduce thread history limits to mitigate Slack rate limits

### Bug Fixes

- Fix `run_id` format to be filename-safe (no colons)
- Fix markdown rendering in Slack responses
- Improve error logging for manifest build failures (write to `xfer-err/` with full context)
