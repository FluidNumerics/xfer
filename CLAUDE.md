# CLAUDE.md — xfer

`xfer` orchestrates large S3↔S3 (and POSIX↔S3) data transfers via rclone-in-a-container, Slurm job arrays, and pyxis/enroot. Manifest-driven, sharded, resumable.

## Mental model

The **user's local workstation** is the orchestrator. Long-running and compute-bound work happens on **Slurm clusters** reached via SSH/scp to login nodes. Commands like `uv run xfer manifest build` that invoke `srun` must run on a login node, not the workstation. `analyze`, `shard`, `rebase`, and `render` are pure file processing and run locally.

## Cross-cutting invariants

- **Paths are per-system.** `--rclone-config`, the xfer repo path, and the run directory all differ between workstation, build cluster, and transfer cluster. Never assume the workstation's path resolves on the cluster. Always ask, verify with `ssh ... test -f`, or consult `run/config.resolved.json`.
- **POSIX-first for manifest build.** If any Slurm cluster has a POSIX mount of the source bucket, build the manifest there against the POSIX path. Listing is latency-bound; POSIX beats S3 by a large margin.
- **CPU-only, load-aware for transfer.** Prefer CPU-only partitions for both build and transfer. Pick the transfer cluster by current load (`sinfo`/`squeue` on candidate login nodes), not habit.
- **Vantage change ⇒ rebase.** If the manifest was built on a host whose view of source/dest differs from the transfer host's view, run `xfer manifest rebase` and re-shard before render. Skipping this makes every array task fail identically.
- **Credential hygiene.** rclone.conf is `0600` everywhere it lives. Never commit it (`.gitignore` already excludes it). Never echo secrets back to the user. Confirm before transmitting credentials over scp.
- **Confirm before cluster-side actions.** SSH probes and `rsync` of local files are fine. `sbatch`, `scontrol update`, `scancel`, and any write to a shared path require explicit user confirmation.

## Workflow skills

Use these in order; each `Skill` trigger runs the corresponding stage. Invoke explicitly when the user's intent matches.

| Skill                     | Stage                                                                  |
| ------------------------- | ---------------------------------------------------------------------- |
| `xfer-rclone-config`      | Create rclone.conf, deploy to each cluster that will run xfer jobs     |
| `xfer-manifest-build`     | Run `xfer manifest build` on a login node (POSIX-preferred source)     |
| `xfer-manifest-analyze`   | File-size histogram → suggested rclone flags + shard count             |
| `xfer-manifest-shard`     | Byte-balanced split of the manifest into `run/shards/`                 |
| `xfer-manifest-rebase`    | Remap source/dest roots when vantage changes; re-shard after           |
| `xfer-slurm-render`       | Render `worker.sh` / `sbatch_array.sh` / `config.resolved.json`        |
| `xfer-slurm-submit`       | Stage run dir to transfer cluster, `sbatch`, return monitoring cmds    |

## Dev conventions

- Python ≥ 3.10, managed with `uv` (`uv venv && uv sync`; `uv run xfer --help`).
- Formatting: `uv run black .`; pre-commit hook available via `uv run pre-commit install`.
- Branch names: `<name>/<branch>` or `<type>/<branch>` where type ∈ {feature, patch, docs}.
- Do **not** squash-merge PRs.
