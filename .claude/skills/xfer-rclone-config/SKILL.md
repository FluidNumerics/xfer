---
name: xfer-rclone-config
description: Create or extend an rclone.conf for xfer — collect S3 endpoints and credentials for each remote (source, destination, optional staging), write the config with 0600 permissions, optionally test it, and guide the user through deploying it to each Slurm cluster that will run xfer jobs. Use whenever the user needs to set up rclone remotes, is bootstrapping a new transfer, or an existing skill reports a missing/incomplete rclone.conf.
---

# xfer-rclone-config

Authors an rclone.conf suitable for xfer. The config is consumed by **containerized rclone** inside Slurm jobs, so it must be:

1. Readable on the workstation (convenient for local testing).
2. Present at a known absolute path on **every Slurm cluster** that will run any stage of xfer (`xfer manifest build` and the transfer array).
3. Restricted to `0600` permissions — it contains S3 secret keys.

The authoritative template is `rclone.conf.example` at the repo root.

## Critical invariant — paths are per-system

The `--rclone-config` flag always takes an **absolute path on whichever host the xfer command runs on**. That host differs between stages:

- `xfer manifest build` — path on the **build cluster's login node**
- `xfer slurm render` — the path baked into `sbatch_array.sh` must be valid on the **transfer cluster's compute nodes**
- Local `uv run xfer analyze/shard/rebase` — path on the **workstation** (only if the user wants to sanity-check remotes locally via host rclone)

A single workstation config does not automatically work on the cluster. **Always ask for and record the path per system.** Do not assume `~/.config/rclone/rclone.conf` on the workstation resolves the same way on the cluster — home directories, shared scratch, and site-standard locations (e.g., `/etc/rclone/rclone.conf`) all differ.

## Step 1 — Inventory what's needed

Ask the user:

1. **Which remotes?** At minimum `source` and `destination`. Some transfers also need a staging remote. Each remote needs a short config-section name (e.g., `s3-src`, `s3-dest`, `weka-s3`).
2. **Per remote**: endpoint URL, access key ID, secret access key, provider (Other / AWS / Wasabi / Ceph / Minio / etc.), region (AWS only), and whether path-style addressing is required (true for most on-prem / VAST / Weka / Minio; false for AWS).
3. **Remote naming consistency across clusters.** If the same bucket will be referenced from more than one cluster, encourage the user to use **the same remote name everywhere** — this avoids needing `xfer-manifest-rebase` later. If a cluster has a POSIX mount of the same bucket, that's fine too; the build skill can point at the POSIX path directly on that cluster.
4. **Target file path on the workstation.** Default `~/.config/rclone/rclone.conf`. If the file already exists, default to **appending** new remotes rather than overwriting — confirm before touching existing sections.

Collect secrets **one at a time** and do not echo them back in your user-facing text. When running the write step, never log the secret values.

## Step 2 — Write the config

For each remote, emit a section following the project's template style (`rclone.conf.example`):

```ini
[<remote-name>]
type = s3
provider = <Other|AWS|Wasabi|Ceph|Minio>
access_key_id = <key>
secret_access_key = <secret>
endpoint = <https://...>
region = <region-if-aws>
force_path_style = <true|false>
no_check_bucket = true
```

Notes:
- `force_path_style = true` for on-prem / VAST / Weka / Minio / Ceph. Leave unset for AWS.
- `no_check_bucket = true` is safe for xfer — workers assume the bucket exists; skipping the probe is faster at high concurrency.
- For AWS, set `region` and omit `endpoint`.

Write the file, then immediately lock it down:

```bash
chmod 600 <path>
```

If appending to an existing file, confirm with the user that no section-name collisions exist before writing; if a section already exists, ask whether to overwrite or pick a different name.

## Step 3 — Smoke-test (optional, workstation-local)

If the user has `rclone` installed locally, offer to run:

```bash
rclone --config <path> listremotes
rclone --config <path> lsd <remote-name>: --max-depth 1
```

The `lsd` probe validates credentials and endpoint reachability. If `rclone` is not installed on the workstation, skip — the real validation happens on the cluster at job time anyway.

## Step 4 — Deploy to each Slurm cluster

For **every cluster** that will run any xfer stage:

1. Ask the user the **absolute path** on that cluster where rclone.conf should live (e.g., `~/.config/rclone/rclone.conf`, `/home/<user>/xfer/rclone.conf`, or a site-provided shared location).
2. Check whether the cluster already has one at that path:

   ```bash
   ssh <user>@<login-node> 'test -f <cluster-path> && echo PRESENT || echo MISSING'
   ```

3. If missing, copy it over (**confirm with the user first — this transmits credentials**):

   ```bash
   scp <workstation-path> <user>@<login-node>:<cluster-path>
   ssh <user>@<login-node> 'chmod 600 <cluster-path>'
   ```

4. Record the `(cluster, path)` pairs in the current conversation so `xfer-manifest-build`, `xfer-slurm-render`, and `xfer-slurm-submit` can use the correct path per system. Example shape:

   ```
   workstation                → ~/.config/rclone/rclone.conf
   alpha-login.example.com    → /home/joe/.config/rclone/rclone.conf
   beta-login.example.com     → /shared/rclone/joe.conf
   ```

## Step 5 — Remind about credential hygiene

Tell the user, explicitly:

- The file must be `0600` on every host it lives on.
- If credentials rotate, all copies must be updated — xfer does not re-fetch.
- Do not commit rclone.conf to the repo (`.gitignore` already excludes it; confirm if unsure).
- If a cluster has a site-managed shared rclone.conf (e.g., admin-provisioned at `/etc/rclone/rclone.conf`), prefer that and skip the scp step — don't duplicate credentials.

## After this skill

If the user is starting fresh, continue to `xfer-manifest-build`. If they just added a remote for a new cluster, they're good to re-run whichever stage prompted this detour.
