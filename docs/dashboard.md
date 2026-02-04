# xfer Web Dashboard

The xfer web dashboard provides a browser-based interface for managing data transfer jobs on Slurm clusters. Users can create transfers, monitor progress, view logs, and cancel jobs without using the command line.

## Features

- **Google Workspace SSO** - Authenticate users via Google OAuth with domain restrictions
- **Simplified Job Creation** - Users select source/destination from pre-configured remotes
- **Admin-Controlled Settings** - Shard count, concurrency, partitions, and resource limits are pre-configured
- **Real-Time Progress** - Monitor shard completion with automatic polling
- **Log Streaming** - View manifest and shard logs directly in the browser
- **Job Cancellation** - Cancel running jobs from the dashboard

## Prerequisites

### 1. Google Cloud OAuth Credentials

The dashboard uses Google Workspace SSO for authentication. You'll need to create OAuth 2.0 credentials:

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Navigate to **APIs & Services > Credentials**
4. Click **Create Credentials > OAuth client ID**
5. Select **Web application**
6. Add authorized redirect URI: `https://your-dashboard-url/auth/callback`
7. Save the **Client ID** and **Client Secret**

### 2. rclone Configuration

Ensure you have an `rclone.conf` file with your S3 remotes configured:

```ini
[s3-source]
type = s3
provider = Other
endpoint = https://s3.source.example.com
access_key_id = YOUR_ACCESS_KEY
secret_access_key = YOUR_SECRET_KEY

[s3-dest]
type = s3
provider = Other
endpoint = https://s3.dest.example.com
access_key_id = YOUR_ACCESS_KEY
secret_access_key = YOUR_SECRET_KEY
```

### 3. Slurm Cluster Access

The dashboard must run on a node with:
- Access to `sbatch`, `scancel`, and `sacct` commands
- Network access to submit jobs to the Slurm controller
- Read access to the rclone.conf file
- Write access to the run base directory

## Installation

Install xfer with the dashboard extras:

```bash
pip install "xfer[dashboard]"
```

Or install from source:

```bash
cd /path/to/xfer
pip install -e ".[dashboard]"
```

## Configuration

Create a configuration file at one of these locations (checked in order):

1. Path specified by `XFER_DASHBOARD_CONFIG` environment variable
2. `/etc/xfer/dashboard.yaml`
3. `~/.config/xfer/dashboard.yaml`
4. `./dashboard.yaml` (current directory)

### Configuration Reference

```yaml
# Schema version (required)
schema: xfer.dashboard.config.v1

# Authentication settings
auth:
  google:
    # OAuth credentials from Google Cloud Console
    client_id: "YOUR_CLIENT_ID.apps.googleusercontent.com"
    client_secret: "YOUR_CLIENT_SECRET"

    # Only allow users from these email domains
    allowed_domains:
      - "yourcompany.com"
      - "subsidiary.org"

  session:
    # Secret key for signing session cookies
    # Generate with: python -c "import secrets; print(secrets.token_hex(32))"
    secret_key: "your-64-character-hex-string"

    # Session cookie name
    cookie_name: "xfer_session"

    # Session lifetime in seconds (default: 24 hours)
    max_age_seconds: 86400

# Database settings
database:
  # SQLite (development)
  url: "sqlite+aiosqlite:///var/lib/xfer/dashboard.db"

  # PostgreSQL (production)
  # url: "postgresql+asyncpg://user:password@localhost/xfer"

# Slurm job settings (admin-controlled, not user-editable)
slurm:
  # Container image containing rclone
  rclone_image: "rclone/rclone:latest"

  # Path to rclone.conf on the cluster (mounted into containers)
  rclone_config: "/shared/config/rclone/rclone.conf"

  # Number of shards for parallel transfers
  num_shards: 256

  # Maximum concurrent Slurm array tasks
  array_concurrency: 64

  # Available partitions (first is default)
  partitions:
    - "transfer"
    - "standard"

  # Resource limits per task
  cpus_per_task: 4
  mem: "8G"
  time_limit: "24:00:00"
  max_attempts: 5

  # rclone copy flags
  rclone_flags: "--transfers 32 --checkers 64 --fast-list --retries 10 --low-level-retries 20"

  # Extra pyxis flags (optional)
  pyxis_extra: ""

# Path restrictions
paths:
  # Base directory for job run directories
  # Each job creates: {run_base_dir}/xfer_{tag}_{run_id}/
  run_base_dir: "/scratch/xfer-runs"

  # Allowed source/destination path prefixes
  # Users can only select paths matching these patterns
  allowed_prefixes:
    - "s3-source:"    # Allow s3-source remote
    - "s3-dest:"      # Allow s3-dest remote
    - "/scratch/"     # Allow local /scratch filesystem

# Server settings
server:
  host: "0.0.0.0"
  port: 8000

  # Public URL (must match Google OAuth redirect URI)
  base_url: "https://xfer.yourcompany.com"

  # Enable debug mode (disables HTTPS-only cookies)
  debug: false
```

## Running the Dashboard

### Development

```bash
# Set config path (optional)
export XFER_DASHBOARD_CONFIG=/path/to/dashboard.yaml

# Run with the built-in server
xfer-dashboard
```

The dashboard will be available at `http://localhost:8000`.

### Production with Gunicorn

For production deployments, use Gunicorn with Uvicorn workers:

```bash
pip install gunicorn

gunicorn xfer.dashboard.main:create_app \
    --worker-class uvicorn.workers.UvicornWorker \
    --workers 4 \
    --bind 0.0.0.0:8000
```

### Production with systemd

Create `/etc/systemd/system/xfer-dashboard.service`:

```ini
[Unit]
Description=xfer Dashboard
After=network.target

[Service]
Type=exec
User=xfer
Group=xfer
Environment=XFER_DASHBOARD_CONFIG=/etc/xfer/dashboard.yaml
ExecStart=/usr/local/bin/gunicorn xfer.dashboard.main:create_app \
    --worker-class uvicorn.workers.UvicornWorker \
    --workers 4 \
    --bind 127.0.0.1:8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable xfer-dashboard
sudo systemctl start xfer-dashboard
```

### Reverse Proxy (nginx)

Place behind nginx for TLS termination:

```nginx
server {
    listen 443 ssl http2;
    server_name xfer.yourcompany.com;

    ssl_certificate /etc/ssl/certs/xfer.crt;
    ssl_certificate_key /etc/ssl/private/xfer.key;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # SSE support for log streaming
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 86400s;
    }
}
```

## Architecture

### Two-Stage Job Submission

The dashboard uses a two-stage approach to keep all computation off the login node:

```
┌─────────────────┐
│   Dashboard     │
│  (Login Node)   │
└────────┬────────┘
         │ sbatch manifest_job.sh
         ▼
┌─────────────────┐
│  Manifest Job   │  Stage 1: Single Slurm job
│  (Compute Node) │
│                 │
│  1. xfer manifest build
│  2. xfer manifest shard
│  3. xfer slurm render
│  4. sbatch sbatch_array.sh ──────┐
└─────────────────┘                │
                                   ▼
                    ┌─────────────────────────┐
                    │    Transfer Array Job   │  Stage 2: Array job
                    │    (Compute Nodes)      │
                    │                         │
                    │  Task 0: shard_000000   │
                    │  Task 1: shard_000001   │
                    │  ...                    │
                    │  Task N: shard_NNNNNN   │
                    └─────────────────────────┘
```

### Job Status Flow

```
pending
    │
    ▼ (dashboard submits manifest job)
manifest_queued
    │
    ▼ (Slurm starts manifest job)
manifest_running
    │
    ├──▶ manifest_failed (on error)
    │
    ▼ (manifest job completes, submits array)
manifest_done
    │
    ▼
transfer_queued
    │
    ▼ (Slurm starts array tasks)
transfer_running
    │
    ├──▶ failed (too many shard failures)
    │
    ▼ (all shards complete)
completed
```

### Run Directory Structure

Each job creates a run directory:

```
/scratch/xfer-runs/xfer_{tag}_{run_id}/
├── manifest_job.sh          # Submitted by dashboard
├── manifest.out             # Manifest job stdout
├── manifest.err             # Manifest job stderr
├── manifest.jsonl           # Object listing
├── transfer_job_id.txt      # Transfer array job ID
├── worker.sh                # Per-shard worker script
├── sbatch_array.sh          # Array job submission script
├── config.resolved.json     # Frozen configuration
├── shards/
│   ├── shard_000000.jsonl
│   ├── shard_000001.jsonl
│   ├── ...
│   └── shards.meta.json     # Shard metadata
├── state/
│   ├── shard_0.done         # Empty file = shard complete
│   ├── shard_0.attempt      # Attempt count
│   ├── shard_5.fail         # Exit code on failure
│   └── ...
└── logs/
    ├── shard_0_attempt_1.log
    ├── shard_5_attempt_1.log
    ├── shard_5_attempt_2.log  # Retry log
    └── ...
```

## API Reference

### HTML Pages

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Dashboard home (redirects to /jobs) |
| `/login` | GET | Login page |
| `/jobs` | GET | Job list page |
| `/jobs/new` | GET | Job creation form |
| `/jobs/{id}` | GET | Job detail page |

### Authentication

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/auth/login` | GET | Initiate Google OAuth flow |
| `/auth/callback` | GET | OAuth callback (redirect URI) |
| `/auth/logout` | GET | Log out and clear session |

### Job Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/jobs` | POST | Create new transfer job |
| `/jobs/{id}/cancel` | POST | Cancel a running job |
| `/api/jobs` | GET | List jobs (JSON) |
| `/api/jobs/{id}` | GET | Get job details (JSON) |

### Remote Discovery

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/remotes` | GET | List available remotes and local prefixes |
| `/api/remotes/sources` | GET | List source options for form dropdowns |

### Log Access

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/logs/{job_id}/manifest` | GET | Stream manifest log (SSE) |
| `/api/logs/{job_id}/manifest/content` | GET | Get manifest log content |
| `/api/logs/{job_id}/shard/{shard_id}` | GET | Get shard log content |
| `/api/logs/{job_id}/shards` | GET | List available shard logs |

### HTMX Partials

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/htmx/jobs/table` | GET | Jobs table (for polling) |
| `/htmx/jobs/{id}/progress` | GET | Progress bar (for polling) |

### Health Check

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check (returns `{"status": "ok"}`) |

## Security Considerations

### Path Validation

All user-provided paths are validated against `allowed_prefixes` to prevent:
- Access to unauthorized S3 buckets
- Access to unauthorized filesystem paths
- Directory traversal attacks

### Shell Injection Prevention

All user inputs interpolated into shell scripts use `shlex.quote()` for proper escaping.

### Session Security

- Sessions are signed with HMAC using the configured secret key
- Cookies are `httponly` (not accessible to JavaScript)
- Cookies use `samesite=lax` to prevent CSRF
- In production (`debug: false`), cookies require HTTPS

### Log Access Control

Log file paths are validated to ensure they're within the job's run directory, preventing directory traversal.

### Domain Restriction

Google OAuth domain restriction is enforced server-side. Only users with email addresses from `allowed_domains` can authenticate.

## Troubleshooting

### "No dashboard configuration file found"

Create a config file at one of the expected locations or set `XFER_DASHBOARD_CONFIG`.

### OAuth redirect URI mismatch

Ensure `server.base_url` in your config matches the redirect URI configured in Google Cloud Console. The callback URL is `{base_url}/auth/callback`.

### "Domain not allowed" error

The user's email domain is not in `auth.google.allowed_domains`. Add their domain to the list.

### Jobs stuck in "manifest_queued"

Check that:
1. The dashboard host can reach the Slurm controller
2. The user running the dashboard has permission to submit jobs
3. The partition specified exists and is accessible

### Progress not updating

The dashboard polls the state directory every 30 seconds. Check that:
1. The run directory is accessible from the dashboard host
2. The worker jobs are writing state files correctly

### Log streaming not working

If behind a reverse proxy, ensure:
1. Proxy buffering is disabled
2. The proxy timeout is sufficient for long-lived SSE connections
