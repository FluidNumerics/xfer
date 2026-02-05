# Slack App Setup for xfer-slackbot

This guide walks through creating and configuring a Slack app for the xfer data transfer bot.

## 1. Create the Slack App

1. Go to [api.slack.com/apps](https://api.slack.com/apps)
2. Click **Create New App**
3. Choose **From scratch**
4. Enter:
   - **App Name:** `xfer-bot` (or your preferred name)
   - **Workspace:** Select your workspace
5. Click **Create App**

## 2. Enable Socket Mode

Socket Mode allows the bot to receive events without exposing a public endpoint.

1. In the left sidebar, go to **Socket Mode**
2. Toggle **Enable Socket Mode** to On
3. You'll be prompted to create an app-level token:
   - **Token Name:** `xfer-socket`
   - **Scopes:** `connections:write` (should be pre-selected)
4. Click **Generate**
5. **Copy the token** (starts with `xapp-`) — this is your `SLACK_APP_TOKEN`

## 3. Configure OAuth Scopes

1. In the left sidebar, go to **OAuth & Permissions**
2. Scroll to **Scopes** → **Bot Token Scopes**
3. Add these scopes:

| Scope | Purpose |
|-------|---------|
| `app_mentions:read` | Receive @mentions |
| `channels:history` | Read messages in public channels |
| `channels:read` | View basic channel info |
| `chat:write` | Send messages |
| `groups:history` | Read messages in private channels (if needed) |
| `groups:read` | View basic private channel info (if needed) |
| `im:history` | Read DMs with the bot |
| `im:read` | View basic DM info |
| `im:write` | Start DMs with users |
| `users:read` | View basic user info |

## 4. Subscribe to Events

1. In the left sidebar, go to **Event Subscriptions**
2. Toggle **Enable Events** to On
3. Expand **Subscribe to bot events**
4. Add these events:

| Event | Purpose |
|-------|---------|
| `app_mention` | When someone @mentions the bot |
| `message.channels` | Messages in public channels |
| `message.groups` | Messages in private channels (if needed) |
| `message.im` | Direct messages to the bot |

5. Click **Save Changes**

## 5. Configure App Home (Optional but Recommended)

1. In the left sidebar, go to **App Home**
2. Under **Show Tabs**, enable:
   - **Messages Tab** — allows DMs with the bot
3. Check **Allow users to send Slash commands and messages from the messages tab**

## 6. Install the App

1. In the left sidebar, go to **Install App**
2. Click **Install to Workspace**
3. Review the permissions and click **Allow**
4. **Copy the Bot User OAuth Token** (starts with `xoxb-`) — this is your `SLACK_BOT_TOKEN`

## 7. Get Channel IDs

You'll need channel IDs for configuration. To find them:

**Option A: From Slack UI**
1. Right-click on a channel name
2. Click **View channel details**
3. Scroll to the bottom — the Channel ID is shown (e.g., `C07ABC123`)

**Option B: Using the API**
```bash
curl -H "Authorization: Bearer xoxb-YOUR-TOKEN" \
  "https://slack.com/api/conversations.list" | jq '.channels[] | {name, id}'
```

## 8. Configure Environment Variables

Create a `.env` file or set these environment variables:

```bash
# Required
export SLACK_BOT_TOKEN="xoxb-your-bot-token"
export SLACK_APP_TOKEN="xapp-your-app-token"
export ANTHROPIC_API_KEY="sk-ant-your-api-key"

# Optional - restrict to specific channels (comma-separated channel IDs)
export XFER_ALLOWED_CHANNELS="C07ABC123,C07DEF456"

# Optional - support channel for backend requests
export XFER_SUPPORT_CHANNEL="C07SUPPORT"

# Optional - customize paths and defaults
export XFER_RUNS_DIR="/scratch/xfer-runs"
export XFER_RCLONE_CONFIG="/home/svc/.config/rclone/rclone.conf"
export XFER_RCLONE_IMAGE="rclone/rclone:latest"
export XFER_SLURM_PARTITION="transfer"
export XFER_SLURM_QOS="data-transfer"
export XFER_ALLOWED_BACKENDS_FILE="/etc/xfer/allowed_backends.yaml"
```

## 9. Run the Bot

```bash
# Install with slackbot dependencies
pip install 'xfer[slackbot]'

# Or if developing locally
cd /path/to/xfer
pip install -e '.[slackbot]'

# Run the bot
xfer-slackbot
```

You should see:
```
2026-02-04 14:30:00 - xfer.slackbot.app - INFO - Starting xfer Slack bot...
2026-02-04 14:30:00 - xfer.slackbot.app - INFO - Runs directory: /home/user/xfer-runs
2026-02-04 14:30:00 - xfer.slackbot.app - INFO - Allowed channels: ['C07ABC123']
```

## 10. Test the Bot

1. Invite the bot to your channel: `/invite @xfer-bot`
2. Mention the bot: `@xfer-bot what backends are available?`
3. Try a transfer request: `@xfer-bot transfer data from s3src:bucket/data to s3dst:archive/data`

## Bot Capabilities

### Available Tools

The bot has access to the following tools:

| Tool | Description |
|------|-------------|
| `submit_transfer` | Submit a new data transfer job |
| `check_status` | Check status of transfer jobs in the thread |
| `list_backends` | List available/allowed rclone backends |
| `list_buckets` | List buckets available at a specific backend |
| `cancel_job` | Cancel a running transfer job |
| `get_transfer_details` | Get detailed shard-level progress |
| `get_manifest_stats` | Scan a source path for file statistics |
| `check_path_exists` | Verify a bucket/path is accessible |
| `request_backend_access` | Request access to a new backend |
| `read_job_logs` | Read job logs and analysis data |

### Discover available buckets

Ask the bot what buckets exist at an endpoint:

```
@xfer-bot what buckets are available on s3src?
@xfer-bot list buckets at gcs
```

The bot will return a list of bucket names that you can use in transfer paths.

### Check data size before transferring

Ask the bot to scan a source and report statistics:

```
@xfer-bot how much data is in s3src:research/experiment1?
```

The bot will return:
- Total file count and size
- File size distribution histogram
- Suggested rclone flags based on file sizes
- Estimated transfer times

### View job logs and analysis

For running or completed jobs, ask the bot to show logs and analysis data:

```
@xfer-bot show me the file size histogram for job 12345
@xfer-bot what rclone commands were run for this transfer?
@xfer-bot show me the logs for job 12345
```

The bot can return:
- File size distribution histogram from manifest analysis
- Suggested rclone flags that were determined during setup
- Tail of prepare job stdout/stderr logs
- Extracted rclone commands that were run

### Custom rclone flags

Specify custom flags that are appended to the intelligent defaults:

```
@xfer-bot transfer s3src:data to s3dst:archive with --bwlimit 100M --checksum
```

Common options:
- `--bwlimit 100M` — Limit bandwidth
- `--checksum` — Verify files with checksums
- `--dry-run` — Test without copying

### Intelligent flag selection

The bot automatically analyzes file sizes and selects optimal rclone flags:
- **Small files** (>70% < 1MB): Higher parallelism (`--transfers 64`)
- **Large files** (>50% > 100MB): Larger buffers (`--buffer-size 256M`)
- **Mixed**: Balanced defaults

All transfers include `--stats 600s --progress` for ETA tracking.

### Thread-based conversations

The bot tracks conversations by Slack thread. Key behaviors:

- **Initial contact**: Mention the bot with `@xfer-bot` to start a conversation
- **Follow-ups**: Once the bot has responded in a thread, any user can ask follow-up questions without mentioning the bot
- **Persistent context**: The bot associates Slurm jobs with the thread they were submitted from, so asking "what's the status?" in a thread will show jobs from that thread
- **Restart resilience**: If the bot restarts, it automatically recovers thread context from Slack's API, so ongoing conversations continue working

## Running as a Service

For production, run the bot as a systemd service:

```ini
# /etc/systemd/system/xfer-slackbot.service
[Unit]
Description=xfer Slack Bot
After=network.target

[Service]
Type=simple
User=xfer-svc
Group=xfer-svc
WorkingDirectory=/opt/xfer
EnvironmentFile=/etc/xfer/slackbot.env
ExecStart=/opt/xfer/venv/bin/xfer-slackbot
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable xfer-slackbot
sudo systemctl start xfer-slackbot
sudo journalctl -u xfer-slackbot -f  # View logs
```

## Testing

### Dry-run tests (no Slack/Slurm required)

Run the dry-run tests to validate the bot logic without connecting to Slack or submitting jobs:

```bash
# Install with slackbot dependencies
pip install -e '.[slackbot]'

# Run dry-run tests
python tests/test_slackbot_dryrun.py
```

This tests:
- Slack comment format and parsing
- Configuration defaults
- Backend validation (from YAML and rclone.conf)
- Prepare script generation
- sacct JSON parsing
- Transfer progress tracking
- Claude tool definitions
- Full flow simulation

### Testing with Slack (no job submission)

To test the Slack integration without submitting real jobs, you can mock the `sbatch` command:

```bash
# Create a mock sbatch that just prints a fake job ID
mkdir -p ~/bin
cat > ~/bin/sbatch << 'EOF'
#!/bin/bash
echo "Submitted batch job 12345"
EOF
chmod +x ~/bin/sbatch

# Add to PATH before running the bot
export PATH=~/bin:$PATH
xfer-slackbot
```

## Troubleshooting

### Bot doesn't respond to mentions
- Check the bot is in the channel (`/invite @xfer-bot`)
- Verify `XFER_ALLOWED_CHANNELS` includes the channel ID (or is unset for all channels)
- Check logs for errors

### Bot doesn't respond to thread messages
- The bot only responds to threads where it has previously participated
- Someone must first mention the bot with `@xfer-bot` to start the conversation
- After a bot restart, the bot will automatically recover context by checking Slack's thread history

### "not_in_channel" errors
- The bot needs to be invited to channels before it can read/write messages

### Socket mode connection issues
- Verify `SLACK_APP_TOKEN` starts with `xapp-`
- Check Socket Mode is enabled in the Slack app settings

### Permission errors
- Review OAuth scopes — you may need to reinstall the app after adding scopes
- The `channels:history` scope is required for the bot to recover thread context after restarts
