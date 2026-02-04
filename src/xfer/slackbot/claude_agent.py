"""
Claude agent with tool definitions for the xfer Slack bot.

This module defines the tools Claude can use and handles the conversation flow.
"""

from __future__ import annotations

import json
from typing import Any

import anthropic

from .config import BotConfig
from .slurm_tools import (
    cancel_job,
    get_allowed_backends,
    get_job_status,
    get_jobs_by_thread,
    get_source_stats,
    get_transfer_progress,
    get_transfer_progress_by_job,
    submit_transfer,
)

# Tool definitions for Claude
TOOLS = [
    {
        "name": "submit_transfer",
        "description": """Submit a new data transfer job. Use this when the user wants to transfer data from one location to another.

Before calling this tool:
1. Confirm you have both source and destination paths
2. Validate that the backends are allowed using list_backends first
3. Ask for clarification if the request is ambiguous

The transfer runs in two phases:
1. A preparation job builds the file manifest and sets up the transfer
2. The transfer array job does the actual data movement

Both jobs are tracked and the user can check status at any time.

The source and destination should be in rclone format: "remote:bucket/path" (e.g., "s3src:mybucket/data")""",
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": "Source path in rclone format (remote:bucket/path)",
                },
                "dest": {
                    "type": "string",
                    "description": "Destination path in rclone format (remote:bucket/path)",
                },
                "num_shards": {
                    "type": "integer",
                    "description": "Number of shards to split the transfer into (default: 256)",
                },
                "time_limit": {
                    "type": "string",
                    "description": "Slurm time limit in HH:MM:SS format (default: 24:00:00)",
                },
                "job_name": {
                    "type": "string",
                    "description": "Custom name for the Slurm job",
                },
                "rclone_flags": {
                    "type": "string",
                    "description": "Additional rclone flags to append to defaults (e.g., '--bwlimit 100M --checksum')",
                },
            },
            "required": ["source", "dest"],
        },
    },
    {
        "name": "check_status",
        "description": """Check the status of transfer jobs in this thread. Use this when the user asks about job status, progress, or wants to know if their transfer is complete.

This tool finds all jobs associated with the current Slack thread and returns their status.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "string",
                    "description": "Specific job ID to check. If not provided, shows all jobs for this thread.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "list_backends",
        "description": """List the available/allowed rclone backends for transfers.

Use this to:
- Show users what storage systems they can transfer to/from
- Validate a backend before submitting a transfer
- Help users understand what options are available""",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "cancel_job",
        "description": """Cancel a running transfer job.

Only jobs that were submitted from this thread can be cancelled.
Use this when the user explicitly requests to cancel or stop a transfer.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "string",
                    "description": "The Slurm job ID to cancel",
                },
            },
            "required": ["job_id"],
        },
    },
    {
        "name": "get_transfer_details",
        "description": """Get detailed progress information for a transfer job.

This provides granular progress: how many file shards are complete, failed, or still pending.
Use this when users want detailed progress beyond just the job state.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "string",
                    "description": "The Slurm job ID to get details for",
                },
            },
            "required": ["job_id"],
        },
    },
    {
        "name": "request_backend_access",
        "description": """Flag a request for access to a backend that is not currently allowed.

Use this when a user needs to transfer to/from a backend that isn't in the allowed list.
This notifies the support team to review and potentially add the backend.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "backend_name": {
                    "type": "string",
                    "description": "The name of the backend being requested",
                },
                "justification": {
                    "type": "string",
                    "description": "Why the user needs access to this backend",
                },
            },
            "required": ["backend_name"],
        },
    },
    {
        "name": "get_manifest_stats",
        "description": """Scan a source path and return file statistics without starting a transfer.

Use this to:
- Preview the data volume before committing to a transfer
- Get file count, total size, and size distribution
- See suggested rclone flags based on file size patterns
- Estimate transfer times

This is helpful when users want to know "how much data is there?" or "how long will this take?" before starting.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": "Source path in rclone format (remote:bucket/path)",
                },
            },
            "required": ["source"],
        },
    },
]

SYSTEM_PROMPT = """You are a helpful data transfer assistant for an HPC cluster. You help researchers submit and monitor data transfer jobs via Slurm.

Your capabilities:
- Submit new data transfer jobs (source -> destination)
- Check status of running/completed transfers
- List available storage backends
- Cancel jobs if requested
- Request access to new backends on behalf of users
- Scan source paths to get file statistics and transfer estimates

Guidelines:
1. Always validate that backends are allowed before submitting transfers
2. If a backend isn't allowed, offer to request access on the user's behalf
3. Provide clear, concise status updates
4. Ask for clarification if the source or destination is ambiguous
5. Be helpful but don't make assumptions about paths - ask if unsure
6. When reporting job status, include relevant details like progress and any errors
7. If users want custom rclone flags (e.g., bandwidth limits, checksum verification), pass them via the rclone_flags parameter

Transfer path format:
- Paths should be in rclone format: "remote:bucket/path"
- Example: "s3src:research-data/experiment1" or "gcs:archive-bucket/backups"

Custom rclone flags:
- Users can specify additional rclone flags like '--bwlimit 100M' or '--checksum'
- These are appended to the default flags, not replacing them
- Common options: --bwlimit (bandwidth limit), --checksum (verify with checksums), --dry-run (test without copying)

Formatting (IMPORTANT - you are responding in Slack, not markdown):
- Use Slack's mrkdwn format, NOT standard markdown
- Bold: *text* (single asterisks, not double)
- Italic: _text_ (underscores)
- Strikethrough: ~text~
- Code: `code` (backticks work the same)
- Code blocks: ```code``` (no language specifier)
- Links: <https://url|display text> (angle brackets with pipe)
- Lists: Use bullet points with "• " or simple dashes
- Do NOT use **double asterisks** for bold
- Do NOT use [text](url) for links
- Do NOT use markdown headers like ## Header

Keep responses brief and focused on the task at hand."""


class ClaudeAgent:
    """Agent that uses Claude to interpret requests and execute tools."""

    def __init__(self, config: BotConfig, slack_client=None):
        self.config = config
        self.client = anthropic.Anthropic(api_key=config.anthropic_api_key)
        self.slack_client = slack_client  # For posting to support channel

    def execute_tool(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        channel_id: str,
        thread_ts: str,
    ) -> str:
        """Execute a tool and return the result as a string."""
        if tool_name == "submit_transfer":
            result = submit_transfer(
                source=tool_input["source"],
                dest=tool_input["dest"],
                config=self.config,
                channel_id=channel_id,
                thread_ts=thread_ts,
                num_shards=tool_input.get("num_shards"),
                time_limit=tool_input.get("time_limit"),
                job_name=tool_input.get("job_name"),
                rclone_flags=tool_input.get("rclone_flags"),
            )
            return json.dumps(result.__dict__, default=str)

        elif tool_name == "check_status":
            job_id = tool_input.get("job_id")
            if job_id:
                # Get job info with progress if available
                progress = get_transfer_progress_by_job(job_id)
                if progress:
                    return json.dumps(progress)
                job = get_job_status(job_id)
                if job:
                    return json.dumps(job.to_dict())
                return json.dumps({"error": f"Job {job_id} not found"})
            else:
                # Get all jobs for this thread with progress info
                jobs = get_jobs_by_thread(channel_id, thread_ts)
                results = []
                for job in jobs:
                    if job.work_dir:
                        progress = get_transfer_progress_by_job(job.job_id)
                        if progress:
                            results.append(progress)
                            continue
                    results.append(job.to_dict())
                return json.dumps(results)

        elif tool_name == "list_backends":
            backends = get_allowed_backends(self.config)
            return json.dumps({"allowed_backends": backends})

        elif tool_name == "cancel_job":
            success, message = cancel_job(
                tool_input["job_id"],
                channel_id,
                thread_ts,
            )
            return json.dumps({"success": success, "message": message})

        elif tool_name == "get_transfer_details":
            job_id = tool_input["job_id"]
            progress = get_transfer_progress_by_job(job_id)
            if not progress:
                # Try to get basic job info at least
                job = get_job_status(job_id)
                if job:
                    return json.dumps(
                        {
                            "job": job.to_dict(),
                            "note": "Could not find run directory for detailed progress",
                        }
                    )
                return json.dumps({"error": f"Job {job_id} not found"})

            return json.dumps(progress)

        elif tool_name == "request_backend_access":
            backend_name = tool_input["backend_name"]
            justification = tool_input.get("justification", "No justification provided")

            # Post to support channel if configured
            support_posted = False
            if self.slack_client and self.config.support_channel:
                try:
                    # Build a message linking back to the original thread
                    self.slack_client.chat_postMessage(
                        channel=self.config.support_channel,
                        text=f"Backend access request: {backend_name}",
                        blocks=[
                            {
                                "type": "header",
                                "text": {
                                    "type": "plain_text",
                                    "text": "🔐 Backend Access Request",
                                },
                            },
                            {
                                "type": "section",
                                "fields": [
                                    {
                                        "type": "mrkdwn",
                                        "text": f"*Backend:*\n`{backend_name}`",
                                    },
                                    {
                                        "type": "mrkdwn",
                                        "text": f"*Requested from:*\n<#{channel_id}>",
                                    },
                                ],
                            },
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": f"*Justification:*\n{justification}",
                                },
                            },
                            {
                                "type": "context",
                                "elements": [
                                    {
                                        "type": "mrkdwn",
                                        "text": f"<https://slack.com/archives/{channel_id}/p{thread_ts.replace('.', '')}|View original thread>",
                                    }
                                ],
                            },
                        ],
                    )
                    support_posted = True
                except Exception as e:
                    # Log but don't fail the request
                    import logging

                    logging.getLogger(__name__).error(
                        f"Failed to post to support channel: {e}"
                    )

            return json.dumps(
                {
                    "status": (
                        "request_submitted" if support_posted else "request_logged"
                    ),
                    "backend": backend_name,
                    "justification": justification,
                    "support_notified": support_posted,
                    "message": (
                        "Your request for backend access has been submitted to the support team."
                        if support_posted
                        else "Your request has been logged but could not notify support. Please contact them directly."
                    ),
                }
            )

        elif tool_name == "get_manifest_stats":
            stats = get_source_stats(
                source=tool_input["source"],
                config=self.config,
            )
            # Convert dataclass to dict for JSON serialization
            result = {
                "source": stats.source,
                "total_files": stats.total_files,
                "total_bytes": stats.total_bytes,
                "total_bytes_human": stats.total_bytes_human,
                "file_size_stats": stats.file_size_stats,
                "suggested_flags": stats.suggested_flags,
                "histogram": stats.histogram,
                "histogram_text": stats.histogram_text,
            }
            if stats.error:
                result["error"] = stats.error
            return json.dumps(result)

        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    def process_message(
        self,
        user_message: str,
        channel_id: str,
        thread_ts: str,
        conversation_history: list[dict] | None = None,
    ) -> str:
        """
        Process a user message and return Claude's response.

        Handles the full tool-use loop until Claude provides a final response.
        """
        messages = conversation_history or []
        messages.append({"role": "user", "content": user_message})

        while True:
            response = self.client.messages.create(
                model=self.config.claude_model,
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            )

            # Check if we need to execute tools
            if response.stop_reason == "tool_use":
                # Add assistant's response to history
                messages.append({"role": "assistant", "content": response.content})

                # Execute each tool and collect results
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        result = self.execute_tool(
                            block.name,
                            block.input,
                            channel_id,
                            thread_ts,
                        )
                        tool_results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": result,
                            }
                        )

                # Add tool results and continue the loop
                messages.append({"role": "user", "content": tool_results})

            else:
                # Extract text response
                text_response = ""
                for block in response.content:
                    if hasattr(block, "text"):
                        text_response += block.text

                return text_response
