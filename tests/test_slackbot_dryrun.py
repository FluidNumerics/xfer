#!/usr/bin/env python3
"""
Dry-run test for xfer-slackbot components.

Tests the bot logic without connecting to Slack or submitting real jobs.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add src to path for local testing
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from xfer.slackbot.config import (
    BotConfig,
    RcloneDefaults,
    SlurmDefaults,
    parse_slack_comment,
    slack_comment,
)
from xfer.slackbot.slurm_tools import (
    JobInfo,
    TransferResult,
    _parse_job_from_sacct_json,
    _write_prepare_script,
    cancel_job,
    get_allowed_backends,
    get_transfer_progress,
    validate_backend,
)


def test_slack_comment_format():
    """Test Slack comment generation and parsing."""
    print("\n=== Testing Slack comment format ===")

    channel_id = "C07ABC123"
    thread_ts = "1234567890.123456"

    comment = slack_comment(channel_id, thread_ts)
    print(f"Generated comment: {comment}")
    assert comment == "slack:C07ABC123/1234567890.123456"

    parsed = parse_slack_comment(comment)
    print(f"Parsed back: {parsed}")
    assert parsed == (channel_id, thread_ts)

    # Test invalid comment
    assert parse_slack_comment("not-a-slack-comment") is None
    assert parse_slack_comment("slack:invalid") is None

    print("✓ Slack comment format tests passed")


def test_config_defaults():
    """Test configuration defaults."""
    print("\n=== Testing config defaults ===")

    config = BotConfig()
    print(f"Default partition: {config.slurm.partition}")
    print(f"Default time_limit: {config.slurm.time_limit}")
    print(f"Default num_shards: {config.slurm.num_shards}")
    print(f"Default rclone image: {config.rclone.image}")

    assert config.slurm.partition == "transfer"
    assert config.slurm.time_limit == "24:00:00"
    assert config.slurm.num_shards == 256

    print("✓ Config defaults tests passed")


def test_allowed_backends_from_yaml():
    """Test loading allowed backends from YAML file."""
    print("\n=== Testing allowed backends from YAML ===")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("allowed_backends:\n  - s3src\n  - s3dst\n  - gcs-archive\n")
        yaml_path = Path(f.name)

    try:
        config = BotConfig()
        config.allowed_backends_file = yaml_path

        backends = get_allowed_backends(config)
        print(f"Loaded backends: {backends}")
        assert backends == ["s3src", "s3dst", "gcs-archive"]

        # Test validation
        valid, msg = validate_backend("s3src:bucket/path", config)
        print(f"s3src valid: {valid} - {msg}")
        assert valid

        valid, msg = validate_backend("unknown:bucket/path", config)
        print(f"unknown valid: {valid} - {msg}")
        assert not valid

        print("✓ Allowed backends tests passed")
    finally:
        yaml_path.unlink()


def test_allowed_backends_from_rclone_conf():
    """Test loading allowed backends from rclone.conf."""
    print("\n=== Testing allowed backends from rclone.conf ===")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
        f.write("""[s3src]
type = s3
provider = AWS

[s3dst]
type = s3
provider = AWS

[gcs-research]
type = google cloud storage
""")
        conf_path = Path(f.name)

    try:
        config = BotConfig()
        config.allowed_backends_file = None
        config.rclone.config_path = conf_path

        backends = get_allowed_backends(config)
        print(f"Parsed backends from rclone.conf: {backends}")
        assert "s3src" in backends
        assert "s3dst" in backends
        assert "gcs-research" in backends

        print("✓ rclone.conf parsing tests passed")
    finally:
        conf_path.unlink()


def test_prepare_script_generation():
    """Test generation of the prepare.sh script."""
    print("\n=== Testing prepare script generation ===")

    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir)

        config = BotConfig()
        config.slurm.qos = "data-transfer"

        script_path = _write_prepare_script(
            run_dir=run_dir,
            source="s3src:mybucket/data",
            dest="s3dst:archive/data",
            config=config,
            num_shards=128,
            array_concurrency=32,
            time_limit="12:00:00",
            job_name="test-xfer",
            comment="slack:C07ABC/1234567890.123456",
        )

        print(f"Script written to: {script_path}")
        assert script_path.exists()

        content = script_path.read_text()
        print("\n--- Script content (first 50 lines) ---")
        for i, line in enumerate(content.splitlines()[:50]):
            print(f"  {i+1:3}: {line}")

        # Verify key elements
        assert "#SBATCH --job-name=test-xfer-prepare" in content
        assert "#SBATCH --comment=" in content
        assert "#SBATCH --chdir=" in content
        assert "#SBATCH --qos=data-transfer" in content
        assert "xfer manifest combine" in content
        assert "--mem=250G" in content
        assert "manifest-worker.sh" in content
        assert "xfer manifest shard" in content
        assert "xfer slurm render" in content
        assert "xfer slurm submit" in content
        assert "--num-shards 128" in content

        print("\n✓ Prepare script generation tests passed")


def test_sacct_json_parsing():
    """Test parsing of sacct --json output."""
    print("\n=== Testing sacct JSON parsing ===")

    # Simulated sacct --json output for a job
    job_data = {
        "job_id": 12345,
        "name": "xfer-slack",
        "partition": "transfer",
        "state": {"current": ["RUNNING"]},
        "time": {
            "submission": 1706976000,
            "start": 1706976060,
            "end": 0,
        },
        "working_directory": "/scratch/xfer-runs/slack_C07ABC_20260204",
        "comment": {"job": "slack:C07ABC/1234567890.123456"},
    }

    job_info = _parse_job_from_sacct_json(job_data)
    print(f"Parsed job: {job_info}")

    assert job_info.job_id == "12345"
    assert job_info.state == "RUNNING"
    assert job_info.name == "xfer-slack"
    assert job_info.work_dir == "/scratch/xfer-runs/slack_C07ABC_20260204"
    assert "slack:C07ABC" in job_info.comment

    # Test array job
    array_job_data = {
        "job_id": 12345,
        "array": {"task_id": {"number": 42}},
        "name": "xfer-slack",
        "partition": "transfer",
        "state": {"current": ["COMPLETED"]},
        "time": {},
        "comment": {"job": ""},
    }

    array_job = _parse_job_from_sacct_json(array_job_data)
    print(f"Parsed array job: {array_job}")
    assert array_job.job_id == "12345_42"
    assert array_job.array_job_id == "12345"

    print("✓ sacct JSON parsing tests passed")


def test_transfer_progress():
    """Test transfer progress calculation from state files."""
    print("\n=== Testing transfer progress calculation ===")

    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir)

        # Create directory structure
        (run_dir / "shards").mkdir()
        (run_dir / "state").mkdir()

        # Write manifest (required before shards)
        (run_dir / "manifest.jsonl").write_text('{"path": "file1.txt"}\n')

        # Write shards metadata
        shards_meta = {
            "num_shards": 10,
            "bytes_total": 1073741824,  # 1 GB
            "num_records": 1000,
        }
        (run_dir / "shards" / "shards.meta.json").write_text(json.dumps(shards_meta))

        # Write request metadata
        request_meta = {
            "source": "s3src:bucket/data",
            "dest": "s3dst:archive/data",
            "prepare_job_id": "12345",
        }
        (run_dir / "request.json").write_text(json.dumps(request_meta))

        # Simulate some progress
        (run_dir / "state" / "shard_0.done").touch()
        (run_dir / "state" / "shard_1.done").touch()
        (run_dir / "state" / "shard_2.done").touch()
        (run_dir / "state" / "shard_3.attempt").write_text("1")
        (run_dir / "state" / "shard_4.attempt").write_text("2")
        (run_dir / "state" / "shard_5.fail").write_text("1")

        progress = get_transfer_progress(run_dir)
        print(f"Progress: {json.dumps(progress, indent=2)}")

        assert progress["phase"] == "transferring"
        assert progress["total_tasks"] == 10
        assert progress["completed"] == 3
        assert progress["failed"] == 1
        assert progress["in_progress"] == 2  # shard_3 and shard_4 have attempts but no done/fail
        assert progress["pending"] == 4  # 10 - 3 - 1 - 2
        assert progress["total_bytes"] == 1073741824
        assert progress["source"] == "s3src:bucket/data"

        print("✓ Transfer progress tests passed")


def test_claude_agent_tools():
    """Test Claude agent tool definitions and execution."""
    print("\n=== Testing Claude agent tools ===")

    from xfer.slackbot.claude_agent import TOOLS

    tool_names = [t["name"] for t in TOOLS]
    print(f"Available tools: {tool_names}")

    expected_tools = [
        "submit_transfer",
        "check_status",
        "list_backends",
        "cancel_job",
        "get_transfer_details",
        "request_backend_access",
    ]

    for tool in expected_tools:
        assert tool in tool_names, f"Missing tool: {tool}"

    # Verify tool schemas
    for tool in TOOLS:
        assert "name" in tool
        assert "description" in tool
        assert "input_schema" in tool
        print(f"  ✓ {tool['name']}: {len(tool['description'])} chars, {len(tool['input_schema'].get('properties', {}))} params")

    print("✓ Claude agent tools tests passed")


def test_triage_respond():
    """Test triage returns True when Haiku says respond=true."""
    print("\n=== Testing triage: respond ===")

    from xfer.slackbot.claude_agent import ClaudeAgent

    config = BotConfig()
    config.anthropic_api_key = "test-key"

    with patch("anthropic.Anthropic") as MockClient:
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='{"respond": true, "reason": "asking about transfer status"}')]
        MockClient.return_value.messages.create.return_value = mock_response

        agent = ClaudeAgent(config)
        result = agent.should_respond_in_thread(
            user_message="What's the status of my transfer?",
            conversation_history=[{"role": "assistant", "content": "Your transfer has been submitted."}],
        )

        assert result is True

        # Verify triage model was used
        call_kwargs = MockClient.return_value.messages.create.call_args
        assert call_kwargs.kwargs["model"] == config.triage_model

    print("✓ Triage respond test passed")


def test_triage_skip():
    """Test triage returns False when Haiku says respond=false."""
    print("\n=== Testing triage: skip ===")

    from xfer.slackbot.claude_agent import ClaudeAgent

    config = BotConfig()
    config.anthropic_api_key = "test-key"

    with patch("anthropic.Anthropic") as MockClient:
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='{"respond": false, "reason": "users talking to each other"}')]
        MockClient.return_value.messages.create.return_value = mock_response

        agent = ClaudeAgent(config)
        result = agent.should_respond_in_thread(
            user_message="hey @alice, did you get my email?",
            conversation_history=[{"role": "assistant", "content": "Transfer submitted."}],
        )

        assert result is False

    print("✓ Triage skip test passed")


def test_triage_error_defaults_to_respond():
    """Test triage fails open (returns True) on error."""
    print("\n=== Testing triage: error defaults to respond ===")

    from xfer.slackbot.claude_agent import ClaudeAgent

    config = BotConfig()
    config.anthropic_api_key = "test-key"

    with patch("anthropic.Anthropic") as MockClient:
        MockClient.return_value.messages.create.side_effect = Exception("API error")

        agent = ClaudeAgent(config)
        result = agent.should_respond_in_thread(
            user_message="check my transfer",
            conversation_history=None,
        )

        assert result is True

    print("✓ Triage error defaults to respond test passed")


def test_full_flow_simulation():
    """Simulate a full transfer request flow."""
    print("\n=== Simulating full transfer flow ===")

    with tempfile.TemporaryDirectory() as tmpdir:
        # Setup config
        config = BotConfig()
        config.runs_base_dir = Path(tmpdir)

        # Create mock rclone.conf
        rclone_conf = Path(tmpdir) / "rclone.conf"
        rclone_conf.write_text("[s3src]\ntype=s3\n\n[s3dst]\ntype=s3\n")
        config.rclone.config_path = rclone_conf

        print(f"Runs base dir: {config.runs_base_dir}")
        print(f"Rclone config: {config.rclone.config_path}")

        # Simulate a transfer request
        channel_id = "C07ABC123"
        thread_ts = "1234567890.123456"
        source = "s3src:research/dataset1"
        dest = "s3dst:archive/dataset1"

        print(f"\nSimulating transfer request:")
        print(f"  Source: {source}")
        print(f"  Dest: {dest}")
        print(f"  Channel: {channel_id}")
        print(f"  Thread: {thread_ts}")

        # Validate backends
        valid, msg = validate_backend(source, config)
        print(f"\nSource validation: {valid} - {msg}")
        assert valid

        valid, msg = validate_backend(dest, config)
        print(f"Dest validation: {valid} - {msg}")
        assert valid

        # Generate run directory name
        from datetime import datetime

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        run_name = f"slack_{channel_id}_{timestamp}"
        run_dir = config.runs_base_dir / run_name
        run_dir.mkdir(parents=True)
        print(f"\nRun directory: {run_dir}")

        # Generate prepare script
        comment = slack_comment(channel_id, thread_ts)
        script_path = _write_prepare_script(
            run_dir=run_dir,
            source=source,
            dest=dest,
            config=config,
            num_shards=config.slurm.num_shards,
            array_concurrency=config.slurm.array_concurrency,
            time_limit=config.slurm.time_limit,
            job_name="xfer-slack",
            comment=comment,
        )
        print(f"Prepare script: {script_path}")
        assert script_path.exists()

        # Write request metadata (as submit_transfer would)
        request_meta = {
            "source": source,
            "dest": dest,
            "channel_id": channel_id,
            "thread_ts": thread_ts,
            "prepare_job_id": "SIMULATED_12345",
            "num_shards": config.slurm.num_shards,
            "submitted_at": datetime.utcnow().isoformat() + "Z",
        }
        (run_dir / "request.json").write_text(json.dumps(request_meta, indent=2))
        print(f"Request metadata written")

        # Check initial progress (no manifest yet)
        progress = get_transfer_progress(run_dir)
        print(f"\nInitial progress: phase={progress['phase']}")
        assert progress["phase"] == "building_manifest"

        # Simulate manifest complete
        (run_dir / "manifest.jsonl").write_text('{"path": "file1.txt", "size": 1000}\n')
        progress = get_transfer_progress(run_dir)
        print(f"After manifest: phase={progress['phase']}")
        assert progress["phase"] == "sharding"

        # Simulate sharding complete
        (run_dir / "shards").mkdir()
        (run_dir / "shards" / "shards.meta.json").write_text(
            json.dumps({"num_shards": 4, "bytes_total": 4000, "num_records": 4})
        )
        progress = get_transfer_progress(run_dir)
        print(f"After sharding: phase={progress['phase']}")
        assert progress["phase"] == "waiting_to_start"

        # Simulate transfer in progress
        (run_dir / "state").mkdir()
        (run_dir / "state" / "shard_0.done").touch()
        (run_dir / "state" / "shard_1.attempt").write_text("1")
        progress = get_transfer_progress(run_dir)
        print(f"During transfer: phase={progress['phase']}, completed={progress['completed']}/{progress['total_tasks']}")
        assert progress["phase"] == "transferring"
        assert progress["completed"] == 1

        # Simulate complete
        (run_dir / "state" / "shard_1.done").touch()
        (run_dir / "state" / "shard_2.done").touch()
        (run_dir / "state" / "shard_3.done").touch()
        progress = get_transfer_progress(run_dir)
        print(f"After complete: phase={progress['phase']}, completed={progress['completed']}/{progress['total_tasks']}")
        assert progress["phase"] == "complete"
        assert progress["completed"] == 4

        print("\n✓ Full flow simulation passed")


def test_cancel_job_by_submitter():
    """Test that the user who submitted a job can cancel it."""
    print("\n=== Testing cancel job by submitter ===")

    channel = "C07ABC123"
    thread_ts = "1234567890.123456"
    comment = slack_comment(channel, thread_ts)

    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir)
        request_meta = {"submitted_by": "U_ALICE"}
        (run_dir / "request.json").write_text(json.dumps(request_meta))

        job_info = JobInfo(
            job_id="123",
            array_job_id=None,
            state="RUNNING",
            name="xfer-slack",
            comment=comment,
            work_dir=str(run_dir),
            submit_time=None,
            start_time=None,
            end_time=None,
            partition="transfer",
        )

        with patch("xfer.slackbot.slurm_tools.get_job_status", return_value=job_info), \
             patch("xfer.slackbot.slurm_tools.run_cmd"):
            success, message = cancel_job("123", channel, thread_ts, user_id="U_ALICE")
            assert success, f"Expected success, got: {message}"
            assert "cancelled" in message

    print("✓ Cancel job by submitter test passed")


def test_cancel_job_by_other_user():
    """Test that a different user cannot cancel someone else's job."""
    print("\n=== Testing cancel job by other user ===")

    channel = "C07ABC123"
    thread_ts = "1234567890.123456"
    comment = slack_comment(channel, thread_ts)

    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir)
        request_meta = {"submitted_by": "U_ALICE"}
        (run_dir / "request.json").write_text(json.dumps(request_meta))

        job_info = JobInfo(
            job_id="123",
            array_job_id=None,
            state="RUNNING",
            name="xfer-slack",
            comment=comment,
            work_dir=str(run_dir),
            submit_time=None,
            start_time=None,
            end_time=None,
            partition="transfer",
        )

        with patch("xfer.slackbot.slurm_tools.get_job_status", return_value=job_info):
            success, message = cancel_job("123", channel, thread_ts, user_id="U_BOB")
            assert not success, f"Expected failure, got success: {message}"
            assert "Only the user who submitted" in message

    print("✓ Cancel job by other user test passed")


def test_cancel_job_legacy_no_submitter():
    """Test that jobs without submitted_by field can be cancelled by anyone (backwards compat)."""
    print("\n=== Testing cancel job legacy (no submitter) ===")

    channel = "C07ABC123"
    thread_ts = "1234567890.123456"
    comment = slack_comment(channel, thread_ts)

    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir)
        request_meta = {"source": "s3src:bucket/data", "dest": "s3dst:archive/data"}
        (run_dir / "request.json").write_text(json.dumps(request_meta))

        job_info = JobInfo(
            job_id="123",
            array_job_id=None,
            state="RUNNING",
            name="xfer-slack",
            comment=comment,
            work_dir=str(run_dir),
            submit_time=None,
            start_time=None,
            end_time=None,
            partition="transfer",
        )

        with patch("xfer.slackbot.slurm_tools.get_job_status", return_value=job_info), \
             patch("xfer.slackbot.slurm_tools.run_cmd"):
            success, message = cancel_job("123", channel, thread_ts, user_id="U_ANYONE")
            assert success, f"Expected success for legacy job, got: {message}"
            assert "cancelled" in message

    print("✓ Cancel job legacy (no submitter) test passed")


def main():
    """Run all dry-run tests."""
    print("=" * 60)
    print("xfer-slackbot Dry Run Tests")
    print("=" * 60)

    test_slack_comment_format()
    test_config_defaults()
    test_allowed_backends_from_yaml()
    test_allowed_backends_from_rclone_conf()
    test_prepare_script_generation()
    test_sacct_json_parsing()
    test_transfer_progress()
    test_claude_agent_tools()
    test_triage_respond()
    test_triage_skip()
    test_triage_error_defaults_to_respond()
    test_full_flow_simulation()
    test_cancel_job_by_submitter()
    test_cancel_job_by_other_user()
    test_cancel_job_legacy_no_submitter()

    print("\n" + "=" * 60)
    print("All dry-run tests passed! ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
