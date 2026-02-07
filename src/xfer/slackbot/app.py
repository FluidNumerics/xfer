"""
Slack Bolt application for the xfer data transfer bot.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from .claude_agent import ClaudeAgent
from .config import BotConfig

logger = logging.getLogger(__name__)


def markdown_to_slack(text: str) -> str:
    """
    Convert standard markdown to Slack's mrkdwn format.

    This is a safety net in case Claude outputs standard markdown
    despite being instructed to use Slack format.
    """
    # Convert **bold** to *bold* (must do before single asterisk handling)
    text = re.sub(r"\*\*([^*]+)\*\*", r"*\1*", text)

    # Convert [text](url) to <url|text>
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"<\2|\1>", text)

    # Convert ~~strikethrough~~ to ~strikethrough~
    text = re.sub(r"~~([^~]+)~~", r"~\1~", text)

    # Remove markdown headers (## Header -> *Header*)
    text = re.sub(r"^#{1,6}\s+(.+)$", r"*\1*", text, flags=re.MULTILINE)

    return text


class ConversationStore:
    """
    Simple in-memory store for conversation history per thread.

    In production, you might want to use Redis or similar for persistence.
    """

    def __init__(self, max_messages: int = 10):
        self.max_messages = max_messages
        self._store: dict[str, list[dict]] = {}

    def _key(self, channel: str, thread_ts: str) -> str:
        return f"{channel}:{thread_ts}"

    def get(self, channel: str, thread_ts: str) -> list[dict]:
        return self._store.get(self._key(channel, thread_ts), [])

    def append(self, channel: str, thread_ts: str, message: dict) -> None:
        key = self._key(channel, thread_ts)
        if key not in self._store:
            self._store[key] = []
        self._store[key].append(message)
        # Trim to max messages
        if len(self._store[key]) > self.max_messages:
            self._store[key] = self._store[key][-self.max_messages :]


def create_app(config: BotConfig | None = None) -> tuple[App, SocketModeHandler]:
    """
    Create and configure the Slack Bolt app.

    Returns the app and socket mode handler.
    """
    if config is None:
        config = BotConfig.from_env()

    app = App(token=config.slack_bot_token)
    # Pass Slack client to agent for support channel notifications
    agent = ClaudeAgent(config, slack_client=app.client)
    conversations = ConversationStore()

    def fetch_thread_history(channel: str, thread_ts: str) -> list[dict] | None:
        """
        Fetch conversation history from a Slack thread and reconstruct it.

        Returns the conversation history if the bot has participated in the thread,
        or None if the bot hasn't participated or there was an error.

        This is used as a fallback when in-memory conversation history is lost
        (e.g., after bot restart).
        """
        try:
            # Get our bot's user ID
            auth_result = app.client.auth_test()
            bot_user_id = auth_result.get("user_id")
            if not bot_user_id:
                return None

            # Fetch thread replies
            result = app.client.conversations_replies(
                channel=channel,
                ts=thread_ts,
                limit=20,  # Limit to recent messages to avoid rate limits
            )
            messages = result.get("messages", [])

            # Check if bot has participated
            bot_participated = any(msg.get("user") == bot_user_id for msg in messages)
            if not bot_participated:
                return None

            # Reconstruct conversation history
            history = []
            for msg in messages:
                text = msg.get("text", "")
                if not text:
                    continue

                if msg.get("user") == bot_user_id:
                    # Bot's message
                    history.append({"role": "assistant", "content": text})
                else:
                    # User message - strip bot mentions
                    text = re.sub(r"<@[A-Z0-9]+>\s*", "", text).strip()
                    if text:
                        history.append({"role": "user", "content": text})

            logger.info(
                f"Reconstructed {len(history)} messages from thread {channel}:{thread_ts}"
            )
            return history

        except Exception as e:
            logger.warning(f"Failed to fetch thread history: {e}")
            return None

    def is_allowed_channel(channel: str) -> bool:
        """Check if the bot should respond in this channel."""
        if not config.allowed_channels:
            return True  # No restrictions
        return channel in config.allowed_channels

    @app.event("app_mention")
    def handle_mention(event: dict[str, Any], say: Any) -> None:
        """Handle @mentions of the bot."""
        channel = event.get("channel", "")
        thread_ts = event.get("thread_ts") or event.get("ts", "")
        user = event.get("user", "")
        text = event.get("text", "")

        if not is_allowed_channel(channel):
            logger.info(f"Ignoring message in non-allowed channel: {channel}")
            return

        # Remove the bot mention from the text
        # Format is typically "<@U12345> message"
        import re

        text = re.sub(r"<@[A-Z0-9]+>\s*", "", text).strip()

        if not text:
            say(
                text="Hi! I'm the data transfer bot. Tell me what you'd like to transfer and I'll help set it up.",
                thread_ts=thread_ts,
            )
            return

        logger.info(f"Processing request from {user} in {channel}: {text[:100]}...")

        # Get conversation history for context
        history = conversations.get(channel, thread_ts)

        try:
            response = agent.process_message(
                user_message=text,
                channel_id=channel,
                thread_ts=thread_ts,
                conversation_history=history.copy() if history else None,
            )

            # Store the exchange
            conversations.append(channel, thread_ts, {"role": "user", "content": text})
            conversations.append(
                channel, thread_ts, {"role": "assistant", "content": response}
            )

            # Convert any markdown to Slack mrkdwn format
            say(text=markdown_to_slack(response), thread_ts=thread_ts)

        except Exception as e:
            logger.exception(f"Error processing message: {e}")
            say(
                text=f"Sorry, I encountered an error processing your request: {str(e)}",
                thread_ts=thread_ts,
            )

    @app.event("message")
    def handle_message(event: dict[str, Any], say: Any) -> None:
        """
        Handle direct messages and thread replies.

        For thread replies, we respond if we've been mentioned in the thread before.
        For DMs, we always respond.
        """
        # Ignore bot messages to prevent loops
        if event.get("bot_id") or event.get("subtype") == "bot_message":
            return

        channel = event.get("channel", "")
        channel_type = event.get("channel_type", "")
        thread_ts = event.get("thread_ts") or event.get("ts", "")
        text = event.get("text", "")
        user = event.get("user", "")

        # Handle DMs
        if channel_type == "im":
            if not text:
                return

            logger.info(f"Processing DM from {user}: {text[:100]}...")

            history = conversations.get(channel, thread_ts)

            try:
                response = agent.process_message(
                    user_message=text,
                    channel_id=channel,
                    thread_ts=thread_ts,
                    conversation_history=history.copy() if history else None,
                )

                conversations.append(
                    channel, thread_ts, {"role": "user", "content": text}
                )
                conversations.append(
                    channel, thread_ts, {"role": "assistant", "content": response}
                )

                # Convert any markdown to Slack mrkdwn format
                say(text=markdown_to_slack(response), thread_ts=thread_ts)

            except Exception as e:
                logger.exception(f"Error processing DM: {e}")
                say(
                    text=f"Sorry, I encountered an error: {str(e)}",
                    thread_ts=thread_ts,
                )

        # For channel messages in threads where we've participated, continue responding
        elif event.get("thread_ts"):
            if not is_allowed_channel(channel):
                return

            # Check if we have history in this thread (meaning we've been mentioned)
            history = conversations.get(channel, thread_ts)

            # If no in-memory history, try to reconstruct from Slack API
            # This handles the case where the bot was restarted and lost memory
            if not history:
                fetched_history = fetch_thread_history(channel, thread_ts)
                if fetched_history is not None:
                    logger.info(
                        f"Restored {len(fetched_history)} messages for thread {channel}:{thread_ts}"
                    )
                    # Restore to in-memory store for future messages in this session
                    for msg in fetched_history:
                        conversations.append(channel, thread_ts, msg)
                    history = fetched_history
                else:
                    # Bot hasn't participated in this thread
                    return

            # Triage: check if message is directed at the bot
            if not agent.should_respond_in_thread(
                user_message=text,
                conversation_history=history.copy() if history else None,
            ):
                logger.info(f"Triage: skipping message from {user} in {channel}:{thread_ts}")
                # Still store the message so future triage/responses have full context
                conversations.append(channel, thread_ts, {"role": "user", "content": text})
                return

            logger.info(
                f"Continuing thread conversation with {user}: {text[:100]}..."
            )

            try:
                response = agent.process_message(
                    user_message=text,
                    channel_id=channel,
                    thread_ts=thread_ts,
                    conversation_history=history.copy() if history else None,
                )

                conversations.append(
                    channel, thread_ts, {"role": "user", "content": text}
                )
                conversations.append(
                    channel, thread_ts, {"role": "assistant", "content": response}
                )

                # Convert any markdown to Slack mrkdwn format
                say(text=markdown_to_slack(response), thread_ts=thread_ts)

            except Exception as e:
                logger.exception(f"Error in thread reply: {e}")
                say(
                    text=f"Sorry, I encountered an error: {str(e)}",
                    thread_ts=thread_ts,
                )

    # Create socket mode handler
    handler = SocketModeHandler(app, config.slack_app_token)

    return app, handler


def main() -> None:
    """Run the bot."""
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    config = BotConfig.from_env()

    # Validate required config
    if not config.slack_bot_token:
        print(
            "Error: SLACK_BOT_TOKEN environment variable is required", file=sys.stderr
        )
        sys.exit(1)
    if not config.slack_app_token:
        print(
            "Error: SLACK_APP_TOKEN environment variable is required", file=sys.stderr
        )
        sys.exit(1)
    if not config.anthropic_api_key:
        print(
            "Error: ANTHROPIC_API_KEY environment variable is required", file=sys.stderr
        )
        sys.exit(1)

    logger.info("Starting xfer Slack bot...")
    logger.info(f"Runs directory: {config.runs_base_dir}")
    logger.info(f"Allowed channels: {config.allowed_channels or 'all'}")

    app, handler = create_app(config)
    handler.start()


if __name__ == "__main__":
    main()
