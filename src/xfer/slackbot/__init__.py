"""
xfer slackbot - Claude-powered Slack bot for data transfer requests.

This module provides a Slack bot that uses Claude to interpret user requests
and manage data transfers via xfer/Slurm.
"""

from .app import create_app

__all__ = ["create_app"]
