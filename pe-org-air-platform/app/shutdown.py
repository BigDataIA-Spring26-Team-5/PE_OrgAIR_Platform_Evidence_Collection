"""
app/shutdown.py

Shared shutdown flag for graceful termination of background tasks.
Both main.py and background task runners import from here to avoid circular imports.
"""

import asyncio

_shutdown_event = asyncio.Event()


def set_shutdown():
    """Signal that the app is shutting down."""
    _shutdown_event.set()


def is_shutting_down() -> bool:
    """Check if the app is shutting down. Used by background tasks."""
    return _shutdown_event.is_set()