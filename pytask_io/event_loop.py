import asyncio
from typing import Callable


def event_loop(client: Callable):
    """The event loop"""
    asyncio.run(client())
