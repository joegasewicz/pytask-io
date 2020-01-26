import asyncio
import pytest


@pytest.fixture
def event_loop():

    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    yield loop
    loop.close()
