import asyncio


def create_worker_queue() -> asyncio.Queue:
    """Creates he asyncio queue"""
    return asyncio.Queue()
