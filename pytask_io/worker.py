import asyncio
from typing import List, Callable

tasks = []


def get_tasks(task: List[Callable]):
    return task


async def worker(work_units: List[Callable], q: asyncio.Queue):
    """
    - Worker
        - Observes task queue.
        - Fetches available task to run.
        - Tasks are run asynchronously on a single asyncIO event loop.
    """
    for w in work_units:
        q.put_nowait(w)  # puts units of work in the asyncio queue
