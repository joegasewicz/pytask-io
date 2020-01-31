import asyncio
from typing import List, Callable

from pytask_io.store import add_uof_result_to_store
from pytask_io.utils import serialize_store_data

tasks = []


def get_tasks(task: List[Callable]):
    return task


async def worker(q: asyncio.Queue, queue_client):
    """
    - Worker
        - Observes task queue.
        - Fetches available task to run.
        - Tasks are run asynchronously on a single asyncIO event loop.
    """
    while True:
        executable_uow = await q.get()

        fnc = executable_uow["function"]
        args = executable_uow["args"]

        current_loop = asyncio.get_running_loop()
        result = await current_loop.run_in_executor(None, fnc, *args)
        # Add results to store

        await add_uof_result_to_store(queue_client, result)
        q.task_done()
