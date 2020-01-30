import asyncio
from typing import List, Callable

from pytask_io.logger import logger
from pytask_io.store import add_uof_result_to_store

tasks = []


def get_tasks(task: List[Callable]):
    return task


async def worker(q: asyncio.Queue):
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
        store_meta = await add_uof_result_to_store("task_result", result)
        print(f"store_meta -------> {store_meta}")
        q.task_done()
        return result

