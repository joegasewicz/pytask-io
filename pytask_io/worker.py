import asyncio
from typing import List, Callable

from pytask_io.logger import logger

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

        logger.info(f"fnc ------> {fnc}")
        logger.info(f"args ------> {args}")
        test_result = fnc(args[0], args[1])

        logger.info(f"Worker results ------> {test_result}")
        result = await current_loop.run_in_executor(None, fnc, *args)

        logger.info(f"Worker results ------> {result}")
        q.task_done()
        return result

