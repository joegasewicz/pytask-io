import asyncio
import logging
import redis
import dill
import json
from toolz.functoolz import compose, curry, pipe
from typing import List, Any, Dict, Tuple, Callable, Awaitable


from pytask_io.worker_queue import create_worker_queue
from pytask_io.event_loop import event_loop
from pytask_io.logger import logger
from pytask_io.worker import worker


async def get_task_from_queue_client(q: redis.Redis) -> Tuple[Callable, List]:  # TODO correct return type
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError as err:
        raise RuntimeError(f"PyTaskIO: {err}")
    result = await current_loop.run_in_executor(None, q.brpop, "tasks")
    return result


async def deserialize_task(task_data: Any):
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError as err:
        raise RuntimeError(f"PyTaskIO: {err}")
    result = await current_loop.run_in_executor(None, dill.loads, task_data)
    return result


async def deserialize_store_data(task_data: Any):
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError as err:
        raise RuntimeError(f"PyTaskIO: {err}")
    if task_data:
        result = await current_loop.run_in_executor(None, dill.loads, task_data)
        return result
    else:
        return None


async def client(queue_client: redis.Redis):
    """
        Client for Workers, Tasks & AsyncIO's Event Loop
    """
    queue = create_worker_queue()

    next_task = await get_task_from_queue_client(queue_client)
    executable_uow, uow_args = await deserialize_task(next_task[1])

    unit_of_work = {
        "function": executable_uow,
        "args": uow_args,
    }

    # Push unit of work into the asyncio queue
    queue.put_nowait(unit_of_work)

    # Create `3` workers tasks to process the queue concurrently
    tasks = []
    for i in range(3):
        task = asyncio.create_task(worker(queue))
        tasks.append(task)
    # Wait until queue is fully processed
    await queue.join()

    # Cancel tasks
    for task in tasks:
        task.cancel()

    # Wait until all worker tasks are cancelled
    await asyncio.gather(*tasks, return_exceptions=True)
