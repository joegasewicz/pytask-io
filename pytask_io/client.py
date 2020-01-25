import asyncio
import logging
import redis
import dill
from toolz.functoolz import compose, curry, pipe
from typing import List, Any, Dict, Tuple, Callable, Awaitable


from pytask_io.worker_queue import create_worker_queue
from pytask_io.event_loop import event_loop
from pytask_io.logger import logger


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


async def client(queue_client: redis.Redis):
    """
        Client for Workers, Tasks & AsyncIO's Event Loop
    """

    queue = create_worker_queue()

    next_task = await get_task_from_queue_client(queue_client)
    logger.info(f"FINAL----> {next_task}")
    executable_uow, uow_args = await deserialize_task(next_task[1])

    logger.info(f"executable_uow----> {executable_uow}")
    logger.info(f"uow_args----> {uow_args}")

    # Serialize units of work (UOW)

    # Push UOW into task queue

    # Get units of work from Redis Store

    # Deserialize UOW
