import asyncio
import redis
from typing import Callable, Any, List
import threading

from pytask_io.worker import worker
from pytask_io.utils import get_task_from_queue_client, deserialize_task
from pytask_io.actions import QueueActions
from pytask_io.logger import logger

tasks = []


async def client(worker_queue: asyncio.Queue, queue_client: redis.Redis, workers_required: int):
    """
    Recursive client for Workers, Tasks & AsyncIO's Event Loop that
    reacts on QueueAction names.
    :param worker_queue:
    :param queue_client:
    :param workers_required:
    :return:
    """
    queue = worker_queue
    next_task = await get_task_from_queue_client(queue_client)
    action_name = ""
    try:
        action_name: str = next_task[1].decode("utf-8")
        logger.debug(f"QueueAction name: {action_name}")
    except UnicodeDecodeError:
        pass

    if action_name == QueueActions.START.name:
        for i in range(workers_required):
            task = asyncio.create_task(worker(queue, queue_client))
            tasks.append(task)
        await client(worker_queue, queue_client, workers_required)

    elif action_name == QueueActions.STOP.name:
        await queue.join()
        for task in tasks:
            task.cancel()
        # Wait until all worker tasks are cancelled
        await asyncio.gather(*tasks, return_exceptions=True)
        event_loop = asyncio.get_running_loop()
        event_loop.stop()

    elif action_name == QueueActions.IDLE.name:
        # We have the option to stop & close the event loop here
        # OR pass execution context to another aspect of the library
        pass

    else:
        uow_metadata = await deserialize_task(next_task[1])
        serialized_uow: List[Callable, List[Any]] = await deserialize_task(uow_metadata["serialized_uow"])
        unit_of_work = {
            "function": serialized_uow[0],
            "args": serialized_uow[1],
        }

        uow_metadata["unit_of_work"] = unit_of_work
        # Push unit of work metada dict into the asyncio queue
        queue.put_nowait(uow_metadata)
        await client(worker_queue, queue_client, workers_required)
