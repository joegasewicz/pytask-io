import asyncio
import redis
import dill
from typing import Callable, List, Any, Tuple, Dict

from pytask_io.client import deserialize_task

# --------------------------------------
#    Public functions
# --------------------------------------


def create_task_queue(host: str = "localhost", port: int = 6379, db: int = 0) -> redis.Redis:
    return redis.Redis(
        host=host,
        port=port,
        db=db,
    )


def serialize_unit_of_work(unit_of_work: Callable, *args) -> bytes:
    """
    Serializes a unit of work & returns the results
    :param unit_of_work:s
    :param args:
    :return:
    """
    serialized_uow = dill.dumps((unit_of_work, [*args]))
    return serialized_uow


async def pole_for_store_results(queue_client: redis.Redis, task_meta: Dict, tries: int, interval: int):
    list_name = task_meta.get("list_name")
    task_index = task_meta.get("task_index")
    dumped = None
    if interval:
        while tries > 0:
            current_loop = asyncio.get_running_loop()
            result = await current_loop.run_in_executor(queue_client.lindex(list_name, task_index))
            dumped = await deserialize_task(result[0])
            print("here------ ", dumped)
            if result:
                break
            await asyncio.sleep(interval)
    return dumped

# --------------------------------------
#   Private functions
# --------------------------------------
