import asyncio
import redis
from typing import Dict


from pytask_io.utils import deserialize_store_data


async def poll_for_store_results(queue_store: redis.Redis, task_meta: Dict, tries: int, interval: int):
    """
    Streams back results to
    :param queue_store:
    :param task_meta:
    :param tries:
    :param interval:
    :return:
    """
    list_name = task_meta.get("list_name")
    task_index = task_meta.get("task_index")
    dumped = None
    if interval:
        while tries > 0:
            current_loop = asyncio.get_running_loop()

            result = await current_loop.run_in_executor(None, queue_store.lindex, *[list_name, task_index])
            if result:
                dumped = await deserialize_store_data(result)
                tries -= 1
                break
            elif not result:
                break
            else:
                await asyncio.sleep(interval)

    return dumped
