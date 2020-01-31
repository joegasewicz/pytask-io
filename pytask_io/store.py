import redis
import asyncio
from typing import Dict

from pytask_io.utils import serialize_store_data


def connect_to_store(host: str = "localhost", port: int = 6379, db: int = 0) -> redis.Redis:
    return redis.Redis(
        host=host,
        port=port,
        db=db,
    )


async def set_cmds(queue_client, serial_data: bytes):
    queue_client.incr("task_auto_index")
    current_index = queue_client.get("task_auto_index").decode("utf-8")
    data_with_index = {
        **{"results": serial_data},
        "task_id": current_index,
    }
    current_loop = asyncio.get_event_loop()
    serialized_data = await current_loop.run_in_executor(None, serialize_store_data, data_with_index)

    result = queue_client.set(f"uow_result_#{current_index}", serialized_data)

    return result


async def add_uof_result_to_store(queue_client, serial_data: bytes):
    # If the store being used is redis:
    result = await set_cmds(queue_clisent, serial_data)
    return result