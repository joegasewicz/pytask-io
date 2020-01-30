import redis
import asyncio
from typing import Dict


def connect_to_store(host: str = "localhost", port: int = 6379, db: int = 0) -> redis.Redis:
    return redis.Redis(
        host=host,
        port=port,
        db=db,
    )


def set_cmds(key: str, value: Dict):
    r = connect_to_store().queue_store
    pipe = r.queue_store.pipeline()
    return pipe.set(key, value).incr("auto_number").execute()


async def add_uof_result_to_store(key: str, value: Dict):
    # If the store being used is redis:
    event_loop = asyncio.get_event_loop()
    result = await event_loop.run_in_executor(None, set_cmds, *(key, value))
    print(f"set --------> {result}")
    return result