import redis
import asyncio
from typing import Dict


def connect_to_store(host: str = "localhost", port: int = 6379, db: int = 0) -> redis.Redis:
    return redis.Redis(
        host=host,
        port=port,
        db=db,
    )


async def set_cmds(queue_client, key: str, value: Dict):
    q = asyncio.get_event_loop()
    pipe = queue_client.pipeline()
    result = pipe.set(key, value).incr("auto_number").execute()
    test =  await q.run_in_executor(None, result)
    print("TIIIII -----------> ", test)
    return test


async def add_uof_result_to_store(queue_client, key: str, value: Dict):
    # If the store being used is redis:
    result = await set_cmds(queue_client, key, value)
    return result