import asyncio
import redis
import marshal
import base64
from typing import Callable, List, Any

# --------------------------------------
#    Public functions
# --------------------------------------


def create_task_queue(host: str = "localhost", port: int = 6379, db: int = 0) -> redis.Redis:
    return redis.Redis(
        host=host,
        port=port,
        db=db,
    )


def serialize_unit_of_work(unit_of_work: Callable, *args):
    function_str = marshal.dumps(unit_of_work)
    args_str = marshal.dumps(*args)
    json = {
        "function": function_str,
        "args": args_str,
    }

    return json


# --------------------------------------
#   Private functions
# --------------------------------------
