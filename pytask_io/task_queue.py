import asyncio
import redis
import dill
from typing import Callable, List, Any, Tuple

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


# --------------------------------------
#   Private functions
# --------------------------------------
