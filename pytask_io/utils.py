import asyncio
import redis
import dill
from typing import List, Any, Tuple, Callable
from datetime import datetime
from warnings import warn
import concurrent.futures

def serialize_unit_of_work(unit_of_work: Any, *args) -> bytes:
    """
    Serializes a unit of work & returns the results
    This will return a new function with the error back to the user
    with a warning.
    :param unit_of_work
    :param args:
    :return:
    """
    try:
        serialized_uow = dill.dumps((unit_of_work, [*args]))
        return serialized_uow
    except TypeError as err:
        warn(
            "[PYTASK_IO WARNING] Task could not be serialized! "
            "PyTaskIO not support frame, generator, traceback & context objects."
        )
        def pytask_io_err(err):
            return err
        return dill.dumps((pytask_io_err, err))


def serialize_store_data(store_data: Any) -> bytes:
    """
    Serializes a unit of work & returns the results
    :param unit_of_work:s
    :param args:
    :return:
    """
    serialized_uow = dill.dumps((store_data))
    return serialized_uow


async def get_task_from_queue_client(q: redis.Redis) -> Tuple[Callable, List]:  # TODO correct return type
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError as err:
        raise RuntimeError(f"PyTaskIO: {err}")
    result = await current_loop.run_in_executor(None, q.brpop, "pytaskio_queue") # TODO pytaskio_queue -get from global
    return result


async def deserialize_task(task_data: Any):
    try:
        current_loop = asyncio.get_event_loop()
    except RuntimeError as err:
        raise RuntimeError(f"PyTaskIO: {err}")
    try:
        result = await current_loop.run_in_executor(None, dill.loads, task_data)
        return result
    except RuntimeError as err:
        raise Exception(err)



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


def deserialize_store_data_sync(task_data: Any):
    """
    Synchronous version of deserialize_store_data
    :param task_data:
    :return:
    """
    deserialized_uow = dill.loads(task_data)
    return deserialized_uow


def get_datetime_now():
    now = datetime.now()
    return now.strftime("%d/%m/%y %H:%M:%S")
