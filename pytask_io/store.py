import redis
import asyncio
from typing import Any, Dict
from functools import partial
from pytask_io.logger import logger

from pytask_io.utils import serialize_store_data, get_datetime_now, serialize_unit_of_work


def _connect_to_store(host: str = "localhost", port: int = 6379, db: int = 0) -> redis.Redis:
    return redis.Redis(
        host=host,
        port=port,
        db=db,
    )


_store = _connect_to_store()
_QUEUE_NAME = "pytaskio_queue"


def _create_uow_metadata(uow_store_name: str, index: int, datetime_now: Any, serialized_uow: bytes):
    """
    Creates the main dict data structure for the library
    :param uow_store_name:
    :param index:
    :param datetime_now: Sets the datetime for the store
    :return:
    """
    uow_metadata = {
        "store_type": "redis",
        "store_name": uow_store_name,
        "store_index": index,
        "store_db": 0,
        "store_created": datetime_now,
        "store_updated": "",
        "queue_type": "redis",
        "queue_name": _QUEUE_NAME,
        "queue_length": "",
        "queue_db": 0,
        "queue_created": "",
        "queue_updated": "",
        "serialized_uow": serialized_uow,
        "serialized_result": "",
        "result_exec_date": ""
    }
    return uow_metadata


def _create_store_index(store: Any):
    store.incr("task_auto_index")
    current_index = store.get("task_auto_index").decode("utf-8")
    return current_index


def _create_store_key(store: Any, current_index, serialized_data: bytes = None) -> Dict[str, Any]:
    uow_store_name = f"uow_result_#{current_index}"
    init_success = store.set(uow_store_name, 0)
    if not init_success:
        logger.error("PyTaskIO Error: Store was unsuccessful creating registry for unit of work.")
    uow_metadata = _create_uow_metadata(
        uow_store_name,
        current_index,
        get_datetime_now(),
        serialized_data,
    )

    serialized_uow_meta = serialize_store_data(uow_metadata)
    update_success = store.set(uow_store_name, serialized_uow_meta)
    if not update_success:
        logger.error("PyTaskIO Error: Store was unsuccessful updating meta for unit of work.")
    return uow_metadata


"""
_init_unit_of_work_meta
    Initiates & returns the main metadata structure for the library.
    :param serialized_uow: The serialized unit of work to ber added
    to the task queue.
"""
_init_unit_of_work_meta = partial(
    _create_store_key,
    _store,
    _create_store_index(_store),
)


def init_unit_of_work(q, unit_of_work, *args) -> Dict[str, Any]:
    """
    Function used directly in PyTaskIO class.
    :param q: The client task queue
    :param unit_of_work: python code to run
    :param args: Unit of work arguments
    :return:
    """
    serialized_uow = serialize_unit_of_work(unit_of_work, *args)
    # The unit of work gets saved to the store in case the task
    # fails and the client / user wants to retry
    uow_metadata = _init_unit_of_work_meta(serialized_uow)

    # Unit of work gets push onto the task queue & metadata gets updated
    serialized_uow_meta = serialize_store_data(uow_metadata)
    # LPUSH returns the queue length after the push operation
    queue_length = q.lpush(_QUEUE_NAME, serialized_uow_meta)
    uow_metadata["queue_created"] = get_datetime_now()
    uow_metadata["queue_length"] = queue_length
    # Returns metadata back to the caller / user.
    return uow_metadata


async def get_uow_from_store(uow_key: str):
    current_loop = asyncio.get_running_loop()
    result = await current_loop.run_in_executor(None, _store.get, uow_key)
    if not result:
        logger.error(
            f"PYTASKIO ERROR: Could not get unit of work with "
            f"key of {uow_key} from store."
        )
    else:
        return result


async def add_uof_result_to_store(executed_uow: Any, uow_metadata: Dict[str, Any]):
    now = get_datetime_now()
    # Serialize results
    serialized_exec_uow = serialize_store_data(executed_uow)
    # Get the uow metadata from the store by the store_name
    metadata_from_store = get_uow_from_store(uow_metadata["store_name"])
    # Add serialized results to store
    metadata_from_store["serialized_result"] = serialized_exec_uow
    metadata_from_store["store_updated"] = now
    metadata_from_store["result_exec_date"] = now
