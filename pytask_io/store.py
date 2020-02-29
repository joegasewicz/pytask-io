import redis
from typing import Any, Dict
from pytask_io.logger import logger

from pytask_io.utils import (
    serialize_store_data,
    get_datetime_now,
    serialize_unit_of_work,
    deserialize_store_data_sync,
)


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
        "unit_of_work": {},
        "serialized_uow": serialized_uow,
        "serialized_result": "",
        "result_exec_date": ""
    }
    return uow_metadata


def _create_store_index(store: Any):
    store.incr("task_auto_index")
    current_index = store.get("task_auto_index").decode("utf-8")
    return current_index


def _create_store_key(current_index, store, serialized_data: bytes = None) -> Dict[str, Any]:
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


def init_unit_of_work(q, store, unit_of_work, *args) -> Dict[str, Any]:
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
    uow_metadata = _create_store_key(
        _create_store_index(store),
        store,
        serialized_uow
    )

    # Unit of work gets push onto the task queue & metadata gets updated
    serialized_uow_meta = serialize_store_data(uow_metadata)
    # LPUSH returns the queue length after the push operation
    queue_length = q.lpush(_QUEUE_NAME, serialized_uow_meta)
    uow_metadata["queue_created"] = get_datetime_now()
    uow_metadata["queue_length"] = queue_length
    # Returns metadata back to the caller / user.
    return uow_metadata


def get_uow_from_store(store, uow_key: str) -> Dict[str, Any]:
    """
    Synchronous client version of get_uow_from_store
    :param store:
    :param uow_key:
    :return:
    """
    uow_store_metadata = store.get(uow_key)
    # TODO uow_store_result is always ''
    result = deserialize_store_data_sync(uow_store_metadata)

    if not result:
        raise ValueError(
            f"[PYTASKIO ValueError]: Could not get unit of work with "
            f"key of {uow_key} from store. Did you pass the correct "
            f"value to `PyTaskIO.get_task` ?"
        )
    else:
        return result


async def add_uof_result_to_store(executed_uow: Any, uow_metadata: Dict[str, Any], store) -> None:
    now = get_datetime_now()

    # Add serialized results to store
    uow_metadata["serialized_result"] = executed_uow
    uow_metadata["store_updated"] = now
    uow_metadata["result_exec_date"] = now

    serialized_metadata = serialize_store_data(uow_metadata)

    update_success = store.set(uow_metadata["store_name"], serialized_metadata)
    if not update_success:
        logger.error("PyTaskIO Error: Store was unsuccessful updating meta for unit of work.")
