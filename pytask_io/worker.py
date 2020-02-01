import asyncio
from typing import List, Callable

from pytask_io.store import add_uof_result_to_store
from pytask_io.utils import serialize_store_data, deserialize_task

tasks = []


def get_tasks(task: List[Callable]):
    return task


async def worker(q: asyncio.Queue, queue_client):
    """
    - Worker
        - Observes task queue.
        - Fetches available task to run.
        - Tasks are run asynchronously on a single asyncIO event loop.
    """
    while True:
        raw_uow_metadata = await q.get()
        #
        current_loop = asyncio.get_running_loop()
        # Deserialize to Python Dict
        uow_metadata = await deserialize_task(raw_uow_metadata)
        uow_deserialize = await(uow_metadata["serialized_uow"])
        print(f"uow_deserialize -----> ", uow_deserialize)
        # Execute the unit of work & pass in the args
        executed_uow = await current_loop.run_in_executor(
            None,
            uow_deserialize[0],
            *uow_deserialize[1],
        )
        await add_uof_result_to_store(executed_uow, uow_metadata)
        q.task_done()
