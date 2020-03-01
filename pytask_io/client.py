import asyncio
import redis


from pytask_io.worker_queue import create_worker_queue
from pytask_io.worker import worker
from pytask_io.utils import get_task_from_queue_client, deserialize_task


async def client(queue_client: redis.Redis, workers_required: int):
    """
        Client for Workers, Tasks & AsyncIO's Event Loop
    """
    queue = create_worker_queue()
    await asyncio.sleep(1)  # TODO this is a quick fix
    next_task = await get_task_from_queue_client(queue_client)
    uow_metadata = await deserialize_task(next_task[1])
    serialized_uow = await deserialize_task(uow_metadata["serialized_uow"])
    unit_of_work = {
        "function": serialized_uow[0],
        "args": serialized_uow[1],
    }

    uow_metadata["unit_of_work"] = unit_of_work

    # Push unit of work metada dict into the asyncio queue
    queue.put_nowait(uow_metadata)

    # Create `3` workers tasks to process the queue concurrently
    tasks = []
    for i in range(workers_required):
        task = asyncio.create_task(worker(queue, queue_client))
        tasks.append(task)
    # Wait until queue is fully processed
    await queue.join()

    # Cancel tasks
    for task in tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled
    await asyncio.gather(*tasks, return_exceptions=True)
