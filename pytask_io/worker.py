import asyncio


async def worker_one(queue: asyncio.Queue):
    result = await queue.get()
    await asyncio.sleep(3)
    print("Worker: 1")
    print(result)
    queue.task_done()


async def worker_two(queue: asyncio.Queue):
    result = await queue.get()
    await asyncio.sleep(1)
    print("Worker: 2")
    print(result)
    queue.task_done()


async def worker_three(queue: asyncio.Queue):
    result = await queue.get()
    await asyncio.sleep(1)
    print("Worker: 3")
    print(result)
    queue.task_done()


async def main():

    # ----- Queue ------
    queue = asyncio.Queue()

    queue.put_nowait("1 ------>>>> ")
    queue.put_nowait("2 ------>>>> ")
    queue.put_nowait("3 ------>>>> ")

    # --- worker tasks to process the queue concurrently
    # TODO Producer here ------>
    task_one = asyncio.create_task(worker_one(queue))
    task_two = asyncio.create_task(worker_two(queue))
    task_three = asyncio.create_task(worker_three(queue))

    # wait until queue is fully processed
    # TODO Consumer here ---->
    await queue.join()

    # Cancel all worker tasks.
    task_one.cancel()
    task_two.cancel()
    task_three.cancel()

    # wait until all worker tasks are cancelled
    await asyncio.gather(task_one, task_two, task_three)
    print("QUEUE COMPLETE")

asyncio.run(main(), debug=True)
