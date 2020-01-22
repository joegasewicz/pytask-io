import asyncio
from toolz.functoolz import pipe, compose, curry


async def worker_one(queue: asyncio.Queue):
    result = await queue.get()
    await asyncio.sleep(3)
    print("Worker: 1")
    print(result())


async def worker_two(queue: asyncio.Queue):
    result = await queue.get()
    await asyncio.sleep(1)
    print("Worker: 2")
    print(result())


async def worker_three(queue: asyncio.Queue):
    result = await queue.get()
    await asyncio.sleep(1)
    print("Worker: 3")
    print(result())

def test_fnc_one():
    return f"1 ------>>>> "

def test_fnc_two():
    return f"2 ------>>>> "

def test_fnc_three():
    return f"3 ------>>>> "

def test_fnc_four():
    return f"4 ------>>>> "


async def producer(queue: asyncio.Queue):
    queue.put_nowait(test_fnc_one)
    queue.put_nowait(test_fnc_two)
    queue.put_nowait(test_fnc_three)
    queue.put_nowait(test_fnc_four)


async def consumer(queue: asyncio.Queue):
    queue.task_done()


async def queue():

    # ----- Queue ------
    queue = asyncio.Queue()

    # Producer adds items to the queue
    await producer(queue)

    # --- worker tasks to process the queue concurrently

    items = [
        worker_one,
        worker_two,
        worker_three,
        worker_two
    ]

    # task_one = compose(
    #     asyncio.create_task,
    #     consumer,
    #     worker_one,
    # )(queue)

    all_tasks = compose(curry(map)(
        lambda worker: pipe(
            queue,
            worker,
            consumer,
            asyncio.create_task,
        )
    ))(items)

    # wait until queue is fully processed
    # TODO Consumer here ---->
    await queue.join()

    # Cancel all worker tasks.
    compose(
        curry(map),
        lambda t: t.cancel(),
    )(all_tasks)

    # wait until all worker tasks are cancelled
    await asyncio.gather(*all_tasks)
    print("QUEUE COMPLETE")

asyncio.run(queue(), debug=True)
