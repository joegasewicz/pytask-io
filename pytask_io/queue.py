import asyncio
from toolz.functoolz import pipe, compose, curry
from typing import List, Any
import time


def test_fnc_one(arg):
    time.sleep(1)
    return arg


def test_fnc_two(arg):
    time.sleep(1)
    return arg


def test_fnc_three(arg):
    time.sleep(2)
    return arg


def test_fnc_four(arg):
    time.sleep(1)
    return arg


async def produce(queue: asyncio.Queue, items: List[Any]):
    for item in items:
        fnc, args = item
        coroutine_from_item = asyncio.coroutine(fnc)
        item = asyncio.create_task(coroutine_from_item(args))
        await queue.put(item)


async def consume(queue: asyncio.Queue):
    while True:
        result = await queue.get()
        result = await result
        print(f"here-------> {result}")
        queue.task_done()


async def main():

    # ----- Queue ------
    queue = asyncio.Queue()

    items = [
        (test_fnc_one, "function 1"),
        (test_fnc_two, "function21"),
        (test_fnc_three, "function 3"),
        (test_fnc_four, "function 4")
    ]

    items_list = [items, items, items]

    producers = compose(
        list,
        curry(map)(
            lambda item: pipe(
                item,
                lambda i: asyncio.create_task(produce(queue, i))
            )
        )
    )(items_list)

    consumer = asyncio.ensure_future(consume(queue))

    # Get tasks from queue & Gather to run tasks concurrently

    await asyncio.gather(*producers)

    await queue.join()

    consumer.cancel()

    print("QUEUE COMPLETE")

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
