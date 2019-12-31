import asyncio
import logging
from typing import Any, Callable, Union, List
import time



class _Config:
    def __init__(self):
        pass


class _Log:
    """Simple wrapper for logging lib"""
    def __init__(self, logger, level="INFO"):
        self.log = logger
        self.log.basicConfig(level=level)

    def info(self, msg):
        self.log.basicConfig(level=self.log.INFO)
        self.log.info(f"[PYTASK_IO] {msg}")


class PyTaskIO:
    """
        Main class
    """
    def __init__(self):
        self.log = _Log(logging, "INFO")


if __name__ == "__main__":


    loop = asyncio.new_event_loop()
    # attach new loop the event loop's policy's watcher
    asyncio.set_event_loop(loop)

    def create_tasks(user_tasks: List):
        for fnc, args in user_tasks:
            async def _task(args):
                return fnc(args)
            loop.create_task(_task(args))


    def test_one(msg: str):
        time.sleep(5)
        print(msg)

    def test_two(msg: str):
        time.sleep(1)
        print(msg)

    create_tasks([
        (test_one, "Hello Joe 1!!!!"),
        (test_two, "Hello Joe 2!!!!")
    ])

    try:
        loop.run_forever()
    finally:
        loop.close()



