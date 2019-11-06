import asyncio
import logging
from typing import Any, Callable, Union
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

    def _create_task(self, fnc: Union):
        """
        :param fnc: Coroutine to run
        :return:
        """
        task = asyncio.create_task(
            fnc,
        )
        self.log.info("task dispatched.")
        return task

    def on_task_complete(self, task_id=None):
        """
        :param task_id:
        :return:
        """
        result = None
        return result

    def on_task_error(self):
        pass

    def start(self):
        """
        Starts the asynio event loop
        :return:
        """
        self.log.info("is running.")
        asyncio.run()

    async def dispatch_task(self, fnc: Callable):
        result = await self._create_task()
        self.log.info("All tasks completed.")
        return result


if __name__ == "__main__":

    async def test_one(msg):
        return msg

    py_task_io = PyTaskIO()
    py_task_io.start()

    task1 = py_task_io.dispatch_task(test_one, "TEST1")
    print(f"result ------> {task1}")

    # res = py_task_io.on_task_complete(task1)

    # print(f"result ------> {res}")

    #  py_task_io.on_task_complete()

