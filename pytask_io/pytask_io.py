import asyncio
from typing import List, Callable
import redis
import time
from threading import Thread
from typing import Dict, Any, Union

from pytask_io.task_queue import (
    pole_for_store_results,
)
from pytask_io.logger import logger
from pytask_io.client import client
from pytask_io.utils import serialize_unit_of_work


def connect_to_store(host: str = "localhost", port: int = 6379, db: int = 0) -> redis.Redis:
    return redis.Redis(
        host=host,
        port=port,
        db=db,
    )


def send_email(arg1: str, arg2: int):
    time.sleep(1)
    return [arg1, arg2]


class PyTaskIO:

    units_of_work: List[Callable]
    queue_client: redis.Redis = connect_to_store()
    queue_store: redis.Redis = connect_to_store()
    loop_thread: Thread
    main_loop: asyncio.AbstractEventLoop
    pole_loop: asyncio.AbstractEventLoop
    task_results: str = "task_results"
    polled_result: Dict = None

    def __init__(self, *args, **kwargs):
        self.init_app()

    def _add_unit_of_work(self, unit_of_work, *args) -> int:
        """
        Adds units of work to the queue client
        :param unit_of_work: A callable / executable Python function
        :param args: The list of arguments required by `unit_of_work`
        :return:
        """
        dumped_uow = serialize_unit_of_work(unit_of_work, *args)
        return self.queue_client.lpush("tasks", dumped_uow)

    def init_app(self):
        threads = []
        self.loop_thread = Thread(name="event_loop", target=self.run_event_loop, daemon=True)
        threads.append(self.loop_thread)
        self.loop_thread.start()

    def run_event_loop(self):
        self.main_loop = asyncio.new_event_loop()
        self.main_loop.create_task(client(self.queue_client))
        self.main_loop.run_forever()
        logger.info("asyncIO event loop running")

    def add_task(self, unit_of_work, *args) -> Dict[str, Any]:
        uow = self._add_unit_of_work(unit_of_work, *args)
        return {
            "list_name": "tasks",
            "task_index": uow,
        }

    def get_task(self, task_meta: Dict[str, Any]) -> Union[Dict[str, Any], bool]:
        """
        :param task_meta: Dict[str, Any] -
        :return Union[Dict, bool]: The result is non blocking
        """
        result = self.queue_client.lpop("task_results")
        return result

    def poll_for_task(self, task_meta: Dict, *args, **kwargs) -> Union[Dict[str, Any], bool]:
        """
        Blocking function to be used either with an async library or from a separate thread
        from the client application's main thread. Example::

        :param task_meta:
        :param args:
        :param kwargs:
        :return:
        """
        tries = kwargs.get("tries")
        interval = kwargs.get("interval")
        if tries:
            # Create event loop in new thread
            self.pole_loop = asyncio.get_event_loop()
            # Coroutine to pole store on event loop
            get_store_results = pole_for_store_results(self.queue_store, task_meta, interval, tries)
            asyncio.set_event_loop(self.pole_loop)
            self.polled_result = self.pole_loop.run_until_complete(get_store_results)
        if self.polled_result:
            task_result_data = {
                "data": self.polled_result,
                **task_meta,
            }
            return task_result_data
