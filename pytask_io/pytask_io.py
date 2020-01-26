import asyncio
from typing import List, Callable
import redis
import time
from threading import Thread

from pytask_io.client import client
from pytask_io.task_queue import create_task_queue, serialize_unit_of_work
from pytask_io.logger import logger

def send_email(arg1: str, arg2: int):
    time.sleep(1)
    return [arg1, arg2]

class PyTaskIO:

    units_of_work: List[Callable]
    queue_client: redis.Redis
    loop_thread: Thread

    def __init__(self, *args, **kwargs):
        self.init_app()

    def _add_unit_of_work(self, unit_of_work, *args) -> None:
        """
        Adds units of work to the queue client
        :param unit_of_work: A callable / executable Python function
        :param args: The list of arguments required by `unit_of_work`
        :return:
        """
        dumped_uow = serialize_unit_of_work(unit_of_work, *args)
        self.queue_client.lpush("tasks", dumped_uow)

    def init_app(self):
        self.queue_client = create_task_queue()
        self.loop_thread = Thread(target=self.run_event_loop, daemon=True)

    def run_event_loop(self):
        current_loop = asyncio.get_event_loop()
        current_loop.create_task(client(self.queue_client))
        current_loop.run_forever()
        logger.info("asyncIO event loop running")

    def add_task(self, unit_of_work, *args):
        pass

