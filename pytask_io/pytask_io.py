import asyncio
from typing import List, Callable
import redis

from pytask_io.client import client
from pytask_io.task_queue import create_task_queue, serialize_unit_of_work


class PyTaskIO:

    units_of_work: List[Callable]
    queue_client: redis.Redis

    def __init__(self, *args, **kwargs):
        self.init_app()

    def add_unit_of_work(self, unit_of_work, *args) -> None:
        """
        Adds units of work to the queue client
        :param unit_of_work: A callable / executable Python function
        :param args: The list of arguments required by `unit_of_work`
        :return:
        """
        dumped_uow = serialize_unit_of_work(unit_of_work, *args)
        self.queue_client.lpush("tasks", dumped_uow)

    def init_app(self):
        self.run_event_loop()
        self.queue_client = create_task_queue()

    def run_event_loop(self):
        asyncio.run(client())
