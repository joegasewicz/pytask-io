import asyncio
from typing import List, Callable
import redis

from pytask_io.client import client
from pytask_io.task_queue import create_task_queue


class PyTaskIO:

    units_of_work: List[Callable]
    queue_client: redis.Redis

    def __init__(self, *args, **kwargs):
        self.init_app()

    def add_unit_of_work(self, unit_of_work, *args):
        """Add UOW to task queue"""



    def init_app(self):
        self.run_event_loop()
        self.queue_client = create_task_queue()

    def run_event_loop(self):
        asyncio.run(client(self.units_of_work))
