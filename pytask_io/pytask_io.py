import asyncio
from typing import List, Callable

from pytask_io.client import client


class PytaskIO:

    units_of_work: List[Callable]

    def __init__(self):
        self.init_app()

    def init_app(self):
        self.run_event_loop()

    def run_event_loop(self):
        asyncio.run(client(self.units_of_work))
