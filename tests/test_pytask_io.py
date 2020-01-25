import asyncio
import pytest
import redis

from pytask_io.task_queue import serialize_unit_of_work

r = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
)

from pytask_io.pytask_io import PyTaskIO
from tests.mock_uow import send_email


class TestPyTaskIO:

    def setup_method(self):
        pass

    def teardown_method(self):
        """Flush all from the store"""
        r.flushall()

    def test_add_unit_of_work(self):

        pytask_io = PyTaskIO()
        pytask_io.add_unit_of_work(send_email, "Hello world!", 1)

        assert serialize_unit_of_work(send_email, "Hello world!", 1) in r.brpop("tasks")


        # Test if the uow is on the queue
        # Test if the uow is on the store

        # assert asyncio.get_running_loop() == True
