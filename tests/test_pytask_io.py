import asyncio
import pytest
import redis

from pytask_io.task_queue import serialize_unit_of_work, create_task_queue
from pytask_io.client import client
from tests.mock_uow import send_email
from tests.fixtures import event_loop


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
        pytask_io._add_unit_of_work(send_email, "Hello world!", 1)

        assert serialize_unit_of_work(send_email, "Hello world!", 1) in r.brpop("tasks")


        # Test if the uow is on the queue
        # Test if the uow is on the store

        # assert asyncio.get_running_loop() == True
    @pytest.mark.y
    def test_init(self, event_loop):
        dumped_uow = serialize_unit_of_work(send_email, "Hello", 1)
        r.lpush("tasks", dumped_uow)
        r.lpush("tasks", dumped_uow)
        r.lpush("tasks", dumped_uow)

        queue_client = create_task_queue()

        assert {} == event_loop.run_until_complete(client(queue_client))
