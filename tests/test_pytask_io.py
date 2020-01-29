import asyncio
import pytest
import redis
import pickle

from pytask_io.task_queue import serialize_unit_of_work, create_task_queue, serialize_store_data
from pytask_io.client import client, deserialize_task
from tests.mock_uow import send_email
from tests.fixtures import event_loop
from pytask_io.pytask_io import PyTaskIO
from tests.mock_uow import send_email


r = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
)




class TestPyTaskIO:

    def setup_method(self):
        pass

    def teardown_method(self):
        """Flush all from the store"""
        # r.flushall()

    def test_add_unit_of_work(self):

        pytask_io = PyTaskIO()
        pytask_io._add_unit_of_work(send_email, "Hello world!", 1)

        assert serialize_unit_of_work(send_email, "Hello world!", 1) in r.brpop("tasks")


        # Test if the uow is on the queue
        # Test if the uow is on the store

        # assert asyncio.get_running_loop() == True


    def test_init(self, event_loop):
        dumped_uow = serialize_unit_of_work(send_email, "Hello", 1)
        r.lpush("tasks", dumped_uow)
        queue_client = create_task_queue()

        results = r.brpop("tasks")
        event_loop.run_until_complete(client(queue_client))

        fnc, args = event_loop.run_until_complete(deserialize_task(results[1]))
        assert ["Hello", 1] == fnc(*args)

    def test_add_task(self):
        py_task = PyTaskIO()
        expected = {
            "list_name": "tasks",
            "task_index": 1,
        }
        assert expected == py_task.add_task(send_email, "Hello", 1)

    @pytest.mark.e
    def test_poll_for_task(self):
        data = {
            "value_1": 1,
            "values_2": "hello"
        }

        dumped_data = serialize_store_data(data)
        r.lpush("task_result", dumped_data)

        expected = {
            "data": data,
            "list_name": "task_result",
            "task_index": 0,
        }

        pytask = PyTaskIO()
        task_meta = {
            "list_name": "task_result",
            "task_index": 0,
        }
        assert pytask.poll_for_task(task_meta, tries=1, interval=1) == expected
