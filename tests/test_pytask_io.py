import pytest
import redis

from pytask_io.client import client
from pytask_io.utils import deserialize_task, serialize_store_data, serialize_unit_of_work
from pytask_io.pytask_io import PyTaskIO, connect_to_store
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
        # loop = asyncio.get_event_loop()
        # if loop.is_running():
        #     loop.close()

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
        queue_client = connect_to_store()

        results = r.brpop("tasks")
        event_loop.run_until_complete(client(queue_client))

        fnc, args = event_loop.run_until_complete(deserialize_task(results[1]))
        assert ["Hello", 1] == fnc(*args)


    def test_add_task(self):
        py_task = PyTaskIO()
        expected = {
            "list_name": "tasks",
            "task_index": 2,
        }

        result = py_task.add_task(send_email, "Hello")

        print("here------> ", result)
        # assert isinstance(result["serialized_uow"], bytes)
        # assert result["store_type"] is "redis"
        # assert result["store_name'"] is "uow_result_#1"
        # assert result["store_index"] is 1
        # assert result["store_db"] is 0
        # assert result["store_created"] is not "None"
        # assert result["store_updated"] is "None"
        # assert result["queue_type"] is "redis"
        # assert result["queue_name"] is "pytaskio_queue"
        # assert result["queue_length"] is 1
        # assert result["queue_db"] is 0
        # assert result["queue_created"] is not "None"
        # assert result["queue_updated"] is "None"
        # assert result["serialized_uow"] is  b"\x80\x03ctests.mock_uow\nsend_email\nq\x00]q\x01X\x05\x00\x00\x00Helloq\x02a\x86q\x03."
        """
        Now test the Redis Store to assert that the data there
        has been updated with the unit of work's results
        """


        assert False is True


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

    @pytest.mark.e
    def test_get_results(self):

        def send_email_quick(msg):
            return msg

        pytask = PyTaskIO()
        pytask.run()
        metadata = pytask.add_task(send_email_quick, "Hello Joe 1")

        assert metadata == {}
        pytask.stop()
        metadata = {"store_name": "uow_result_#4"}
        result = pytask.get_task(metadata)

        assert result["serialized_result"] == "Hello Joe 1"