import asyncio
import threading
import time

import pytest
import redis
from freezegun import freeze_time
from pytask_io.exceptions import NotReadyException

from pytask_io import PyTaskIO
from pytask_io.client import client
from pytask_io.utils import (
    deserialize_store_data_sync,
    serialize_store_data,
    serialize_unit_of_work,
)
from tests.mock_uow import send_email
import dill


class TestPyTaskIO:
    def setup_method(self):
        """Set up redis client and PyTaskIO"""
        self.r = redis.Redis(host="localhost", port=6379, db=0)

    def teardown_method(self):
        """Flush all from the store and close event loop"""
        self.r.flushall()
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.close()

    def test_init(self):
        """Test keywords assignment to attributes in __init__"""
        pytask = PyTaskIO(
            store_host="localhost", store_port=6379, store_db=0, workers=1
        )
        assert pytask.store_host == "localhost"
        assert pytask.store_port == 6379
        assert pytask.store_db == 0
        assert pytask.workers == 1

        assert pytask.queue_client is None
        assert pytask.queue_store is None
        assert pytask.loop_thread is None
        assert pytask.main_loop is None

    def test_init_defaults_fallback(self):
        """Ensure PyTaskIO fallbacks to defaults if no options are passed."""
        pytask = PyTaskIO()

        assert pytask.store_host == PyTaskIO.store_host
        assert pytask.store_port == PyTaskIO.store_port
        assert pytask.store_db == PyTaskIO.store_db
        assert pytask.workers == PyTaskIO.workers

    def test_run(self):
        """Ensure a thread is spawned and connection to host is made"""
        pytask = PyTaskIO(
            store_host="localhost", store_port=6379, store_db=0, workers=1
        )
        pytask.run()

        assert len(threading.enumerate()) == 2

        new_thread = threading.enumerate()[1]

        assert new_thread.is_alive() is True
        assert new_thread.daemon is True
        assert new_thread.name == "event_loop"
        pytask.stop()

    @freeze_time("1955-11-12 12:00:00")
    def test_add_task(self):
        """Ensure a task is correctly added and result can be fetched for queue storage."""
        pytask = PyTaskIO(
            store_host="localhost", store_port=6379, store_db=0, workers=1
        )
        pytask.run()

        result = pytask.add_task(send_email, "Hello")
        assert result["store_type"] == "redis"
        assert result["store_name"] == "uow_result_#1"
        assert result["store_index"] == "1"
        assert result["store_db"] == 0
        assert result["store_created"] == "12/11/55 12:00:00"
        assert result["store_updated"] == ""
        assert result["queue_type"] == "redis"
        assert result["queue_name"] == "pytaskio_queue"
        assert result["queue_length"] == 2
        assert result["queue_db"] == 0
        assert result["queue_created"] == "12/11/55 12:00:00"
        assert result["queue_updated"] == ""
        assert result["unit_of_work"] == {}
        assert result["serialized_uow"] == b"\x80\x04\x95.\x00\x00\x00\x00\x00\x00\x00\x8c\x0etests.mock_uow\x94\x8c\nsend_email\x94\x93\x94]\x94\x8c\x05Hello\x94a\x86\x94."
        assert result["serialized_result"] == ""
        assert result["result_exec_date"] == ""
        pytask.stop()

    def test_poll_for_task(self):
        data = {"value_1": 1, "values_2": "hello"}

        dumped_data = serialize_store_data(data)
        self.r.lpush("task_result", dumped_data)

        expected = {
            "data": data,
            "list_name": "task_result",
            "task_index": 0,
        }

        task_meta = {
            "list_name": "task_result",
            "task_index": 0,
        }
        pytask = PyTaskIO(
            store_host="localhost", store_port=6379, store_db=0, workers=1
        )
        pytask.run()
        assert pytask.poll_for_task(task_meta, tries=1, interval=1) == expected
        pytask.stop()

    def test_get_task_serialized_uow(self):
        pytask = PyTaskIO(
            store_host="localhost", store_port=6379, store_db=0, workers=1
        )
        pytask.run()
        def send_email_quick(msg):
            return msg

        metadata = pytask.add_task(send_email_quick, "Hello Joe 1")
        time.sleep(1)

        assert metadata is not None
        result = pytask.get_task(metadata)
        serialized_fn = dill.dumps((send_email_quick, ["Hello Joe 1"]))
        assert result["serialized_uow"] == serialized_fn
        pytask.stop()

    def test_add_unit_of_work(self):

        # meta = pytask.add_task(send_email, "Hello world!")
        # TODO: Fix this, `brpop` is blocking as key `tasks` is not present.
        # assert serialize_unit_of_work(send_email, "Hello world!") in self.r.brpop("tasks")

        # Test if the uow is on the queue
        # Test if the uow is on the store

        # assert asyncio.get_running_loop() == True
        pass
