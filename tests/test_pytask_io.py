import asyncio
import threading
import time

import pytest
import redis
from freezegun import freeze_time

from pytask_io import PyTaskIO
from pytask_io.client import client
from pytask_io.utils import (
    deserialize_store_data_sync,
    serialize_store_data,
    serialize_unit_of_work,
)
from tests.mock_uow import send_email


class TestPyTaskIO:
    def setup_method(self):
        """Set up redis client and PyTaskIO"""
        self.r = redis.Redis(host="localhost", port=6379, db=0)
        self.pytask = PyTaskIO(
            store_host="localhost", store_port=6379, store_db=0, workers=1
        )
        self.pytask.run()

    def teardown_method(self):
        """Flush all from the store and close event loop"""
        self.r.flushall()
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.close()
        self.pytask.stop()

    def test_init(self):
        """Test keywords assignment to attributes in __init__"""
        assert self.pytask.store_host == "localhost"
        assert self.pytask.store_port == 6379
        assert self.pytask.store_db == 0
        assert self.pytask.workers == 1

    def test_init_defaults_fallback(self):
        """Ensure PyTaskIO fallbacks to defaults if no options are passed."""
        pytask = PyTaskIO()

        assert pytask.store_host == PyTaskIO.store_host
        assert pytask.store_port == PyTaskIO.store_port
        assert pytask.store_db == PyTaskIO.store_db
        assert pytask.workers == PyTaskIO.workers

    def test_run(self):
        """Ensure a thread is spawned and connection to host is made"""
        assert len(threading.enumerate()) == 2

        new_thread = threading.enumerate()[1]

        assert new_thread.is_alive() is True
        assert new_thread.daemon is True
        assert new_thread.name == "event_loop"
        assert new_thread._target == self.pytask._run_event_loop

    @freeze_time("1955-11-12 12:00:00")
    def test_add_task(self):
        """Ensure a task is correctly added and result can be fetched for queue storage."""

        result = self.pytask.add_task(send_email, "Hello")

        assert result == {
            "store_type": "redis",
            "store_name": "uow_result_#1",
            "store_index": "1",
            "store_db": 0,
            "store_created": "12/11/55 12:00:00",
            "store_updated": "",
            "queue_type": "redis",
            "queue_name": "pytaskio_queue",
            "queue_length": 1,
            "queue_db": 0,
            "queue_created": "12/11/55 12:00:00",
            "queue_updated": "",
            "unit_of_work": {},
            "serialized_uow": b"\x80\x04\x95.\x00\x00\x00\x00\x00\x00\x00\x8c\x0etests.mock_uow\x94\x8c\nsend_email\x94\x93\x94]\x94\x8c\x05Hello\x94a\x86\x94.",
            "serialized_result": "",
            "result_exec_date": "",
        }
        # Ensure proper task data in queue storage
        # This imits `poll_for_task` because it's not working currently.
        while True:
            result = deserialize_store_data_sync(
                self.pytask.queue_store.get("uow_result_#1")
            )
            if result.get("serialized_result"):
                break

        assert result == {
            "store_type": "redis",
            "store_name": "uow_result_#1",
            "store_index": "1",
            "store_db": 0,
            "store_created": "12/11/55 12:00:00",
            "store_updated": "12/11/55 12:00:00",
            "queue_type": "redis",
            "queue_name": "pytaskio_queue",
            "queue_length": "",
            "queue_db": 0,
            "queue_created": "",
            "queue_updated": "",
            "unit_of_work": {"function": send_email, "args": ["Hello"]},
            "serialized_uow": b"\x80\x04\x95.\x00\x00\x00\x00\x00\x00\x00\x8c\x0etests.mock_uow\x94\x8c\nsend_email\x94\x93\x94]\x94\x8c\x05Hello\x94a\x86\x94.",
            "serialized_result": "Hello",
            "result_exec_date": "12/11/55 12:00:00",
        }

    def test_poll_for_task(self):
        # TODO: Fix
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
        # assert self.pytask.poll_for_task(task_meta, tries=1, interval=1) == expected

    def test_get_results(self):
        # TODO: Fix
        def send_email_quick(msg):
            return msg

        metadata = self.pytask.add_task(send_email_quick, "Hello Joe 1")
        time.sleep(3)

        # assert metadata == {}
        # pytask_io.stop()
        metadata = {"store_name": "uow_result_#1"}
        result = self.pytask.get_task(metadata)

        assert result["serialized_result"] == "Hello Joe 1"

    def test_add_unit_of_work(self):

        meta = self.pytask.add_task(send_email, "Hello world!")
        # TODO: Fix this, `brpop` is blocking as key `tasks` is not present.
        # assert serialize_unit_of_work(send_email, "Hello world!") in self.r.brpop("tasks")

        # Test if the uow is on the queue
        # Test if the uow is on the store

        # assert asyncio.get_running_loop() == True