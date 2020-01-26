import asyncio
import pytest
import redis

from pytask_io.task_queue import serialize_unit_of_work
from tests.mock_uow import send_email
from tests.fixtures import event_loop
from pytask_io.worker import worker

r = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
)

#
# def test_worker(event_loop):
#     dumped_uow = serialize_unit_of_work(send_email, ["Hello", 1])
#     r.lpush("tasks", dumped_uow)
#     r.lpush("tasks", dumped_uow)
#     r.lpush("tasks", dumped_uow)
#
#     queue = asyncio.Queue()
#
#     assert {} == event_loop.run_until_complete(worker(queue))
