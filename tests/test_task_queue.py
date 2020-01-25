import pytest
import dill

from pytask_io.task_queue import serialize_unit_of_work
from tests.mock_uow import send_email


def test_serialize_unit_of_work():
    result = serialize_unit_of_work(send_email, "Hello", 1)
    assert isinstance(result, bytes)
    assert (send_email, ["Hello", 1]) == dill.loads(result)
