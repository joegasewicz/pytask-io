import asyncio
import pytest

from pytask_io.pytask_io import PyTaskIO
from tests.mock_uow import send_email


class TestPyTaskIO:

    def test_add_unit_of_work(self):

        pytask_io = PyTaskIO()
        result = pytask_io.add_unit_of_work(send_email, "Hello world!", 1)

        assert result is True

        # Test if the uow is on the queue
        # Test if the uow is on the store

        # assert asyncio.get_running_loop() == True
