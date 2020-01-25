from pytask_io.client import client
from pytask_io.event_loop import event_loop
from tests.mock_uow import send_email

units_of_work = [send_email]


def test_client():
    result = client(units_of_work)
    assert result == {}