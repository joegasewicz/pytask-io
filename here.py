from pytask_io.pytask_io import PyTaskIO


def send_email(arg1):
    return "hello " + arg1

pytaskio = PyTaskIO()
pytaskio.add_task(send_email, "joe")