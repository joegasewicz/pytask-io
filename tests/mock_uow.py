"""
 Mock Unit Of Work Functions
"""
import time


def send_email(arg1: str, arg2: int):
    time.sleep(1)
    return {
        "key_1": arg1,
        "key_2": arg2,
    }

"""

def send_email(arg1, arg2):  time.sleep(1); return [arg1, arg2]

pytask.add_unit_of_work(send_email, "Hello", 1)
"""
