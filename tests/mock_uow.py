"""
 Mock Unit Of Work Functions
"""
import time


def send_email(arg1: str):
    print(f"FUNCTION EXECUTED!")
    return arg1

"""

def send_email(arg1, arg2):  time.sleep(1); return [arg1, arg2]

pytask.add_unit_of_work(send_email, "Hello", 1)
"""
