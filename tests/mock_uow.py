"""
 Mock Unit Of Work Functions
"""
import time


def send_email(arg1: str, arg2: int):
    time.sleep(1)
    return [arg1, arg2]

