"""
    - Takes a WSGI application interface  & provides async background tasks
    - Should be compatible with any WSGI applcation interface
    - Dispatch the task from a request handler with a task id
    - When the task is complete it returns a message container the task id plus any payload
    - Provide decorators that enhance functions to subscribe to tasks with task ids
"""
from .server import PyTaskIO

if __name__ == "__main__":
    py_task_io = PyTaskIO()
    py_task_io.start()
