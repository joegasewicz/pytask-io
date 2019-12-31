"""
    - Create and run an asyncio event loop
    - dipatch corouties

"""
from .server import PyTaskIO

if __name__ == "__main__":
    py_task_io = PyTaskIO()
    py_task_io.start()
