"""
 Original pattern based on Hugo Troche's white paper:
 "The Task Pattern: A Design Pattern for Processing and Monitoring Long-Running Tasks"
 See: https://www.developerdotstar.com/mag/articles/troche_taskpattern.html
 This is based on .3 Advanced Task Pattern
"""

# class Queue(list):
#     """The queue"""
#     def __getitem__(self, item):
#         return list.__getitem__(self, item)

from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Callable
import asyncio
import time


class AbstractCommand(ABC):

    @abstractmethod
    def execute(self, args=None) -> None:
        pass


class Invoker:
    """Mediator / Invoker"""
    _queue: List[Tuple[AbstractCommand, Any]]

    def __init__(self, queue: List[Tuple[AbstractCommand, Any]]):
        self._queue = queue

    def set_command(self, command: AbstractCommand, args=None) -> None:
        self._queue.append((command, args))

    async def run(self) -> None:
        await asyncio.gather(*(command[0].execute(command[1]) for command in self._queue))


class AbstractTask(ABC):

    progress: int

    @abstractmethod
    def run(self, result: Any) -> None:
        pass

"""
    Concrete Commands
"""


class RunCommand(AbstractCommand):

    def __init__(self, task: AbstractTask):  # pass reciever commands here
        self.task = task

    async def execute(self, args=None) -> None:
        fnc = args
        """
        Execute the task now
        """
        result = await fnc()
        print("RUN_CMD starting ------> ")
        self.task.progress += 1
        print(f"Progress is ----> {self.task.progress}")
        self.task.run(result)
        print("RUN_CMD completed ------> ")


class ProgressCommand(AbstractCommand):

    def __init__(self, task: AbstractTask):  # pass reciever commands here
        self.task = task

    async def execute(self, args=None) -> None:
        print("PROGRESS_CMD starting ------> ")
        self.task.progress += 1
        await asyncio.sleep(1)
        print("PROGRESS_CMD completed ------> ")
        print(f"Progress is ----> {self.task.progress}")


class LongTask(AbstractTask):
    """Request"""

    progress: int = 0

    def run(self, result: Any) -> None:
        print(f"LONG TASK RESULT ------> {result}")


"""
    Task
"""


class PyTaskIO:

    _queue: List[Tuple[AbstractCommand, Any]]

    long_task: AbstractTask = LongTask()

    run_command: AbstractCommand

    progress_command: AbstractCommand

    """Task set up"""
    def __init__(self):
        self.run_command = RunCommand(self.long_task)
        self.progress_command = ProgressCommand(self.long_task)

        self._queue = []

    def run_task(self, task_fn: Any):
        """
        Init user task and add response to a queue ...?
        Maybe the run command can start the st
        """
        invoker = Invoker(self._queue)
        invoker.set_command(self.run_command, task_fn)
        invoker.set_command(self.progress_command)
        asyncio.run(invoker.run())


py_task = PyTaskIO()

# py_task.run_task()

def send_email():
    time.sleep(2)
    return "EMAIL RESPONSE <-------"

print("sending email ------> ")
py_task.run_task(send_email)
print("email sent <----------")
