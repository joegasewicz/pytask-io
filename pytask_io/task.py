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
from typing import Any, List


class AbstractCommand(ABC):

    @abstractmethod
    def execute(self) -> None:
        pass


class Invoker:
    """Mediator / Invoker"""

    _queue: List[AbstractCommand]

    def __init__(self, queue: List[AbstractCommand]):
        self._queue = queue

    def set_command(self, command: AbstractCommand) -> None:
        self._queue.append(command)

    def run(self) -> None:
        for command in self._queue:
            command.execute()


class AbstractTask(ABC):

    progress: int

    @abstractmethod
    def run(self) -> None:
        pass


"""
    Concrete Commands
"""


class RunCommand(AbstractCommand):

    def __init__(self, task: AbstractTask):  # pass reciever commands here
        self.task = task

    def execute(self) -> None:
        self.task.run()


class ProgressCommand(AbstractCommand):

    def __init__(self, task: AbstractTask):  # pass reciever commands here
        self.task = task

    def execute(self) -> None:
        self.task.progress += 1
        print(f"Progress is ----> {self.task.progress}")


class LongTask(AbstractTask):
    """Request"""

    progress: int = 0

    def run(self) -> None:
        print("long task running ------> ")

"""
    Task
"""

class PyTaskIO:

    _queue: List[AbstractCommand]

    long_task: AbstractTask = LongTask()

    run_command: AbstractCommand

    progress_command: AbstractCommand

    """Task set up"""
    def __init__(self):
        self.run_command = RunCommand(self.long_task)
        self.progress_command = ProgressCommand(self.long_task)

        self._queue = []

    def run_task(self):
        invoker = Invoker(self._queue)
        invoker.set_command(self.run_command)
        invoker.set_command(self.progress_command)
        invoker.run()


py_task = PyTaskIO()
py_task.run_task()



