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


"""
    Concrete Commands
"""
class AbstractTask(ABC):

    @abstractmethod
    def run(self) -> None:
        pass


class SuccessCommand(AbstractCommand):

    def __init__(self, task: AbstractTask):  # pass reciever commands here
        self.task = task

    def execute(self) -> None:
        self.task.run()


class LongTask(AbstractTask):
    """Request"""
    def run(self) -> None:
        print("long task running ------> ")


long_task = LongTask()
success_command = SuccessCommand(long_task)

queue = []

invoker = Invoker(queue)
invoker.set_command(success_command)
invoker.run()








