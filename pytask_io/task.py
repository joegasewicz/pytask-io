"""
 Original pattern based on Hugo Troche's white paper:
 "The Task Pattern: A Design Pattern for Processing and Monitoring Long-Running Tasks"
 See: https://www.developerdotstar.com/mag/articles/troche_taskpattern.html

 This is based on .3 Advanced Task Pattern
"""

from abc import ABC, abstractmethod


class AbstractCommand(ABC):

    @abstractmethod
    def execute(self) -> None:
        pass


class AbstractTask(ABC):
    taskSize: int
    message: str
    progress: int

    @abstractmethod
    def get_command(self) -> AbstractCommand:
        pass

    @abstractmethod
    def set_command(self, command: AbstractCommand) -> None:
        pass

    @abstractmethod
    def get_success_command(self) -> AbstractCommand:
        pass

    @abstractmethod
    def set_success_command(self, command: AbstractCommand) -> None:
        pass

    @abstractmethod
    def get_fail_command(self) -> AbstractCommand:
        pass

    @abstractmethod
    def set_fail_command(self, command: AbstractCommand) -> None:
        pass

    @abstractmethod
    def fail(self) -> None:
        pass

    @abstractmethod
    def succeed(self) -> None:
        pass

    @abstractmethod
    def get_message(self) -> str:
        pass

    @abstractmethod
    def get_task_size(self) -> int:
        pass

    @abstractmethod
    def get_progress(self) -> int:
        pass

    @abstractmethod
    def run(self) -> None:
        pass



