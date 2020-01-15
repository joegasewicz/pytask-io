"""
 Original pattern based on Hugo Troche's white paper:
 "The Task Pattern: A Design Pattern for Processing and Monitoring Long-Running Tasks"
 See: https://www.developerdotstar.com/mag/articles/troche_taskpattern.html
"""

from abc import ABC, abstractmethod


class AbstractTask(ABC):
    taskSize: int
    message: str
    progress: int

    @abstractmethod
    def get_progress(self) -> int:
        pass

    @abstractmethod
    def get_message(self) -> str:
        pass

    @abstractmethod
    def get_task_size(self) -> int:
        pass

    @abstractmethod
    def set_progress(self, progress: int) -> None:
        pass

    @abstractmethod
    def set_message(self, message: str) -> None:
        pass

    @abstractmethod
    def set_task_size(self, size: int) -> None:
        pass

    @abstractmethod
    def run(self) -> None:
        pass


class LongTask(AbstractTask):
    pass


