import abc
from abc import abstractmethod
from task import Task


class TaskQueue(abc.ABC):
    """
    This is the interface for the TaskQueue, it will be implement in two ways.
    One is library-based, and the other is service-based (gPRC).
    """

    @abstractmethod
    def __init__(self, name="default"):
        pass

    @abstractmethod
    def enqueue(
        self,
        task_data: bytes,
        vqueue_name: str = "",
        priority: int = 50,
        delay_millis: int = 0,
    ) -> str:
        pass

    @abstractmethod
    def receive(self, max_number: int = 1, wait_time_seconds: int = 0) -> list[Task]:
        """Receives a list of tasks from the Queue, and the queue will put the tasks on the in process status, which will not be received by other workers before the visibility timeout"""
        pass

    @abstractmethod
    def ack(self, task_id: str) -> bool:
        """Marks the task as completed successfully

        Returns: if the task_id exists and the task is marked as completed, returns True
        """
        pass

    @abstractmethod
    def nack(self, task_id: str, error_messsage: str) -> bool:
        """Marks the task as failed.

        Returns: if the task_id exists and the task is marked as failed with error_message set, returns True.
        """
        pass

    @abstractmethod
    def requeue(self, task_id: str) -> bool:
        pass

    @abstractmethod
    def __len__(self) -> int:
        """Returns the number of uncompleted tasks in th queu, excluding those are in the process"""
        pass

    # following methods are for task management.

    @abstractmethod
    def delete(self, task_id: str) -> bool:
        """"""
        pass

    @abstractmethod
    def update(self, task_id: str, **kwargs):
        pass
