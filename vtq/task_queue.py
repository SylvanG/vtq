class TaskQueue:
    """
    This is the interface for the TaskQueue, it will be implement in two ways.
    One is library-based, and the other is service-based (gPRC).
    """

    def __init__(self, name="default"):
        pass

    def enqueue(
        self,
        task_data: bytes,
        vqueue_name: str = "",
        priority: int = 50,
        delay_millis: int = 0,
    ) -> str:
        pass

    def read(self, max_number: int = 1, wait_time_seconds: int = 0):
        pass

    def ack(self, task_id: str):
        pass

    def nack(self, task_id: str, error_messsage: str):
        pass

    def requeue(self, task_id: str):
        pass

    def __len__(self):
        pass

    # following methods are for task management.

    def delete(self, task_id: str):
        pass

    def update(self, task_id: str, **kwargs):
        pass
