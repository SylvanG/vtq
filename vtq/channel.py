from collections.abc import Callable


class Channel:
    """any messsage queue, such as pub/sub system, or signal system

    E.g. it can be Blinker Signal, or ZeroMQ, etc.
    """

    def send_task(self, task_id: str, visible_at: float):
        """send added/updated task with visible_at seconds to the channel"""
        pass

    def connect_to_task(self, subscriber: Callable[[str, float], None]):
        pass

    def disconnect(self, subscriber: Callable):
        pass

    def send_vqueue(self, name: str, hidden: bool):
        pass

    def connect_to_vqueue(self, subscriber: Callable[[str, bool], None]):
        pass
