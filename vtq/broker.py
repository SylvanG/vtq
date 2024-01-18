import logging

from rolecraft.broker import (
    BaseBroker,
    HeaderBytesRawMessage,
    IrrecoverableError,
    QueueNotFound,
)
from rolecraft.broker.broker import ReceiveFuture

from .task import Task
from .workspace import DefaultWorkspace, Workspace, WorkspaceFactory

logger = logging.getLogger(__name__)


class Broker(BaseBroker):
    def __init__(self, workspace_factory: WorkspaceFactory | None = None) -> None:
        self._workspace_factory = workspace_factory or DefaultWorkspace
        self._workspaces = dict[str, Workspace]()

    def prepare_queue(self, queue_name: str):
        if queue_name not in self._workspaces:
            ws = self._workspace_factory(queue_name)
            ws.init()
            self._workspaces[queue_name] = ws

    def close(self):
        super().close()
        for ws in self._workspaces.values():
            ws.close()

    def _get_workspace(
        self, queue_name: str, auto_create_queue: bool = False
    ) -> Workspace:
        ws = self._workspaces.get(queue_name)
        if not ws:
            if not auto_create_queue:
                raise QueueNotFound
            self.prepare_queue(queue_name)
            ws = self._workspaces[queue_name]
        return ws

    def enqueue(
        self,
        queue_name: str,
        message: HeaderBytesRawMessage,
        *,
        auto_create_queue: bool = False,
        **kwargs,
    ) -> str:
        ws = self._get_workspace(queue_name, auto_create_queue)

        # TODO: handle meta data and retrie
        return ws.coordinator.enqueue(message.data, **kwargs)

    def block_receive(
        self,
        queue_name: str,
        *,
        header_keys: list[str] | None = None,
        auto_create_queue: bool = False,
        **kwargs,
    ) -> ReceiveFuture[list[HeaderBytesRawMessage]]:
        ws = self._get_workspace(queue_name, auto_create_queue)

        return ws.coordinator.block_receive(**kwargs).transform(
            lambda tasks: [self._task_to_msg(t, header_keys) for t in tasks]
        )  # type: ignore

    def receive(
        self,
        queue_name: str,
        *,
        max_number: int = 1,
        header_keys: list[str] | None = None,
        auto_create_queue: bool = False,
        **kwargs,
    ) -> list[HeaderBytesRawMessage]:
        ws = self._get_workspace(queue_name, auto_create_queue)
        return [
            self._task_to_msg(t, header_keys)
            for t in ws.coordinator.receive(max_number, wait_time_seconds=0, **kwargs)
        ]

    def _task_to_msg(
        self,
        task: Task,
        header_keys: list[str] | None = None,
    ) -> HeaderBytesRawMessage:
        # TODO: handle header_keys and retrie
        return HeaderBytesRawMessage(
            id=task.id, data=task.data, headers=task.meta.to_dict() if task.meta else {}
        )

    def qsize(self, queue_name: str) -> int:
        ws = self._get_workspace(queue_name)
        return len(ws.coordinator)

    def ack(self, message: HeaderBytesRawMessage, queue_name: str, *, result=None):
        ws = self._get_workspace(queue_name)
        if not ws.coordinator.ack(message.id):
            # TODO: distinguash RecoverableError and IrrecoverableError
            raise IrrecoverableError(f"Ack failed for {message.id}", message)

    def nack(
        self,
        message: HeaderBytesRawMessage,
        queue_name: str,
        *,
        exception: Exception,
    ):
        ws = self._get_workspace(queue_name)
        if not ws.coordinator.nack(message.id, error_message=str(exception)):
            # TODO: distinguash RecoverableError and IrrecoverableError
            raise IrrecoverableError(f"NAck failed for {message.id}", message)

    def requeue(self, message: HeaderBytesRawMessage, queue_name: str):
        ws = self._get_workspace(queue_name)
        if not ws.coordinator.requeue(message.id):
            # TODO: distinguash RecoverableError and IrrecoverableError
            raise IrrecoverableError(f"Requeue failed for {message.id}", message)

    def retry(
        self,
        message: HeaderBytesRawMessage,
        queue_name: str,
        *,
        delay_millis: int = 0,
        exception: Exception | None = None,
    ) -> HeaderBytesRawMessage:
        ws = self._get_workspace(queue_name)
        if not ws.coordinator.retry(
            message.id, delay_millis=delay_millis, error_message=str(exception)
        ):
            # TODO: distinguash RecoverableError and IrrecoverableError
            raise IrrecoverableError(f"Retry failed for {message.id}", message)
        return message
