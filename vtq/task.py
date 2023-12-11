import dataclasses


@dataclasses.dataclass
class TaskMeta:
    retries: int


@dataclasses.dataclass
class Task:
    id: str
    data: bytes
    meta: TaskMeta | None
