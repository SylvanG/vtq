import dataclasses


@dataclasses.dataclass
class TaskMeta:
    retries: int

    def to_dict(self) -> dict[str, int | float | str]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class Task:
    id: str
    data: bytes
    meta: TaskMeta | None
