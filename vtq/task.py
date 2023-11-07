import dataclasses


@dataclasses.dataclass
class Task:
    id: str
    data: bytes
