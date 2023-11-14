import dataclasses
from vtq import rate_limit


@dataclasses.dataclass
class Bucket:
    name: str = ""
    weight: int = 100


@dataclasses.dataclass
class RateLimit:
    name: str
    type: rate_limit.RateLimitType


@dataclasses.dataclass
class VQueueConfiguration:
    priority: int = 50
    bucket: Bucket = dataclasses.field(default_factory=Bucket)
    visibility_timeout_seconds: int = 3600 * 24


class ConfigurationFetcher:
    def __init__(self, workspace="default"):
        self._workspace = workspace

    def configuration_for(self, vqueue_name: str = "") -> VQueueConfiguration:
        """used when adding a new task to the table to specify such as"""
        return VQueueConfiguration()

    def rate_limit_for(self, vqueue_name: str = "") -> RateLimit:
        pass
