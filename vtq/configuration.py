import dataclasses
import rate_limit


@dataclasses.dataclass
class Bucket:
    name: str
    weight: int


@dataclasses.dataclass
class RateLimit:
    name: str
    type: rate_limit.RateLimitType


@dataclasses.dataclass
class VQueueConfiguration:
    priority: int
    bucket: Bucket
    visibility_timeout_seconds: int


class ConfigurationFetcher:
    def __init__(self, workspace="default"):
        self._workspace = workspace

    def configuration_for(vqueue_name: str = "") -> VQueueConfiguration:
        """used when adding a new task to the table to specify such as"""
        pass

    def rate_limit_for(vqueue_name: str = "") -> RateLimit:
        pass
