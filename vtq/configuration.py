import dataclasses
import functools
import operator
import re
from collections.abc import Callable
from pathlib import Path

import yaml

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


class ConfigurationDataLoader:
    def __init__(self, workspace, configuration_dir: Path | None = None) -> None:
        self._workspace = workspace
        self._configuration_dir = configuration_dir or Path.cwd()

    def load(self) -> dict | None:
        return self._load_yaml_config()

    def _load_yaml_config(self) -> dict | None:
        path = self._configuration_dir / f"ws_{self._workspace}.yml"
        if not path.exists():
            return
        with path.open() as f:
            return yaml.safe_load(f)


class ConfigurationFetcher:
    def __init__(self, loader: ConfigurationDataLoader):
        self._loader = loader
        self._rate_limiter_config_factory: Callable[
            [str], RateLimit | None
        ] | None = None

        config = self._loader.load()
        if config:
            self._parse_config(config)

    def _parse_config(self, config: dict):
        if "rate_limiters" in config:
            self._parse_config_rate_limiers(config["rate_limiters"])

    def _parse_config_rate_limiers(self, config: list[dict]):
        rate_limit_conditions: list[tuple[Callable, RateLimit]] = []
        for rate_limit_config in config:
            type_name: str = rate_limit_config["type"].lower()
            rate_limit_type = rate_limit.RateLimitType(type_name)
            rate_limit_data = RateLimit(
                name=rate_limit_config["name"],
                type=rate_limit_type,
            )
            condition = self._parse_vqueue_condition(rate_limit_config["vqueues"])
            rate_limit_conditions.append((condition, rate_limit_data))

        if rate_limit_conditions:

            def factory(vqueue_name: str):
                rate_limit_data = self._eval_conditions(
                    rate_limit_conditions, vqueue_name
                )
                if rate_limit_data:
                    return dataclasses.replace(
                        rate_limit_data,
                        name=rate_limit_data.name.format(vqueue_name=vqueue_name),
                    )

            self._rate_limiter_config_factory = factory

    def _eval_conditions[R, I](
        self, conditions: list[tuple[Callable[[I], bool], R]], data: I
    ) -> R | None:
        for condition, result in conditions:
            if condition(data):
                return result

    def _parse_vqueue_condition(self, config: list[dict]) -> Callable[[str], bool]:
        conditions = []
        for vqueue_config in config:
            name: str = vqueue_config["name"]
            if "*" in name:
                conditions.append(re.compile(name.replace("*", ".*")).match)
            else:
                conditions.append(functools.partial(operator.eq, name))

        return lambda n: any(map(lambda f: f(n), conditions))

    def configuration_for(self, vqueue_name: str = "") -> VQueueConfiguration:
        """used when adding a new task to the table to specify such as"""
        return VQueueConfiguration()

    def rate_limit_for(self, vqueue_name: str = "") -> RateLimit:
        if self._rate_limiter_config_factory:
            return self._rate_limiter_config_factory(vqueue_name)
