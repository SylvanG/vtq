import unittest
from vtq import configuration


class ConfigurationFetcherTestCase(unittest.TestCase):
    def setUp(self) -> None:
        config = self._create_config()
        self._config = config

        class Loader:
            def load(self):
                return config.to_dict()

        self._loader = Loader()

    def _get_fetcher(self):
        return configuration.ConfigurationFetcher(loader=self._loader)

    def _create_config(self):
        class Config:
            def __init__(self) -> None:
                self._data = {}

            def add_rate_limiter(self, name, type, *vqueue_names):
                data = {
                    "name": name,
                    "type": type,
                }
                for vqueue_name in vqueue_names:
                    vqueues_data: list = data.setdefault("vqueues", [])
                    vqueues_data.append({"name": vqueue_name})
                rate_limiters: list = self._data.setdefault("rate_limiters", [])
                rate_limiters.append(data)

            def to_dict(self):
                return self._data

        return Config()

    def test_rate_limit_for(self):
        self._config.add_rate_limiter("mutex_vq", "MUTEX", "vq")
        data = self._get_fetcher().rate_limit_for("vq")
        assert data is not None
        assert data.type.name == "MUTEX"
        assert data.name == "mutex_vq"

        assert not self._get_fetcher().rate_limit_for("vq1")

    def test_rate_limit_for_with_empty_config(self):
        assert self._get_fetcher().rate_limit_for("vq") is None

    def test_rate_limit_for_with_fuzzy_name(self):
        self._config.add_rate_limiter("mutex_vq", "MUTEX", "vq*")
        data = self._get_fetcher().rate_limit_for("vq")
        assert data is not None
        assert data.type.name == "MUTEX"
        assert data.name == "mutex_vq"

        data = self._get_fetcher().rate_limit_for("vq1")
        assert data is not None
        assert data.type.name == "MUTEX"
        assert data.name == "mutex_vq"

        data = self._get_fetcher().rate_limit_for("vq123")
        assert data is not None

        assert not self._get_fetcher().rate_limit_for("1vq")

    def test_rate_limit_for_with_multiple(self):
        self._config.add_rate_limiter("mutex_vq", "MUTEX", "vq*")
        self._config.add_rate_limiter("mutex_vq2", "MUTEX", "*vq*")
        self._config.add_rate_limiter("mutex_other", "MUTEX", "other")

        data = self._get_fetcher().rate_limit_for("vq")
        assert data and data.name == "mutex_vq"

        data = self._get_fetcher().rate_limit_for("prefix_vq")
        assert data and data.name == "mutex_vq2"

        data = self._get_fetcher().rate_limit_for("other")
        assert data and data.name == "mutex_other"

        assert not self._get_fetcher().rate_limit_for("not_found")

    def test_rate_limit_for_with_vqueue_name_template(self):
        self._config.add_rate_limiter("mutex_{vqueue_name}", "MUTEX", "vq*")
        data = self._get_fetcher().rate_limit_for("vq")
        assert data is not None
        assert data.type.name == "MUTEX"
        assert data.name == "mutex_vq"

        data = self._get_fetcher().rate_limit_for("vq1")
        assert data is not None
        assert data.type.name == "MUTEX"
        assert data.name == "mutex_vq1"
