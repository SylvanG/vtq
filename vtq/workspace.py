import abc
from abc import abstractmethod
import peewee
from vtq import configuration
from vtq import coordinator
from vtq import model
from vtq import channel


class Workspace(abc.ABC):
    @abstractmethod
    def __init__(self, name: str) -> None:
        pass

    @property
    @abstractmethod
    def database(self) -> peewee.Database:
        pass

    @property
    @abstractmethod
    def model_cls_factory(self) -> model.ModelClsFactory:
        pass

    @property
    @abstractmethod
    def coordinator(self) -> coordinator.Coordinator:
        pass


class DefaultWorkspace(Workspace):
    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._db = None
        self._coordinator = None

    def init(self):
        cls_factory = self.model_cls_factory
        vq_cls = cls_factory.generate_virtual_queue_cls()
        task_cls = cls_factory.generate_task_cls(vq_cls)
        task_error_cls = cls_factory.generate_task_error_cls(task_cls)

        with self.database:
            self.database.create_tables([vq_cls, task_cls, task_error_cls])

    @property
    def database(self) -> peewee.Database:
        if not self._db:
            self._db = model.get_sqlite_database()
        return self._db

    @property
    def model_cls_factory(self) -> model.ModelClsFactory:
        return model.ModelClsFactory(prefix=self.name, database=self.database)

    @property
    def configuration_fetcher(self) -> configuration.ConfigurationFetcher:
        return configuration.ConfigurationFetcher(workspace=self.name)

    @property
    def channel(self) -> channel.Channel | None:
        return channel.Channel()

    @property
    def coordinator(self) -> coordinator.Coordinator:
        if not self._coordinator:
            cls_factory = self.model_cls_factory
            vq_cls = cls_factory.generate_virtual_queue_cls()
            task_cls = cls_factory.generate_task_cls(vq_cls)
            task_error_cls = cls_factory.generate_task_error_cls(task_cls)
            self._coordinator = coordinator.Coordinator(
                database=self.database,
                virtual_queue_cls=vq_cls,
                task_cls=task_cls,
                task_error_cls=task_error_cls,
                config_fetcher=self.configuration_fetcher,
                channel=self.channel,
            )
        return self._coordinator


class MemoryWorkspace(DefaultWorkspace):
    @property
    def database(self) -> peewee.Database:
        """Note: SQLite `:memory:` mode is a special mode where each time the connection is dropped, the data is lost

        https://stackoverflow.com/a/24708173
        https://www.sqlite.org/inmemorydb.html
        Shared in-memory databases: This allows separate database connections to share the same in-memory database. Of course, all database connections sharing the in-memory database need to be in the same process. The database is automatically deleted and memory is reclaimed when the last connection to the database closes.
        """

        if not self._db:
            uri = f"file:{self.name}?mode=memory&cache=shared"
            self._db = model.get_sqlite_database(uri, pool_size=10)
        return self._db
