import peewee
from playhouse import pool
import functools
import uuid
import logging


InitMilliTimeStampField = functools.partial(
    peewee.TimestampField, resolution=3, default=0
)
CurrrentMilliTimeStampField = functools.partial(peewee.TimestampField, resolution=3)


class BinaryUUIDField(peewee.BinaryUUIDField):
    def db_value(self, value):
        if isinstance(value, str) and len(value) == 32:
            return self._constructor(uuid.UUID(value).bytes)
        return super().db_value(value)


class BaseModel(peewee.Model):
    class Meta:
        # will be default in Peewee 4.0, table name will be snakecase
        legacy_table_names = False


class VirtualQueue(BaseModel):
    name = peewee.CharField(primary_key=True)
    updated_at = CurrrentMilliTimeStampField()

    # configuration
    priority = peewee.SmallIntegerField(default=50)
    bucket_name = peewee.CharField(default="")
    bucket_weight = peewee.IntegerField(default=100)
    visibility_timeout = peewee.IntegerField(default=86400)
    rate_limit_type = peewee.CharField(default="")

    # states
    visible_at = InitMilliTimeStampField()
    rate_limit_ver = peewee.BigIntegerField(default=0)


class Task(BaseModel):
    id = BinaryUUIDField(primary_key=True, default=uuid.uuid4)
    data = peewee.BlobField()
    vqueue = peewee.ForeignKeyField(
        VirtualQueue, column_name="vqueue_name", backref="tasks"
    )
    queued_at = CurrrentMilliTimeStampField()
    visible_at = InitMilliTimeStampField()
    status = peewee.SmallIntegerField(default=0)
    started_at = InitMilliTimeStampField()
    ended_at = InitMilliTimeStampField()
    priority = peewee.SmallIntegerField(default=50)
    updated_at = CurrrentMilliTimeStampField()
    retries = peewee.SmallIntegerField(default=0)

    @property
    def id_str(self):
        return self.id.hex  # type: ignore


class TaskError(BaseModel):
    task = peewee.ForeignKeyField(Task, backref="errors")
    happened_at = CurrrentMilliTimeStampField()
    err_msg = peewee.CharField(max_length=80 * 100)
    retry_count = peewee.SmallIntegerField()

    class Meta:
        primary_key = peewee.CompositeKey("task", "happened_at")


def build_task_from_query_result(allow_unknown_fields=False, **kwargs):
    vqueue_data = {}
    task_data = {}
    remaining = {}
    for field_name, value in kwargs.items():
        if field_name.startswith("vqueue_"):
            vqueue_field = VirtualQueue._meta.fields[field_name[7:]]
            vqueue_data[vqueue_field.name] = vqueue_field.python_value(value)
        elif field_name in Task._meta.fields:
            field = Task._meta.fields[field_name]
            task_data[field_name] = field.python_value(value)
        elif allow_unknown_fields:
            remaining[field_name] = value

    if vqueue_data:
        assert "name" in vqueue_data
        task_data["vqueue"] = VirtualQueue(**vqueue_data)

    task = Task(**task_data)

    if allow_unknown_fields:
        for k, v in remaining.items():
            setattr(task, k, v)

    return task


def get_sqlite_database(
    name: str = "vtq.db", pool_size: int = 0, check_same_thread=True
):
    """If pool_size > 0, check_same_thread could be used to allow the SQLite connection to be shared across threads, but you should take care of not using the same connection the same tim ein different threads."""
    # https://docs.peewee-orm.com/en/latest/peewee/database.html#recommended-settings
    pragmas = {
        "journal_mode": "wal",
        "cache_size": -1 * 64000,  # 64MB
        "foreign_keys": 1,
        "ignore_check_constraints": 0,
        "synchronous": 0,
    }
    if pool_size:
        # Please check following doc to see all parameters
        # https://docs.peewee-orm.com/en/latest/peewee/playhouse.html#PooledDatabase
        return pool.PooledSqliteDatabase(
            name,
            max_connections=pool_size,
            stale_timeout=3600,
            timeout=0,  # block forever if pool is full
            pragmas=pragmas,
            autoconnect=False,
            # https://stackoverflow.com/a/48234567
            check_same_thread=check_same_thread,
        )
    return peewee.SqliteDatabase(name, pragmas=pragmas, autoconnect=False)


def generate_model_class[M: type[BaseModel]](
    model_class: M,
    attrs: dict | None = None,
    prefix: str = "default",
    database: peewee.Database | None = None,
) -> M:
    cls_name_prefix = "".join(map(str.capitalize, prefix.split("_")))
    attrs = attrs or {}

    # class Meta:
    #     without_rowid = True

    # attrs["Meta"] = Meta
    cls = type(cls_name_prefix + model_class.__name__, (model_class,), attrs)
    if database:
        database.bind([cls])
    return cls


class ModelClsFactory:
    def __init__(
        self, prefix: str = "default", database: peewee.Database | None = None
    ):
        self._prefix = prefix
        self._database = database or get_sqlite_database()

    def _generate_cls[M: type[BaseModel]](
        self, model: M, attrs: dict | None = None
    ) -> M:
        return generate_model_class(
            model, attrs=attrs, prefix=self._prefix, database=self._database
        )

    def generate_virtual_queue_cls(self) -> type[VirtualQueue]:
        return self._generate_cls(VirtualQueue)

    def generate_task_cls(self, virtual_queue_cls: type[VirtualQueue]) -> type[Task]:
        attrs = {
            "vqueue": peewee.ForeignKeyField(
                virtual_queue_cls, column_name="vqueue_name", backref="tasks"
            )
        }
        return self._generate_cls(Task, attrs=attrs)

    def generate_task_error_cls(self, task_cls: type[Task]) -> type[TaskError]:
        attrs = {"task": peewee.ForeignKeyField(task_cls, backref="errors")}
        return self._generate_cls(TaskError, attrs=attrs)


def enable_debug_logging(disable_handler=False):
    peewee.logger.setLevel(logging.DEBUG)
    if not disable_handler:
        peewee.logger.addHandler(logging.StreamHandler())


def retry_sqlite_db_table_locked[F](f: F, logger=None) -> F:
    logger = logger or logging.getLogger("RetrySqliteDbTableLocked")

    @functools.wraps(f)
    def wrap(*args, **kwargs):
        while True:
            try:
                return f(*args, **kwargs)
            except peewee.OperationalError as e:
                if "database table is locked" in e.args[0]:
                    logger.warning(f"{f.__name__}: Retry the method as {e}")
                    continue
                raise

    return wrap


if __name__ == "__main__":
    enable_debug_logging()

    db = get_sqlite_database()
    cls_factory = ModelClsFactory(database=db)
    DefaultVirtualQueue = cls_factory.generate_virtual_queue_cls()
    DefaultTask = cls_factory.generate_task_cls(DefaultVirtualQueue)
    DefaultTaskError = cls_factory.generate_task_error_cls(DefaultTask)

    with db:
        db.drop_tables([DefaultTask, DefaultVirtualQueue, DefaultTaskError])
        db.create_tables([DefaultTask, DefaultVirtualQueue, DefaultTaskError])
        vq = DefaultVirtualQueue.create(name="test_vq", priority=70)
        task = DefaultTask.create(data=b"123", vqueue=vq)
        print(task.vqueue_name)
        print()

        t: Task = DefaultTask.select().get()
        print(t.id, t.visible_at, t.queued_at, t.started_at, t.updated_at)
        print(t.vqueue)
        print(t.errors)
