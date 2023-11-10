import peewee
import functools
import uuid


InitMilliTimeStampField = functools.partial(
    peewee.TimestampField, resolution=3, default=0
)
CurrrentMilliTimeStampField = functools.partial(peewee.TimestampField, resolution=3)


class BaseModel(peewee.Model):
    class Meta:
        # will be default in Peewee 4.0, table name will be snakecase
        legacy_table_names = False


class VirtualQueue(BaseModel):
    name = peewee.CharField(primary_key=True)
    priority = peewee.SmallIntegerField(default=50)
    bucket_name = peewee.CharField(default="")
    bucket_weight = peewee.IntegerField(default=100)
    visibility_timeout = peewee.IntegerField(default=86400)
    hidden = peewee.BooleanField(default=False)
    updated_at = CurrrentMilliTimeStampField()


class Task(BaseModel):
    id = peewee.BinaryUUIDField(primary_key=True, default=uuid.uuid4)
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


class TaskError(BaseModel):
    task = peewee.ForeignKeyField(Task, backref="errors")
    happened_at = CurrrentMilliTimeStampField()
    err_msg = peewee.CharField(max_length=80 * 100)

    class Meta:
        primary_key = peewee.CompositeKey("task", "happened_at")


def get_sqlite_database(name: str = "vtq.db"):
    # https://docs.peewee-orm.com/en/latest/peewee/database.html#recommended-settings
    pragmas = {
        "journal_mode": "wal",
        "cache_size": -1 * 64000,  # 64MB
        "foreign_keys": 1,
        "ignore_check_constraints": 0,
        "synchronous": 0,
    }
    return peewee.SqliteDatabase(name, pragmas=pragmas, autoconnect=False)


def generate_model_class[M: type[BaseModel]](
    model_class: M,
    attrs: dict | None = None,
    workspace: str = "default",
    database: peewee.Database | None = None,
) -> M:
    cls_name_prefix = "".join(map(str.capitalize, workspace.split("_")))
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
        self, workspace: str = "default", database: peewee.Database | None = None
    ):
        self._workspace = workspace
        self._database = database or get_sqlite_database()

    def _generate_cls[M: type[BaseModel]](
        self, model: M, attrs: dict | None = None
    ) -> M:
        return generate_model_class(
            model, attrs=attrs, workspace=self._workspace, database=self._database
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
    import logging

    peewee.logger.setLevel(logging.DEBUG)
    if not disable_handler:
        peewee.logger.addHandler(logging.StreamHandler())


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

        t: Task = DefaultTask.select().get()
        print(t.id, t.visible_at, t.queued_at, t.started_at, t.updated_at)
        print(t.vqueue)
        print(t.errors)
