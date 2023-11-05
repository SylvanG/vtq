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
    completed_at = InitMilliTimeStampField()
    priority = peewee.SmallIntegerField(default=50)


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
    cls = type(cls_name_prefix + model_class.__name__, (model_class,), attrs or {})
    if database:
        database.bind([cls])
    return cls


def generate_model_classes(
    workspace: str = "default", database: peewee.Database | None = None
) -> tuple[type[Task], type[VirtualQueue]]:
    database = database or get_sqlite_database()

    GeneratedVirtualQueue = generate_model_class(
        VirtualQueue, workspace=workspace, database=db
    )
    attrs = {
        "vqueue": peewee.ForeignKeyField(
            GeneratedVirtualQueue, column_name="vqueue_name", backref="tasks"
        )
    }
    GeneratedTask = generate_model_class(
        Task, attrs=attrs, workspace=workspace, database=db
    )
    return GeneratedTask, GeneratedVirtualQueue


if __name__ == "__main__":
    import logging

    peewee.logger.setLevel(logging.DEBUG)
    peewee.logger.addHandler(logging.StreamHandler())

    db = get_sqlite_database()
    DefaultTask, DefaultVirtualQueue = generate_model_classes(database=db)

    with db:
        db.drop_tables([DefaultTask, DefaultVirtualQueue])
        db.create_tables([DefaultTask, DefaultVirtualQueue])
        vq = DefaultVirtualQueue.create(name="test_vq")
        task = DefaultTask.create(data=b"123", vqueue=vq)

        t: Task = DefaultTask.select().get()
        print(t.id, t.visible_at, t.queued_at, t.started_at)
