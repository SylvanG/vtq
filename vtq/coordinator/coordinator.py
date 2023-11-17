import functools
import logging
import time
import threading
import peewee
from vtq import task_queue
from vtq import model
from vtq import channel
from vtq import configuration
from vtq.task import Task
from vtq.coordinator import task as task_mod

logger = logging.getLogger(name=__name__)


_INVISIBLE_TIMESTAMP_SECONDS = 2**31 - 1


def retry_sqlite_db_table_locked(f):
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


class Coordinator(task_queue.TaskQueue):
    def __init__(
        self,
        database: peewee.Database,
        virtual_queue_cls: type[model.VirtualQueue],
        task_cls: type[model.Task],
        task_error_cls: type[model.TaskError],
        config_fetcher: configuration.ConfigurationFetcher,
        channel: channel.Channel | None = None,
    ):
        self._db = database
        self._vq_cls = virtual_queue_cls
        self._task_cls = task_cls
        self._task_error_cls = task_error_cls

        self._channel = channel
        self._config_fetcher = config_fetcher

        self._receive_lock = threading.Lock()
    def enqueue(
        self,
        task_data: bytes,
        vqueue_name: str = "",
        priority: int = 50,
        delay_millis: int = 0,
    ) -> str:
        """Insert the task data into the SQL table. Then publish the event that task is added."""
        visible_at = time.time() + delay_millis / 1000.0 if delay_millis else 0

        with self._db.connection_context():
            try:
                task: model.Task = self._task_cls.create(
                    data=task_data,
                    vqueue_name=vqueue_name,
                    priority=priority,
                    visible_at=visible_at,
                )
            except peewee.IntegrityError as e:
                if e.args[0] != "FOREIGN KEY constraint failed":
                    raise
                logger.warning(f"VQ '{vqueue_name}' doesn't exists, creating it")

                task = self._enqueue_task_with_new_vq(
                    task_data, vqueue_name, priority, visible_at
                )

        if self._channel:
            self._channel.send(task.id)
        return task.id

    def _enqueue_task_with_new_vq(
        self, task_data, vqueue_name, priority, visible_at
    ) -> model.Task:
        with self._db.atomic():
            vq_config = self._config_fetcher.configuration_for(vqueue_name)
            self._vq_cls.insert(
                name=vqueue_name,
                priority=vq_config.priority,
                bucket_name=vq_config.bucket.name,
                bucket_weight=vq_config.bucket.weight,
                visibility_timeout=vq_config.visibility_timeout_seconds,
            ).execute()
            task: model.Task = self._task_cls.create(
                data=task_data,
                vqueue_name=vqueue_name,
                priority=priority,
                visible_at=visible_at,
            )
        return task

    def receive(self, max_number: int = 1, wait_time_seconds: int = 0) -> list[Task]:
        """Get tasks from the SQL table, then update the VQ `hidden` status by the result from the Rate Limit."""
        tasks: list[Task] = []
        while len(tasks) < max_number:
            task = self._receive()
            if not task:
                if tasks or wait_time_seconds <= 0:
                    return tasks
                # TODO: block to wait at most `wait_time_seconds`
                raise NotImplementedError
            tasks.append(task)

        return tasks

    def _receive(self) -> Task | None:
        """Get a task from the SQL table, then update the VQ `hidden` status by the result from the Rate Limit."""
        with self._receive_lock:
            while True:
                task = self._read()
                if not task:
                    return

                # TODO: set maxium retry and log it.
                if self._update_task_and_vq(task):
                    return Task(str(task.id), task.data)

                logger.warning("_update_task_and_vq failed and retry")

    def _read(self) -> model.Task | None:
        fn = peewee.fn
        task_cls = self._task_cls
        vq_cls = self._vq_cls
        with self._db.connection_context():
            current_ts = time.time()
            # select the max priority layer from the avaible VQs and available tasks.
            available_task_query = (
                task_cls.select(task_cls, vq_cls)
                .join(vq_cls)
                .where((~vq_cls.hidden) & (current_ts >= task_cls.visible_at))
            )

            max_vq_priority = available_task_query.select(
                fn.max(vq_cls.priority).alias("max_vq_priority")
            )

            priority_layer_query = available_task_query.where(
                vq_cls.priority == max_vq_priority
            )

            # TODO: implement bucket random weighted-priority selection
            # d = (
            #     priority_layer_query.select(vq_cls.bucket_name)
            #     .group_by(vq_cls.bucket_name)
            #     .dicts()
            # )
            # print(list(d))

            task: model.Task | None = (
                priority_layer_query.select(
                    task_cls.id,
                    task_cls.data,
                    task_cls.vqueue_name,
                    task_cls.priority,  # TODO: for debug, remove it later
                    task_cls.updated_at,
                    vq_cls.updated_at.alias("vqueue_updated_at"),
                    vq_cls.visibility_timeout.alias("vqueue_visibility_timeout"),
                )
                .order_by(
                    self._task_cls.priority.desc(), self._task_cls.queued_at.asc()
                )
                .objects()  # there is a peewee bug, the Task model only has id/priority property populated, but without vqueue_name.
                .first()
            )
            print(task)
            if not task:
                return
            print(task.id, task.vqueue_name, task.priority)
            return task

    @retry_sqlite_db_table_locked
    def _update_task_and_vq(self, task) -> bool:
        task_cls = self._task_cls
        vq_cls = self._vq_cls

        # TODO: check rate limit

        with self._db:  # open connection with a transaction
            current_ts = time.time()
            vqueue_subquery = vq_cls.select(vq_cls.name).where(
                (vq_cls.name == task.vqueue_name)
                & (vq_cls.updated_at == task.vqueue_updated_at)
                # & ~vq_cls.hidden
            )
            rv = (
                task_cls.update(
                    status=50,
                    visible_at=task.vqueue_visibility_timeout + current_ts,
                    updated_at=current_ts,
                )
                .where(
                    (task_cls.id == task.id)
                    & (task_cls.vqueue_name == vqueue_subquery)
                    & (task_cls.updated_at == task.updated_at)
                    # & (task_cls.status < 10)
                    # & (task_cls.visible_at <= current_ts)
                )
                .execute()
            )

            # TODO: update VQ hidden according to Rate limit policy
            return bool(rv)

    def _get_task_only_status(self, task_id: str) -> model.Task | None:
        task: model.Task | None = (
            self._task_cls.select(self._task_cls.id, self._task_cls.status)
            .where(self._task_cls.id == task_id)
            .first()
        )
        return task

    @retry_sqlite_db_table_locked
    def ack(self, task_id: str) -> bool:
        with self._db.connection_context():
            task = self._get_task_only_status(task_id)
            if not task:
                return False
            if task_mod.is_succeeded(task):
                return True
            if not task_mod.is_wip(task):
                return False

            # TODO: change to conditional atomic update, using where clause with update
            current_ts = time.time()
            task.status = 100
            task.visible_at = _INVISIBLE_TIMESTAMP_SECONDS
            task.ended_at = current_ts
            task.updated_at = current_ts
            task.save()
        return True

    @retry_sqlite_db_table_locked
    def nack(self, task_id: str, error_message: str) -> bool:
        with self._db.connection_context():
            task = self._get_task_only_status(task_id)
            if not task:
                return False
            if task_mod.is_failed(task):
                return True
            if not task_mod.is_wip(task):
                return False

            # TODO: change to conditional atomic update, using where clause with update
            with self._db.atomic():
                current_ts = time.time()
                task.status = 101
                task.visible_at = _INVISIBLE_TIMESTAMP_SECONDS
                task.ended_at = current_ts
                task.updated_at = current_ts
                task.save()
                if error_message:
                    self._task_error_cls.create(
                        task_id=task.id,
                        err_msg=error_message,
                        happended_at=current_ts,
                    )
        return True

    @retry_sqlite_db_table_locked
    def requeue(self, task_id: str) -> bool:
        with self._db.connection_context():
            task = self._get_task_only_status(task_id)
            if not task:
                return False
            if task.is_unstarted():
                return True
            if not task.is_wip():
                return False

            # TODO: change to conditional atomic update, using where clause with update
            current_ts = time.time()
            task.status = 0
            task.visible_at = 0
            task.updated_at = current_ts
            task.save()
        return True

    @retry_sqlite_db_table_locked
    def retry(
        self, task_id: str, delay_millis: int = 0, error_message: str = ""
    ) -> bool:
        with self._db.connection_context():
            task = self._get_task_only_status(task_id)
            if not task:
                return False
            if task.is_pending():
                return True
            if not task.is_wip():
                return False

            # TODO: change to conditional atomic update, using where clause with update
            # with self._db.atomic():
            current_ts = time.time()
            visible_at = current_ts + delay_millis / 1000.0 if delay_millis else 0
            task.status = 1
            task.visible_at = visible_at
            task.updated_at = current_ts
            task.save()
            if error_message:
                self._task_error_cls.create(
                    task_id=task.id,
                    err_msg=error_message,
                    happended_at=current_ts,
                )
        return True

    @retry_sqlite_db_table_locked
    def __len__(self) -> int:
        with self._db.connection_context():
            # visible_at is used to fetch aviable task, and status is used to find uncompleted task
            return (
                self._task_cls.select(peewee.fn.COUNT(self._task_cls.id))
                .where(self._task_cls.status < 100)
                .scalar()
            )

    def delete(self, task_id: str):
        raise NotImplementedError

    def update(self, task_id: str, **kwargs):
        raise NotImplementedError


if __name__ == "__main__":
    logging.basicConfig()
    model.enable_debug_logging(disable_handler=True)
    c = Coordinator()
    task_id = c.enqueue(task_data=b"123")
    print(task_id)
    # print(c.ack(task_id))
    print(c.receive())
    print(c.receive())
