import collections
import contextlib
import functools
import itertools
import logging
import threading
import time
from collections.abc import Callable, Iterable

import peewee

from vtq import channel, configuration, model, rate_limit, task_queue
from vtq.coordinator import common, notification_worker
from vtq.coordinator import task as task_mod
from vtq.coordinator.task import TaskStatus
from vtq.task import Task, TaskMeta
from vtq.waiting_queue import ReceiveFuture, SimpleReceiveFuture, WaitingQueueFactory

from . import cache_util as _cache_util

logger = logging.getLogger(name=__name__)

_INVISIBLE_TIMESTAMP_SECONDS = 2**31 - 1
_MAXIMUM_RATE_LIMITER_VERSION = 2**31 - 1

retry_sqlite_db_table_locked = functools.partial(
    model.retry_sqlite_db_table_locked, logger=logger
)


# TODO: refactor by extracting the model query out to a DAO class
class Coordinator(task_queue.TaskQueue):
    def __init__(
        self,
        database: peewee.Database,
        virtual_queue_cls: type[model.VirtualQueue],
        task_cls: type[model.Task],
        task_error_cls: type[model.TaskError],
        config_fetcher: configuration.ConfigurationFetcher,
        waiting_queue_factory: WaitingQueueFactory,
        task_notification_worker: notification_worker.NotificationWorker,
        rate_limiter_factory: rate_limit.RateLimiterFactory | None = None,
        channel: channel.Channel | None = None,
    ):
        self._db = database
        self._vq_cls = virtual_queue_cls
        self._task_cls = task_cls
        self._task_error_cls = task_error_cls

        self._channel = channel
        self._config_fetcher = config_fetcher
        self._rate_limiter_factory = rate_limiter_factory
        self.rate_limit_release_error_handler: Callable | None = None
        self.rate_limit_expose_vqueue_error_handler: Callable | None = None
        self.rate_limit_acquire_error_handler: Callable[[], bool] | None = None

        self._task_notification_worker = task_notification_worker
        self._waiting_queue_factory = waiting_queue_factory

    @property
    def _support_select_for_update(self) -> bool:
        # TODO: check database
        return False

    @property
    def _is_sqlite(self) -> bool:
        # TODO: check database
        return True

    @functools.cached_property
    def _support_ceiling_function(self) -> bool:
        if not self._is_sqlite:
            return True

        # Assuming there is no ongoing open connection or transaction.
        with self._db.connection_context():
            cursor = self._db.execute_sql(
                "select exists(select 1 from pragma_function_list where name='ceiling');"
            )
            return cursor.fetchone()[0] == 1

    @property
    def _load_balancing_enabled(self) -> bool:
        # TODO: add load balancing option
        return True

    @retry_sqlite_db_table_locked
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
            self._channel.send_task(str(task.id), visible_at)
        return task.id_str

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

    @property
    @_cache_util.cached_method
    def _waiting_queue(self):
        return self._waiting_queue_factory(
            self._fetch,
            self._task_notification_worker.connect_to_available_task,
            default_factory=list,
        )  # FIXME: lazy start notification worker? add a start method

    def block_receive(
        self,
        max_number: int = 1,
        wait_time_seconds: float | None = None,
        *,
        exhausted: bool = True,
    ) -> ReceiveFuture[list[Task]]:
        if self._waiting_queue.empty():
            if tasks := self._fetch(max_number, exhausted):
                return SimpleReceiveFuture(tasks)

        return self._waiting_queue.wait(
            wait_time_seconds, max_number=max_number, exhausted=exhausted
        )

    def _fetch(self, max_number: int, exhausted: bool) -> list[Task]:
        tasks = self._receive_mutliple(max_number)
        if exhausted:
            while len(tasks) < max_number:
                if new := self._receive_mutliple(max_number):
                    tasks.extend(new)
                else:
                    break

        return tasks

    def receive(
        self,
        max_number: int = 1,
        wait_time_seconds: int = 0,
        *,
        exhausted: bool = True,
    ) -> list[Task]:
        """Get tasks from the SQL table, then update the VQ `visible_at` status by the result from the Rate Limit."""
        if wait_time_seconds == 0:
            return self._fetch(max_number, exhausted)
        return self.block_receive(
            max_number, wait_time_seconds, exhausted=exhausted
        ).result()

    # ---- Receive one ---
    # TODO: modify it

    # ---- End: Receive one ---

    # ---- Receive multiple ---

    def _get_available_task_query(
        self,
        current_ts: float,
        limit=1,
        load_balancing_enabled=True,
        use_cte=True,
    ):
        """There is a limitation on this query, it filters the task of the highest priority vqueue layer, even there is available task in the below priority, it won't choose them. The second limitation is that for the rate limiter, it only filters the type of Mutex, so for other rate limiter, you should do it outside the function"""
        fn = peewee.fn
        task_cls = self._task_cls
        vq_cls = self._vq_cls

        class Schema:
            id = task_cls.id
            data = task_cls.data
            vqueue_name = task_cls.vqueue_name
            priority = task_cls.priority  # TODO: for debug, remove it later
            updated_at = task_cls.updated_at
            queued_at = task_cls.queued_at
            retries = task_cls.retries
            vqueue_priority = vq_cls.priority.alias("vqueue_priority")
            vqueue_updated_at = vq_cls.updated_at.alias("vqueue_updated_at")
            vqueue_visibility_timeout = vq_cls.visibility_timeout.alias(
                "vqueue_visibility_timeout"
            )
            vqueue_rate_limit_type = vq_cls.rate_limit_type.alias(
                "vqueue_rate_limit_type"
            )
            vqueue_row_number = (
                fn.ROW_NUMBER()
                .over(
                    partition_by=[task_cls.vqueue_name],
                    order_by=[task_cls.priority.desc(), task_cls.queued_at.asc()],
                )
                .alias("vqueue_row_number")
            )
            vqueue_max_priority = (
                fn.MAX(vq_cls.priority).over().alias("vqueue_max_priority")
            )
            vqueue_bucket_name = vq_cls.bucket_name.alias("vqueue_bucket_name")
            vqueue_bucket_weight = vq_cls.bucket_weight.alias("vqueue_bucket_weight")

        schema = Schema

        # select visible task, vqueue_row_number, vqueue_max_priority, and vqueue_bucket_name
        available_task_subquery = (
            task_cls.select(
                schema.id,
                schema.data,  # TODO: may put in the final query use join
                schema.vqueue_name,
                schema.priority,
                schema.updated_at,
                schema.queued_at,
                schema.retries,
                schema.vqueue_priority,
                schema.vqueue_updated_at,
                schema.vqueue_visibility_timeout,
                schema.vqueue_rate_limit_type,
                schema.vqueue_row_number,
                schema.vqueue_max_priority,
                # only need when load balancing is enabled
                schema.vqueue_bucket_name,
                schema.vqueue_bucket_weight,
            )
            .join(vq_cls)
            .where(
                (current_ts >= vq_cls.visible_at) & (current_ts >= task_cls.visible_at)
            )
        )

        # Select from the above subquery, with filtering max vqueue priority, and vqueue first task of the mutex rate limiter, and optionaly select the bucket row number based on the filter result.
        schema = available_task_subquery.c

        select = task_cls.select(
            schema.id,
            schema.data,
            schema.vqueue_name,
            schema.priority,
            schema.updated_at,
            schema.queued_at,
            schema.retries,
            schema.vqueue_priority,
            schema.vqueue_updated_at,
            schema.vqueue_visibility_timeout,
            schema.vqueue_rate_limit_type,
        )

        if load_balancing_enabled:
            bucket_row_number = (
                fn.ROW_NUMBER()
                .over(
                    partition_by=[schema.vqueue_bucket_name],
                    order_by=[schema.priority.desc(), schema.queued_at.asc()],
                )
                .alias("bucket_row_number")
            )
            select = select.select_extend(
                schema.vqueue_bucket_name,
                schema.vqueue_bucket_weight,
                bucket_row_number,
            )

        filtered_task_subquery = select.from_(available_task_subquery).where(
            (schema.vqueue_priority == schema.vqueue_max_priority)
            & (
                (
                    schema.vqueue_rate_limit_type.not_in(("MUTEX", "MUTEX_COMPOSITE"))
                    & (schema.vqueue_row_number <= limit)
                )
                | (schema.vqueue_row_number == 1)
            )
        )

        filtered_task_subquery = (
            filtered_task_subquery.alias("filtered_task_subquery")
            if not use_cte
            else filtered_task_subquery.cte("filtered_task_subquery")
        )

        # Select a limited number of tasks in the order of task priority, enqueue time, either as a whole or by bucket.
        # TODO: refator to select the data in the final step
        schema = filtered_task_subquery.c
        task_query = task_cls.select(
            schema.id,
            schema.data,
            schema.vqueue_name,
            schema.priority,  # TODO: for debug, remove it later
            schema.queued_at,  # TODO: for debug, remove it later
            schema.retries,
            schema.updated_at,
            schema.vqueue_updated_at,
            schema.vqueue_visibility_timeout,
            schema.vqueue_rate_limit_type,
            schema.vqueue_bucket_name,
            schema.vqueue_bucket_weight,
        ).from_(filtered_task_subquery)

        if use_cte:
            if load_balancing_enabled:
                bucket_subquery = self._build_bucket_query(
                    filtered_task_subquery, limit
                )

                # TODO: remove it. It's for debug
                task_query = task_query.select_extend(
                    bucket_subquery.c.bucket_count, schema.bucket_row_number
                )

                task_query = task_query.join(
                    bucket_subquery,
                    on=(
                        schema.vqueue_bucket_name
                        == bucket_subquery.c.vqueue_bucket_name
                    ),
                ).where(schema.bucket_row_number <= bucket_subquery.c.bucket_count)
            task_query = task_query.with_cte(filtered_task_subquery)
        elif load_balancing_enabled:
            task_query = task_query.where(bucket_row_number <= limit)
        else:
            task_query = task_query.order_by(
                schema.priority.desc(), schema.queued_at.asc()
            ).limit(limit)

        return task_query.objects(
            constructor=functools.partial(
                model.build_task_from_query_result, allow_unknown_fields=False
            )
        )

    def _build_bucket_query(self, filtered_task_subquery, limit):
        """return rows of `vqueue_bucket_name, bucket_count`"""
        fn = peewee.fn

        # Compute the sum of buckets
        bucket_weight_subquery = (
            peewee.Select(
                [filtered_task_subquery],
                [filtered_task_subquery.c.vqueue_bucket_weight],
            )
            .group_by(filtered_task_subquery.c.vqueue_bucket_name)
            .alias("bucket_weight_subquery")
        )
        bucket_weight_sum = peewee.Select(
            from_list=[bucket_weight_subquery],
            columns=[
                fn.SUM(bucket_weight_subquery.c.vqueue_bucket_weight).alias(
                    "weight_sum"
                )
            ],
        ).alias("bucket_weight_sum")

        # Build the bucket query
        raw_bucket_count = (
            filtered_task_subquery.c.vqueue_bucket_weight
            * limit
            / bucket_weight_sum.c.weight_sum
        )
        if self._support_ceiling_function:
            bucket_count = fn.CEILNG(raw_bucket_count).alias("bucket_count")
        else:
            bucket_count = (raw_bucket_count + 1).alias("bucket_count")

        return peewee.Select(
            from_list=[filtered_task_subquery, bucket_weight_sum],
            columns=[filtered_task_subquery.c.vqueue_bucket_name, bucket_count],
        ).group_by(filtered_task_subquery.c.vqueue_bucket_name)

    @retry_sqlite_db_table_locked
    def _receive_mutliple(self, max_number: int) -> list[Task]:
        """First, read and update task where are possible, then put those with rate limit one into the confirmation process.

        The results may be less than requried even there are enough available tasks in the db. This is caused by the rate limit filter, and we only retrieve max_number of tasks from the table.
        """
        query = self._build_retrieve_multiple_query(max_number)

        with contextlib.ExitStack() as stack:
            # open a connection and a write transation
            stack.enter_context(self._db.connection_context())
            stack.enter_context(
                self._db.atomic()
                if self._support_select_for_update
                else self._db.atomic("IMMEDIATE")
            )

            # retrieve then update(confirm) the task immediately. They should be in a transaction so that there is no other process read the same task again causing the issue. So the there must be very few logic and put slow logic outside the transaction.
            confirmable_tasks, remaining = self._retrieve_multiple(query, max_number)
            self._update_multiple(confirmable_tasks + remaining)

        return [
            Task(task.id_str, task.data, TaskMeta(retries=task.retries))
            for task in sorted(
                confirmable_tasks + self._confirm_with_rate_limit(remaining),
                key=lambda t: (-t.priority, t.queued_at),
            )
        ]

    def _build_retrieve_multiple_query(self, max_number):
        current_ts = time.time()

        query: Iterable[model.Task] = self._get_available_task_query(
            current_ts,
            limit=max_number,
            load_balancing_enabled=self._load_balancing_enabled,
            use_cte=False,
        )

        if self._support_select_for_update:
            # TODO: test which tables and rows are locked
            query = query.for_update()
        return query

    def _retrieve_multiple(
        self, query, max_number: int
    ) -> tuple[list[model.Task], list[model.Task]]:
        # Use pessimistic lock on the task rows for the reading and updating as the synchronization solution

        tasks = sorted(query, key=lambda t: (-t.priority, t.queued_at))

        if self._load_balancing_enabled and len(tasks) > max_number:
            tasks = self._filter_by_load_balancing(tasks, max_number)

        confirmable, remaining = [], []

        def is_confirmable(task: model.Task) -> bool:
            return task.vqueue.rate_limit_type in (
                "",
                rate_limit.RateLimitType.MUTEX.name,
            )

        for task in tasks:
            confirmable.append(task) if is_confirmable(task) else remaining.append(task)
        return confirmable, remaining

    def _update_multiple[T: model.Task](self, tasks: list[T], confiming_seconds=60):
        """Assume this is done in the context of pessimitic lock of the task rows. The confirmation should be finished in one minute by default."""
        if not tasks:
            return

        current_ts = time.time()

        vqueues_to_update = []

        for task in tasks:
            task.visible_at = current_ts + task.vqueue.visibility_timeout
            task.status = task_mod.TaskStatus.PROCESSING.value
            task.updated_at = current_ts

            if task.vqueue.rate_limit_type == rate_limit.RateLimitType.MUTEX.name:
                task.vqueue.visible_at = _INVISIBLE_TIMESTAMP_SECONDS
                task.vqueue.updated_at = current_ts
                vqueues_to_update.append(task.vqueue)
            elif task.vqueue.rate_limit_type:
                task.visible_at = current_ts + confiming_seconds
                task.status = task_mod.TaskStatus.CONFIRMING.value

        self._task_cls.bulk_update(
            tasks,
            fields=[
                self._task_cls.status,
                self._task_cls.updated_at,
                self._task_cls.visible_at,
            ],
        )

        if vqueues_to_update:
            self._vq_cls.bulk_update(
                vqueues_to_update,
                fields=[
                    self._vq_cls.visible_at,
                    self._vq_cls.updated_at,
                ],
            )

    def _update_multiple_for_confirming[T: model.Task](
        self,
        tasks: list[T],
        confirming_num: int,
        current_ts: float,
    ) -> int:
        """Assume this is done in the context of pessimitic lock of the task rows. The confirmation should be finished in one minute by default."""
        if not tasks:
            return

        for task in tasks[:confirming_num]:
            task.visible_at = current_ts + task.vqueue.visibility_timeout
            task.status = task_mod.TaskStatus.PROCESSING.value
            task.updated_at = current_ts

        for task in tasks[confirming_num:]:
            task.visible_at = 0
            # This may cause the loss of information regarding the retrying status, reverting back to the unstarted status
            task.status = task_mod.TaskStatus.UNSTARTED.value
            task.updated_at = current_ts

        # TODO: check they are in confirming status and invisible status
        updated_count = self._task_cls.bulk_update(
            tasks,
            fields=[
                self._task_cls.status,
                self._task_cls.updated_at,
                self._task_cls.visible_at,
            ],
        )
        return updated_count

    def _filter_by_load_balancing[T: model.Task](
        self, tasks: list[T], num: int
    ) -> list[T]:
        """Assume tasks have already been ordered by priority and enqueue time.

        It returns the tasks ordered by priority and enqueue time for each bucket.
        """
        assert num < len(tasks)

        buckets = collections.defaultdict(list)
        for task in tasks:
            # TODO: remove it, it's for debugging
            # assert isinstance(task.bucket_row_number, int)
            # assert isinstance(task.bucket_count, float)
            # assert task.bucket_row_number > 0
            # assert task.bucket_row_number <= task.bucket_count
            buckets[task.vqueue.bucket_name].append(task)

        bucket_tasks = []
        bucket_weights = []
        for one_bucket_tasks in buckets.values():
            # one_bucket_tasks.sort(key=lambda t: (-t.priority, t.queued_at))
            bucket_tasks.append(one_bucket_tasks)
            bucket_weights.append(one_bucket_tasks[0].vqueue.bucket_weight)

        dist = common.wrs_distribution(weights=bucket_weights, k=num)
        return list(
            itertools.chain(
                *(bucket_tasks[idx][:count] for idx, count in enumerate(dist))
            )
        )

    def _confirm_with_rate_limit[T: model.Task](self, tasks: list[T]) -> list[T]:
        """The task has been locked/tagged, and it has to acquire the access to the resource from the rate limiter. If fail to acquire the access, it has to release the task lock.

        Assume the tasks have been ordered by priority and enqueue time for each vqueue.

        Returns: list of tasks that were confirmed with rate limiter. The order of the returned tasks are not guaranteed.
        """
        if not tasks:
            return []

        vqueues = collections.defaultdict(list)
        for task in tasks:
            vqueues[task.vqueue_name].append(task)

        rv = []
        for vqueue_name, tasks in vqueues.items():
            assert sorted(tasks, key=lambda t: (-t.priority, t.queued_at)) == tasks
            rv.extend(self._confirm_vqueue_with_rate_limit(vqueue_name, tasks))
        return rv

    def _confirm_vqueue_with_rate_limit(
        self, tasks: list[model.Task], vqueue_name: str
    ) -> list[bool]:
        """Assume the tasks have been ordered by priority and enqueue time."""
        rate_limiter = self._get_rate_limiter_for_vqueue(vqueue_name)
        if not rate_limiter:
            return []

        while True:
            try:
                access = rate_limiter.acquire_access(count=len(tasks))
            except Exception as e:
                logger.error(
                    "Acquire access of rate limiter %s failed for %s",
                    rate_limiter,
                    vqueue_name,
                    exc_info=e,
                )
                if not self.rate_limit_acquire_error_handler:
                    raise

                should_retry = self.rate_limit_acquire_error_handler(
                    vqueue_name, rate_limiter, e
                )
                if not should_retry:
                    self._hide_vqueue_permanently(vqueue_name)
                    break

        if not access.count:
            # acquired but didn't hide the VQueue
            raise RuntimeError(
                f"rate limiter resource {rate_limiter} access with no count for {vqueue_name}"
            )

        try:
            self._confirm_tasks_after_rate_limit(tasks, vqueue_name, access)
        except Exception as e:
            logger.error(
                "confirming the tasks in %s with rate limiter failed",
                vqueue_name,
                exc_info=e,
            )

            # FIXME: check if it is a count-based rate limiter
            self._release_rate_limit(vqueue_name, access.count, rate_limiter)
            return []
        else:
            return tasks[: access.count]

    @retry_sqlite_db_table_locked
    def _confirm_tasks_after_rate_limit(self, tasks, vqueue_name, access):
        with self._db:  # open a connection and a transaction
            current_ts = time.time()
            updated_count = self._update_multiple_for_confirming(
                tasks, confirming_num=access.count, current_ts=current_ts
            )
            if updated_count != len(tasks):
                logger.info(
                    f"some tasks may be deleted during the confirming process in {vqueue_name}"
                )
                raise NotImplementedError(
                    f"some tasks may be deleted during the confirming process in {vqueue_name}"
                )
            if access.limit_reached:
                self._hide_vqueue_for_rate_limit(
                    vqueue_name,
                    change_version=access.version,
                    visible_at=access.until,
                    current_ts=current_ts,
                )

    # ---- End: Receive multiple ---

    @functools.cache
    def _get_rate_limiter(
        self,
        rate_limit_type: rate_limit.RateLimitType,
        resource_name: str,
    ) -> rate_limit.RateLimiter | None:
        if not self._rate_limiter_factory:
            logger.error(f"no rate limiter factory for the '{rate_limit_type}'")
            return

        return self._rate_limiter_factory(rate_limit_type, resource_name)

    def _get_rate_limiter_for_vqueue(
        self, vqueue_name
    ) -> rate_limit.RateLimiter | None:
        rate_limit_conf = self._config_fetcher.rate_limit_for(vqueue_name)
        return (
            self._get_rate_limiter(rate_limit_conf.type, rate_limit_conf.name)
            if rate_limit_conf
            else None
        )

    def _release_rate_limit(self, vqueue_name: str, count=1, rate_limiter=None):
        rate_limiter = rate_limiter or self._get_rate_limiter_for_vqueue(vqueue_name)
        if not rate_limiter:
            return

        try:
            change = rate_limiter.release_access(count=count)
        except Exception as e:
            logger.error(
                "release rate limit resource %s error for %s",
                rate_limiter,
                vqueue_name,
                exc_info=e,
            )
            if self.rate_limit_release_error_handler:
                self.rate_limit_release_error_handler(vqueue_name, rate_limiter, e)
            return

        if not change:
            return

        try:
            self._expose_vqueue_for_rate_limit(vqueue_name, change.version)
        except Exception as e:
            logger.error(
                "Expose VQueue %s error during the releasing rate limit resource with version %s",
                vqueue_name,
                change.version,
            )
            if self.rate_limit_expose_vqueue_error_handler:
                self.rate_limit_expose_vqueue_error_handler(
                    vqueue_name, change.version, e
                )

    @retry_sqlite_db_table_locked
    def _expose_vqueue_for_rate_limit(self, vqueue_name: str, change_version: int):
        with self._db.connection_context():
            updated = (
                self._vq_cls.update(
                    visible_at=0,
                    updated_at=time.time(),
                    rate_limit_ver=change_version,
                )
                .where(
                    (self._vq_cls.name == vqueue_name)
                    & (self._vq_cls.rate_limit_ver < change_version)
                )
                .execute()
            )

            if not updated:
                logger.debug(
                    f"Vqueue rate limit update skipped for {vqueue_name} because the version {change_version} is behind."
                )

    @retry_sqlite_db_table_locked
    def _hide_vqueue_permanently(self, vqueue_name: str):
        with self._db.connection_context():
            self._hide_vqueue_for_rate_limit(
                vqueue_name,
                _MAXIMUM_RATE_LIMITER_VERSION,
                _INVISIBLE_TIMESTAMP_SECONDS,
            )

    def _hide_vqueue_for_rate_limit(
        self,
        vqueue_name: str,
        change_version: int,
        visible_at: float,
        current_ts: float | None = None,
    ):
        updated = (
            self._vq_cls.update(
                visible_at=visible_at,
                updated_at=current_ts or time.time(),
                rate_limit_ver=change_version,
            )
            .where(
                (self._vq_cls.name == vqueue_name)
                & (self._vq_cls.rate_limit_ver < change_version)
            )
            .execute()
        )

        if not updated:
            logger.debug(
                f"Vqueue rate limit update skipped for {vqueue_name} because the version {change_version} is behind."
            )

    # ---- ACK ---

    @retry_sqlite_db_table_locked
    def _get_task_for_ack(self, task_id: str) -> model.Task | None:
        # using optimistic lock
        task_query = (
            self._task_cls.select(
                self._task_cls.id,
                self._task_cls.status,
                self._task_cls.vqueue_name.alias("vqueue_name"),
                self._task_cls.updated_at,
                self._task_cls.retries,  # used for retry and nack
                self._vq_cls.rate_limit_type.alias("vqueue_rate_limit_type"),
                self._vq_cls.updated_at.alias("vqueue_updated_at"),
            )
            .join(self._vq_cls)
            .where(self._task_cls.id == task_id)
            .objects(constructor=model.build_task_from_query_result)
        )
        return task_query.first()

    @retry_sqlite_db_table_locked
    def _update_task_for_ack(
        self,
        task: model.Task,
        status=TaskStatus.SUCCEEDED.value,
        visible_at=_INVISIBLE_TIMESTAMP_SECONDS,
        error_message: str | None = None,
        retries: int | None = None,
        release_time_based_rate_limiter=False,
    ) -> bool:
        """Update the task for ack-related operations (ACK/NACK/REQUEUE/RETRY).

        It will update task status, visiable_at, updated_at, and record error message if provided.

        Besides, it will release the count-based rate limit resource if exists.

        If release_time_based_rate_limiter is specified, it will release the time-based rate if exists. However, it haven't been implemented.
        """
        if release_time_based_rate_limiter:
            raise NotImplementedError

        assert task.id
        assert task.updated_at
        assert task.status

        task_cls = self._task_cls
        vq_cls = self._vq_cls

        is_mutex_limiter = (
            task.vqueue.rate_limit_type == rate_limit.RateLimitType.MUTEX.name
        )

        txn_context = (
            # FIXME: check if rate_limiter is count-based
            self._db.atomic
            if is_mutex_limiter or error_message
            else contextlib.nullcontext
        )

        with txn_context() as transaction:
            current_ts = time.time()
            data = dict(
                status=status,
                visible_at=visible_at,
                updated_at=current_ts,
            )
            if visible_at == _INVISIBLE_TIMESTAMP_SECONDS:
                data["ended_at"] = current_ts
            if retries is not None:
                data["retries"] = retries

            updated = (
                task_cls.update(**data)
                .where(
                    (task_cls.id == task.id)
                    & (task_cls.updated_at == task.updated_at)
                    & (task_cls.status == task.status)
                )
                .execute()
            )
            if not updated:
                return False

            if error_message:
                self._task_error_cls.create(
                    task_id=task.id,
                    err_msg=error_message,
                    happended_at=current_ts,
                    retry_count=task.retries,
                )

            if is_mutex_limiter:
                updated = (
                    vq_cls.update(visible_at=0, updated_at=current_ts)
                    .where(
                        (vq_cls.name == task.vqueue_name)
                        & (vq_cls.updated_at == task.vqueue.updated_at)
                    )
                    .execute()
                )
                if not updated:
                    transaction.rollback()
                    return False

        if task.vqueue.rate_limit_type and not is_mutex_limiter:
            self._release_rate_limit(task.vqueue_name)

        return True

    def ack(self, task_id: str) -> bool:
        with self._db.connection_context():
            task = self._get_task_for_ack(task_id)
            if not task:
                return False
            if task_mod.is_succeeded(task):
                return True
            if not task_mod.is_wip(task):
                return False

            updated = self._update_task_for_ack(task)
            if not updated:
                logger.error(
                    f"ack failed because task status has been changed since the start for task {task_id}"
                )

        return updated

    def nack(self, task_id: str, error_message: str) -> bool:
        with self._db.connection_context():
            task = self._get_task_for_ack(task_id)
            if not task:
                return False
            if task_mod.is_failed(task):
                return True
            if not task_mod.is_wip(task):
                return False

            return self._update_task_for_ack(
                task,
                status=TaskStatus.FAILED.value,
                error_message=error_message,
            )

    def requeue(self, task_id: str) -> bool:
        with self._db.connection_context():
            task = self._get_task_for_ack(task_id)
            if not task:
                return False
            if task.is_unstarted():
                return True
            if not task.is_wip():
                return False

            updated = self._update_task_for_ack(
                task, status=TaskStatus.UNSTARTED.value, visible_at=0
            )

        if updated and self._channel:
            self._channel.send_task(str(task.id), task.visible_at)

        return updated

    def retry(
        self, task_id: str, delay_millis: int = 0, error_message: str = ""
    ) -> bool:
        with self._db.connection_context():
            task = self._get_task_for_ack(task_id)
            if not task:
                return False
            if task.is_unstarted() and task.retries > 0:
                return True
            if not task.is_wip():
                return False

            visible_at = time.time() + delay_millis / 1000.0 if delay_millis else 0
            updated = self._update_task_for_ack(
                task,
                status=TaskStatus.UNSTARTED.value,
                visible_at=visible_at,
                error_message=error_message,
                retries=task.retries + 1,
            )

        if updated and self._channel:
            self._channel.send_task(str(task.id), visible_at)

        return updated

    # ---- END: ACK ---

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
