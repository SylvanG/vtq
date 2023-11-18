import unittest
import time
import concurrent.futures
import uuid
import peewee
from vtq import workspace
from vtq.coordinator.notification_worker import SimpleNotificationWorker


class CoordinatorTestCase(unittest.TestCase):
    def setUp(self):
        notification_worker = SimpleNotificationWorker(interval=0.1)
        ws = workspace.MemoryWorkspace(notificaiton_worker=notification_worker)
        ws.init()
        ws.flush_all()
        self.ws = ws
        self.db = ws.database
        self.coordinator = ws.coordinator
        self.vq_cls = self.coordinator._vq_cls
        self.task_cls = self.coordinator._task_cls
        self.task_error_cls = self.coordinator._task_error_cls

    def tearDown(self):
        self.ws.notification_worker.stop()

    def _enable_model_debug_logging(self):
        from vtq import model

        model.enable_debug_logging()

    def _add_vq(self, name="default", **kwargs):
        with self.db:
            self.vq_cls.create(name=name, **kwargs)

        class VQ:
            def __init__(self, name, db, task_cls):
                self.name = name
                self.db = db
                self.task_cls = task_cls

            def add_task(self, **kwargs) -> str:
                kwargs.setdefault("data", b"data")
                with self.db:
                    task = self.task_cls.create(vqueue_name=name, **kwargs)
                return str(task.id)

        return VQ(name, self.db, self.task_cls)

    def _get_ids(self, tasks):
        return list(map(lambda t: str(t.id), tasks))

    def _new_task_id(self):
        return str(uuid.uuid4())

    def _get_task_model(self, task_id):
        with self.db:
            task_models = (
                self.task_cls.select(self.task_cls)
                .where(self.task_cls.id == task_id)
                .prefetch(self.task_error_cls)
            )
            assert len(task_models) <= 1
            return task_models[0] if task_models else None

    def test_receive_check_task_visible_at(self):
        vq = self._add_vq()

        ts = time.time()
        _ = vq.add_task(visible_at=ts + 100)
        task_id2 = vq.add_task(visible_at=ts)
        task_id3 = vq.add_task(visible_at=ts - 100)

        tasks = self.coordinator.receive(max_number=3)
        assert len(tasks) == 2
        assert set(self._get_ids(tasks)) == set((task_id2, task_id3))

    def test_receive_check_task_vq_hidden(self):
        vq = self._add_vq("default")
        vq_hidden = self._add_vq("hidden", hidden=True)

        _ = vq_hidden.add_task()
        task_id2 = vq.add_task()
        tasks = self.coordinator.receive(max_number=2)
        assert len(tasks) == 1
        assert tasks[0].id == task_id2

    def test_receive_task_priority(self):
        vq = self._add_vq()
        task_id1 = vq.add_task(priority=50)
        task_id2 = vq.add_task(priority=100)
        task_id3 = vq.add_task(priority=30)

        tasks = self.coordinator.receive(max_number=3)
        assert len(tasks) == 3
        assert self._get_ids(tasks) == [task_id2, task_id1, task_id3]

    def test_receive_task_enqueue_time(self):
        vq = self._add_vq()
        task_id1 = vq.add_task()
        task_id2 = vq.add_task()
        task_id3 = vq.add_task()

        tasks = self.coordinator.receive(max_number=3)
        assert len(tasks) == 3
        assert self._get_ids(tasks) == [task_id1, task_id2, task_id3]

    def test_receive_vq_priority(self):
        vq = self._add_vq("default", priority=50)
        vq_high_priority = self._add_vq("high", priority=70)

        task_id1 = vq.add_task()
        task_id2 = vq_high_priority.add_task()

        tasks = self.coordinator.receive(max_number=3)
        assert len(tasks) == 2
        assert self._get_ids(tasks) == [task_id2, task_id1]

    def test_receive_hybrid(self):
        vq = self._add_vq("default", priority=50)
        vq_hidden = self._add_vq("hidden", priority=50, hidden=True)
        vq_high_priority = self._add_vq("high", priority=70)

        ts = time.time()

        _ = vq_high_priority.add_task(priority=30, visible_at=ts + 100)
        p2 = vq_high_priority.add_task(priority=30, visible_at=ts - 100)
        p3 = vq_high_priority.add_task(priority=30, visible_at=ts)
        _ = vq_high_priority.add_task(priority=70, visible_at=ts + 100)
        p5 = vq_high_priority.add_task(priority=70, visible_at=ts)
        p6 = vq_high_priority.add_task(priority=70, visible_at=ts - 100)

        _ = vq_hidden.add_task(priority=30, visible_at=ts + 100)
        _ = vq_hidden.add_task(priority=30, visible_at=ts - 100)
        _ = vq_hidden.add_task(priority=30, visible_at=ts)
        _ = vq_hidden.add_task(priority=70, visible_at=ts + 100)
        _ = vq_hidden.add_task(priority=70, visible_at=ts)
        _ = vq_hidden.add_task(priority=70, visible_at=ts - 100)

        _ = vq.add_task(priority=30, visible_at=ts + 100)
        t2 = vq.add_task(priority=30, visible_at=ts - 100)
        t3 = vq.add_task(priority=30, visible_at=ts)
        _ = vq.add_task(priority=70, visible_at=ts + 100)
        t5 = vq.add_task(priority=70, visible_at=ts)
        t6 = vq.add_task(priority=70, visible_at=ts - 100)

        tasks = self.coordinator.receive(max_number=3 * 6)
        assert len(tasks) == 8
        assert self._get_ids(tasks) == [p5, p6, p2, p3, t5, t6, t2, t3]

    @unittest.skip
    def test_receive_vq_bucket(self):
        pass

    def test_ack(self):
        vq = self._add_vq()
        vq.add_task()
        task = self.coordinator.receive(max_number=1)[0]

        rv = self.coordinator.ack(task.id)
        assert rv is True
        assert not self.coordinator.receive(max_number=1)

    def test_ack_idempotent(self):
        vq = self._add_vq()
        vq.add_task()
        task = self.coordinator.receive(max_number=1)[0]

        rv = self.coordinator.ack(task.id)
        assert rv is True
        rv = self.coordinator.ack(task.id)
        assert rv is True

    def test_ack_non_exist_task(self):
        rv = self.coordinator.ack(self._new_task_id())
        assert rv is False

    def test_ack_non_wip_task(self):
        vq = self._add_vq()
        task_id = vq.add_task()
        rv = self.coordinator.ack(task_id)
        assert rv is False

    def test_nack(self):
        vq = self._add_vq()
        vq.add_task()
        task = self.coordinator.receive(max_number=1)[0]

        rv = self.coordinator.nack(task.id, "network error")
        assert rv is True
        assert not self.coordinator.receive(max_number=1)

    def test_nack_idempotent(self):
        vq = self._add_vq()
        vq.add_task()
        task = self.coordinator.receive(max_number=1)[0]

        rv = self.coordinator.nack(task.id, "network error")
        assert rv is True
        rv = self.coordinator.nack(task.id, "network error")
        assert rv is True
        rv = self.coordinator.nack(task.id, "network error 2")
        assert rv is True

    def test_nack_non_exist_task(self):
        rv = self.coordinator.nack(self._new_task_id(), error_message="network error")
        assert rv is False

    def test_nack_non_wip_task(self):
        vq = self._add_vq()
        task_id = vq.add_task()
        rv = self.coordinator.nack(task_id, error_message="network error")
        assert rv is False

    def test_requeue(self):
        vq = self._add_vq()
        vq.add_task()
        task = self.coordinator.receive(max_number=1)[0]

        rv = self.coordinator.requeue(task.id)
        assert rv is True
        assert self.coordinator.receive(max_number=1)[0].id == task.id

    def test_requeue_idempotent(self):
        vq = self._add_vq()
        vq.add_task()
        task = self.coordinator.receive(max_number=1)[0]

        rv = self.coordinator.requeue(task.id)
        assert rv is True
        rv = self.coordinator.requeue(task.id)
        assert rv is True

    def test_requeue_non_exist_task(self):
        rv = self.coordinator.requeue(self._new_task_id())
        assert rv is False

    def test_retry(self):
        vq = self._add_vq()
        vq.add_task()
        task = self.coordinator.receive(max_number=1)[0]

        rv = self.coordinator.retry(task.id)
        assert rv is True
        assert self.coordinator.receive(max_number=1)[0].id == task.id

    def test_retry_with_delay(self):
        vq = self._add_vq()
        vq.add_task()
        task = self.coordinator.receive(max_number=1)[0]

        rv = self.coordinator.retry(task.id, delay_millis=1000)
        assert rv is True
        assert not self.coordinator.receive(max_number=1)

    def test_retry_with_error_message(self):
        vq = self._add_vq()
        vq.add_task()
        task = self.coordinator.receive(max_number=1)[0]

        rv = self.coordinator.retry(task.id, error_message="network error")
        assert rv is True
        assert self.coordinator.receive(max_number=1)[0].id == task.id

        task_model = self._get_task_model(task.id)
        assert len(task_model.errors) == 1
        assert task_model.errors[0].err_msg == "network error"

    def test_retry_idempotent(self):
        vq = self._add_vq()
        vq.add_task()
        task = self.coordinator.receive(max_number=1)[0]

        rv = self.coordinator.retry(task.id, delay_millis=1000)
        assert rv is True
        rv = self.coordinator.retry(task.id, delay_millis=1000)
        assert rv is True
        rv = self.coordinator.retry(task.id, delay_millis=2000)
        assert rv is True

    def test_retry_non_exist_task(self):
        rv = self.coordinator.retry(self._new_task_id())
        assert rv is False

    def test_len(self):
        assert len(self.coordinator) == 0

    def test_len_with_tasks(self):
        vq = self._add_vq()

        vq.add_task()
        assert len(self.coordinator) == 1

        vq.add_task()
        assert len(self.coordinator) == 2

    def test_len_with_completed_tasks(self):
        vq = self._add_vq()
        vq.add_task()
        vq.add_task()
        task = self.coordinator.receive()[0]
        self.coordinator.ack(task.id)
        assert len(self.coordinator) == 1

    def test_multithread_receive(self):
        vq = self._add_vq()
        for _ in range(100):
            vq.add_task()

        def receivce(i):
            try:
                task = self.coordinator.receive()
                assert task
            except Exception as e:
                print(e)
                raise

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(receivce, range(100)))

    def test_multithread_ack(self):
        vq = self._add_vq()
        for _ in range(100):
            vq.add_task()
        q = []
        for task in self.coordinator.receive(max_number=100):
            q.append(task.id)

        def ack_wrap(ack_fn):
            def wrap(task_id):
                try:
                    assert ack_fn(task_id)
                except Exception as e:
                    print(e)
                    raise

            return wrap

        def raw_ack(task_id):
            id_bytes = uuid.UUID(task_id).bytes
            # For a connection context without a transaction, use Database.connection_context().
            # `with self.db:` is using execution context, where a transaction is used, which will have a BEGIN before executing the following SQL and potentially cause more database lock operation errors (even if the execution is not timeout and total time is less than 0.03s)
            with self.db.connection_context():
                ts = int(time.time() * 1000)
                c = self.db.execute_sql(
                    'SELECT "t1"."id", "t1"."status" FROM "default_task" AS "t1" WHERE ("t1"."id" = ?) LIMIT ?',
                    [id_bytes, 1],
                )
                c.fetchone()
                c.close()

                c = self.db.execute_sql(
                    'UPDATE "default_task" SET "visible_at" = ?, "status" = ?, "ended_at" = ?, "updated_at" = ? WHERE ("default_task"."id" = ?)',
                    [2147483647000, 100, ts, ts, id_bytes],
                )
                c.close()
                return c.rowcount

        with self.subTest("ack"):
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(ack_wrap(self.coordinator.ack), q))

        with self.subTest("raw_ack"):
            assert (
                self.db._max_connections > 1
            ), "single connection pool will block threads working concurrently"
            with self.assertRaisesRegex(
                peewee.OperationalError, "database table is locked: default_task"
            ):
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                    list(executor.map(ack_wrap(raw_ack), q))

    def test_multithread_retry(self):
        vq = self._add_vq()
        for _ in range(100):
            vq.add_task()
        q = [task.id for task in self.coordinator.receive(max_number=100)]

        def retry(task_id):
            try:
                assert self.coordinator.retry(task_id)
            except Exception as e:
                print(e)
                raise

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(retry, q))

    def test_receive_block(self):
        vq = self._add_vq()

        def block_receive():
            try:
                tasks = self.coordinator.receive(wait_time_seconds=3600)
            except Exception as e:
                print(e)
                raise
            else:
                assert tasks

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(block_receive)
            time.sleep(0.1)
            vq.add_task()
            assert not future.exception(), future.exception()

    def test_receive_multiple_block(self):
        vq = self._add_vq()

        def block_receive():
            try:
                tasks = self.coordinator.receive(wait_time_seconds=3600)
            except Exception as e:
                print(e)
                raise
            else:
                assert tasks

        with concurrent.futures.ThreadPoolExecutor() as executor:
            fs = [executor.submit(block_receive) for i in range(10)]
            time.sleep(0.1)
            for i in range(11):
                vq.add_task()
            for future in concurrent.futures.as_completed(fs):
                assert not future.exception(), future.exception()

        tasks = self.coordinator.receive()
        assert tasks
        assert len(tasks) == 1

        tasks = self.coordinator.receive()
        assert not tasks
