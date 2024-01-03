import unittest
import time
import concurrent.futures
import uuid
from vtq import workspace
from vtq import model
from vtq.coordinator.notification_worker import SimpleNotificationWorker


class CoordinatorTestCase(unittest.TestCase):
    def setUp(self):
        self.notification_interval = 0.1
        notification_worker = SimpleNotificationWorker(
            interval=self.notification_interval
        )
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
        model.enable_debug_logging()

    def _add_vq(self, name="default", **kwargs):
        with self.db:
            self.vq_cls.create(name=name, **kwargs)

        class VQ:
            def __init__(self, name, db, task_cls):
                self.name = name
                self.db = db
                self.task_cls = task_cls
                self.queued_at_start = 0
                self.ordered_queued_at = False

            def enable_ordered_queued_at(self, start=1):
                self.ordered_queued_at = True
                self.queued_at_start = start

            def disable_ordered_queued_at(self):
                self.ordered_queued_at = False

            @model.retry_sqlite_db_table_locked
            def add_task(self, **kwargs) -> str:
                kwargs.setdefault("data", b"data")
                if self.ordered_queued_at:
                    kwargs.setdefault("queued_at", self.queued_at_start)
                    self.queued_at_start += 1
                with self.db:
                    task = self.task_cls.create(vqueue_name=name, **kwargs)
                return task.id.hex

        return VQ(name, self.db, self.task_cls)

    def _get_ids(self, tasks):
        return list(map(lambda t: t.id, tasks))

    def _new_task_id(self):
        return uuid.uuid4().hex

    def _get_task_model(self, task_id):
        with self.db:
            task_models = (
                self.task_cls.select(self.task_cls)
                .where(self.task_cls.id == task_id)
                .prefetch(self.task_error_cls)
            )
            assert len(task_models) <= 1
            return task_models[0] if task_models else None

    def test_enqueue(self):
        task_id = self.coordinator.enqueue(b"123")
        assert task_id
        assert isinstance(task_id, str)

    def test_receive_data(self):
        vq = self._add_vq()
        data = b"mydata"
        task_id = vq.add_task(data=data)
        tasks = self.coordinator.receive(max_number=1)
        assert len(tasks) == 1
        assert self._get_ids(tasks)[0] == task_id

        task = tasks[0]
        assert task.data == data
        assert task.meta.retries == 0

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
        ts = time.time()
        vq = self._add_vq("default")
        vq_hidden = self._add_vq("hidden", visible_at=ts + 100)

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
        task_id1 = vq.add_task(queued_at=1)
        task_id2 = vq.add_task(queued_at=2)
        task_id3 = vq.add_task(queued_at=3)

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

    def test_receive_rate_limit_mutex(self):
        vq = self._add_vq()
        vq_mutex_1 = self._add_vq("vq_mutex_1", rate_limit_type="MUTEX")
        vq_mutex_2 = self._add_vq("vq_mutex_2", rate_limit_type="MUTEX")

        task_id1 = vq.add_task(queued_at=1)
        task_id2 = vq_mutex_1.add_task(queued_at=2)
        task_id3 = vq_mutex_2.add_task(queued_at=3)
        _ = vq_mutex_2.add_task(queued_at=4)
        _ = vq_mutex_1.add_task(queued_at=5)
        task_id6 = vq.add_task(queued_at=6)

        tasks = self.coordinator.receive(max_number=6)
        assert len(tasks) == 4, len(tasks)
        assert self._get_ids(tasks) == [task_id1, task_id2, task_id3, task_id6]

    def test_receive_load_balancing(self):
        vq = self._add_vq("vq", bucket_weight=100)
        vq_lb = self._add_vq("vq_lb", bucket_name="vq_lb", bucket_weight=200)

        vq_task_ids = set()
        vq_lb_task_ids = set()
        for _ in range(100):
            vq_task_ids.add(vq.add_task())
            vq_lb_task_ids.add(vq_lb.add_task())

        task_ids = set(
            (self.coordinator.receive(max_number=1)[0].id for _ in range(100))
        )
        vq_tasks_num = len(task_ids.intersection(vq_task_ids))
        assert abs(vq_tasks_num - 33) <= 20, vq_tasks_num

    def test_receive_load_balancing_batch_query(self):
        vq = self._add_vq("vq", bucket_weight=100)
        vq_lb = self._add_vq("vq_lb", bucket_name="vq_lb", bucket_weight=200)

        vq_task_ids = set()
        vq_lb_task_ids = set()
        for _ in range(100):
            vq_task_ids.add(vq.add_task())
            vq_lb_task_ids.add(vq_lb.add_task())

        task_ids = set((task.id for task in self.coordinator.receive(max_number=100)))
        assert len(task_ids) == 100, len(task_ids)
        vq_tasks_num = len(task_ids.intersection(vq_task_ids))
        assert abs(vq_tasks_num - 33) <= 20, vq_tasks_num

    def test_receive_load_balancing_bucket_weight_0(self):
        vq = self._add_vq("vq", bucket_weight=100)
        vq_lb = self._add_vq("vq_lb", bucket_name="vq_lb", bucket_weight=0)

        vq_task_ids = set()
        vq_lb_task_ids = set()
        for _ in range(100):
            vq_task_ids.add(vq.add_task())
            vq_lb_task_ids.add(vq_lb.add_task())

        task_ids = set((task.id for task in self.coordinator.receive(max_number=100)))
        assert len(task_ids) == 100
        vq_tasks_num = len(task_ids.intersection(vq_task_ids))
        assert vq_tasks_num == 100

    def test_receive_load_balancing_multiple_vqs_in_one_bucket(self):
        vq = self._add_vq("vq", bucket_weight=100)
        vq_lb = self._add_vq("vq_lb", bucket_name="vq_lb", bucket_weight=200)
        vq_lb2 = self._add_vq("vq_lb2", bucket_name="vq_lb", bucket_weight=200)

        vq_task_ids = set()
        vq_lb_task_ids = set()
        vq_lb2_task_ids = set()
        queued_at = 0
        for _ in range(100):
            queued_at += 1
            vq_task_ids.add(vq.add_task(queued_at=queued_at))
            queued_at += 1
            vq_lb_task_ids.add(vq_lb.add_task(queued_at=queued_at))
            queued_at += 1
            vq_lb2_task_ids.add(vq_lb2.add_task(queued_at=queued_at))

        task_ids = set((task.id for task in self.coordinator.receive(max_number=100)))
        assert len(task_ids) == 100
        vq_tasks_num = len(task_ids.intersection(vq_task_ids))
        assert abs(vq_tasks_num - 33) <= 20, vq_tasks_num

        vq_lb_tasks_num = len(task_ids.intersection(vq_lb_task_ids))
        vq_lb2_tasks_num = len(task_ids.intersection(vq_lb2_task_ids))

        assert abs(vq_lb2_tasks_num - vq_lb_tasks_num) <= 1, (
            vq_lb_tasks_num,
            vq_lb2_tasks_num,
        )

    def test_receive_load_balancing_multiple_buckets(self):
        vq = self._add_vq("vq", bucket_weight=100)
        vq_lb = self._add_vq("vq_lb", bucket_name="vq_lb", bucket_weight=200)
        vq_lb2 = self._add_vq("vq_lb2", bucket_name="vq_lb2", bucket_weight=100)

        vq_task_ids = set()
        vq_lb_task_ids = set()
        vq_lb2_task_ids = set()
        for _ in range(100):
            vq_task_ids.add(vq.add_task())
            vq_lb_task_ids.add(vq_lb.add_task())
            vq_lb2_task_ids.add(vq_lb2.add_task())

        task_ids = set((task.id for task in self.coordinator.receive(max_number=100)))
        assert len(task_ids) == 100
        vq_tasks_num = len(task_ids.intersection(vq_task_ids))
        assert abs(vq_tasks_num - 25) <= 20, vq_tasks_num

        vq_lb2_tasks_num = len(task_ids.intersection(vq_lb2_task_ids))
        assert abs(vq_lb2_tasks_num - 25) <= 20, vq_lb2_tasks_num

    def test_receive_hybrid(self):
        ts = time.time()

        vq = self._add_vq("default", priority=50)
        vq_hidden = self._add_vq("hidden", priority=50, visible_at=ts + 100)
        vq_high_priority = self._add_vq("high", priority=70)
        vq_mutex = self._add_vq("vq_mutex", priority=50, rate_limit_type="MUTEX")
        vq_lb1 = self._add_vq(
            "vq_lb1", priority=60, bucket_name="lb1", bucket_weight=100
        )
        vq_lb2 = self._add_vq(
            "vq_lb2", priority=60, bucket_name="lb2", bucket_weight=100
        )
        vq_lb3 = self._add_vq(
            "vq_lb3",
            priority=60,
            bucket_name="lb3",
            bucket_weight=100,
            rate_limit_type="MUTEX",
        )

        vq_high_priority.enable_ordered_queued_at(1)
        _ = vq_high_priority.add_task(priority=30, visible_at=ts + 100)
        p2 = vq_high_priority.add_task(priority=30, visible_at=ts - 100)
        p3 = vq_high_priority.add_task(priority=30, visible_at=ts)
        _ = vq_high_priority.add_task(priority=70, visible_at=ts + 100)
        p5 = vq_high_priority.add_task(priority=70, visible_at=ts)
        p6 = vq_high_priority.add_task(priority=70, visible_at=ts - 100)

        vq_hidden.enable_ordered_queued_at(11)
        _ = vq_hidden.add_task(priority=30, visible_at=ts + 100)
        _ = vq_hidden.add_task(priority=30, visible_at=ts - 100)
        _ = vq_hidden.add_task(priority=30, visible_at=ts)
        _ = vq_hidden.add_task(priority=70, visible_at=ts + 100)
        _ = vq_hidden.add_task(priority=70, visible_at=ts)
        _ = vq_hidden.add_task(priority=70, visible_at=ts - 100)

        vq.enable_ordered_queued_at(21)
        _ = vq.add_task(priority=30, visible_at=ts + 100)
        t2 = vq.add_task(priority=30, visible_at=ts - 100)
        t3 = vq.add_task(priority=30, visible_at=ts)
        _ = vq.add_task(priority=70, visible_at=ts + 100)
        t5 = vq.add_task(priority=70, visible_at=ts)
        t6 = vq.add_task(priority=70, visible_at=ts - 100)

        vq_mutex.enable_ordered_queued_at(31)
        m = [
            vq_mutex.add_task(priority=30, visible_at=ts + 100),
            vq_mutex.add_task(priority=30, visible_at=ts - 100),
            vq_mutex.add_task(priority=30, visible_at=ts),
            vq_mutex.add_task(priority=70, visible_at=ts + 100),
            vq_mutex.add_task(priority=70, visible_at=ts),
            vq_mutex.add_task(priority=70, visible_at=ts - 100),
        ]

        vq_lb1.enable_ordered_queued_at(41)
        lb1 = [
            vq_lb1.add_task(priority=30, visible_at=ts + 100),
            vq_lb1.add_task(priority=30, visible_at=ts - 100),
            vq_lb1.add_task(priority=30, visible_at=ts),
            vq_lb1.add_task(priority=70, visible_at=ts + 100),
            vq_lb1.add_task(priority=70, visible_at=ts),
            vq_lb1.add_task(priority=70, visible_at=ts - 100),
        ]

        vq_lb2.enable_ordered_queued_at(51)
        lb2 = [
            vq_lb2.add_task(priority=30, visible_at=ts + 100),
            vq_lb2.add_task(priority=30, visible_at=ts - 100),
            vq_lb2.add_task(priority=30, visible_at=ts),
            vq_lb2.add_task(priority=70, visible_at=ts + 100),
            vq_lb2.add_task(priority=70, visible_at=ts),
            vq_lb2.add_task(priority=70, visible_at=ts - 100),
        ]

        vq_lb3.enable_ordered_queued_at(61)
        lb3 = [
            vq_lb3.add_task(priority=30, visible_at=ts + 100),
            vq_lb3.add_task(priority=30, visible_at=ts - 100),
            vq_lb3.add_task(priority=30, visible_at=ts),
            vq_lb3.add_task(priority=70, visible_at=ts + 100),
            vq_lb3.add_task(priority=70, visible_at=ts),
            vq_lb3.add_task(priority=70, visible_at=ts - 100),
        ]

        tasks = self.coordinator.receive(max_number=7 * 6)
        assert len(tasks) == 4 + 9 + 5
        assert self._get_ids(tasks)[:4] == [p5, p6, p2, p3]
        assert self._get_ids(tasks)[-5:] == [t5, t6, m[4], t2, t3]
        assert self._get_ids(tasks)[4:8] == [lb1[4], lb1[5], lb2[4], lb2[5]]
        assert self._get_ids(tasks)[9:13] == [lb1[1], lb1[2], lb2[1], lb2[2]]
        assert self._get_ids(tasks)[8] == lb3[4]

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

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(ack_wrap(self.coordinator.ack), q))

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
                assert tasks, "no task received"
                assert len(tasks) == 1, "received more than 1 task"

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(block_receive)
            time.sleep(0.1)
            vq.add_task()
            assert not future.exception(), future.exception()

    def test_receive_block_timeout(self):
        tasks = self.coordinator.receive(
            wait_time_seconds=min(self.notification_interval / 2, 0.1)
        )
        assert not tasks

    def test_receive_block_timeout2(self):
        tasks = self.coordinator.receive(
            wait_time_seconds=self.notification_interval + 0.1
        )
        assert not tasks

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

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            fs = [executor.submit(block_receive) for i in range(20)]
            time.sleep(0.1)
            for _ in range(21):
                vq.add_task()
            for future in concurrent.futures.as_completed(fs):
                assert not future.exception(), future.exception()

        tasks = self.coordinator.receive()
        assert tasks
        assert len(tasks) == 1

        tasks = self.coordinator.receive()
        assert not tasks
