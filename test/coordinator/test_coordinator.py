import unittest
import time
import uuid
from vtq import workspace


class CoordinatorTestCase(unittest.TestCase):
    def setUp(self):
        ws = workspace.DefaultWorkspace()
        self.db = ws.database
        self.coordinator = ws.coordinator
        self.vq_cls = self.coordinator._vq_cls
        self.task_cls = self.coordinator._task_cls
        self.task_error_cls = self.coordinator._task_error_cls

        # clear tables
        with self.db:
            self.task_error_cls.truncate_table()
            self.task_cls.truncate_table()
            self.vq_cls.truncate_table()

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
                return task.id

        return VQ(name, self.db, self.task_cls)

    def _get_ids(self, tasks):
        return list(map(lambda t: t.id, tasks))

    def _new_task_id(self):
        return uuid.uuid4()

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
