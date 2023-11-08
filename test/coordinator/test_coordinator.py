import unittest
import time
from vtq import coordinator


class CoordinatorTestCase(unittest.TestCase):
    def setUp(self):
        self.coordinator = coordinator.Coordinator()
        self.db = self.coordinator._db
        self.vq_cls = self.coordinator._vq_cls
        self.task_cls = self.coordinator._task_cls

        # clear tables
        with self.db:
            self.task_cls.truncate_table()
            self.vq_cls.truncate_table()

    def _enable_model_debug_logging():
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
