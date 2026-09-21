import datetime
import unittest

from huey.api import MemoryHuey
from huey.exceptions import TaskException


class FakeClock(object):
    def __init__(self):
        self.t = 1000000.0

    def __call__(self):
        return self.t


def as_dt(epoch):
    return datetime.datetime.fromtimestamp(
        epoch, tz=datetime.timezone.utc).replace(tzinfo=None)


class AgingHueyTestMixin(object):
    def get_huey(self, **kwargs):
        raise NotImplementedError

    def setUp(self):
        self.clock = FakeClock()
        self.huey = self.get_huey()
        self.huey.storage.now = self.clock
        self.state = []

        def record(n):
            self.state.append(n)
            return n

        self.high = self.huey.task(priority=10, name='high')(record)
        self.low = self.huey.task(priority=0, name='low')(record)

    def test_disabled_by_default(self):
        huey = MemoryHuey(utc=False)
        self.assertFalse(huey.storage.aging)

    def test_flood_starvation_relief(self):
        self.low('maint')
        for i in range(20):
            self.clock.t += 1
            self.high('h%d' % i)
            task = self.huey.dequeue()
            self.huey.execute(task, as_dt(self.clock.t))
        self.assertEqual(self.state[0], 'h0')
        self.assertNotIn('maint', self.state)

        # 100+ seconds later, the aged maintenance task beats fresh highs.
        self.clock.t = 1000102.0
        self.high('fresh')
        task = self.huey.dequeue()
        self.assertEqual(task.name, 'low')
        self.huey.execute(task, as_dt(self.clock.t))
        self.assertEqual(self.state[-1], 'maint')

    def test_same_priority_fifo(self):
        for i in range(5):
            self.low('l%d' % i)
            self.clock.t += 3
        order = []
        while True:
            task = self.huey.dequeue()
            if task is None:
                break
            order.append(task.args[0])
        self.assertEqual(order, ['l%d' % i for i in range(5)])

    def test_retry_without_delay_resets_wait(self):
        attempts = [0]

        @self.huey.task(priority=0, retries=1)
        def flaky():
            attempts[0] += 1
            if attempts[0] == 1:
                raise Exception('boom')
            self.state.append('ok')

        flaky()
        self.clock.t = 1000100.0
        self.high('h')
        # The flaky task has waited 100s -- it aged to priority 10 and runs
        # before the fresh static-priority-10 task (same eff, but older).
        task = self.huey.dequeue()
        self.assertEqual(task.name, 'flaky')
        ts = as_dt(self.clock.t)
        self.huey.execute(task, ts)  # Fails and re-enqueues immediately.
        # The re-enqueued retry is fresh again: the high task runs first.
        task = self.huey.dequeue()
        self.assertEqual(task.name, 'high')
        task = self.huey.dequeue()
        self.assertEqual(task.name, 'flaky')

    def test_eta_task_waits_in_schedule_not_queue(self):
        eta = as_dt(self.clock.t + 60)
        self.low.schedule(('scheduled',), eta=eta)
        # The worker dequeues the not-ready task, and execute() parks it in
        # the schedule instead of running it.
        task = self.huey.dequeue()
        self.huey.execute(task, as_dt(self.clock.t))
        self.assertEqual(self.huey.pending_count(), 0)
        self.assertEqual(self.huey.scheduled_count(), 1)

        # Before the eta nothing becomes runnable.
        self.assertEqual(self.huey.read_schedule(as_dt(self.clock.t + 59)),
                         [])
        # At/after eta the scheduler reads and re-enqueues the task; its
        # aging clock starts from this fresh enqueue time.
        ready = self.huey.read_schedule(as_dt(self.clock.t + 60))
        self.assertEqual(len(ready), 1)
        for task in ready:
            self.huey.enqueue(task)
        self.assertEqual(self.huey.pending_count(), 1)
        self.assertEqual(self.huey.scheduled_count(), 0)
        # Freshly enqueued at eta: the aging clock started at enqueue time.
        self.assertEqual(self.huey.dequeue().args[0], 'scheduled')

    def test_invalid_aging_config(self):
        self.assertRaises(ValueError, self.get_huey, aging_step=0)
        self.assertRaises(ValueError, self.get_huey, aging_step=1,
                          aging_threshold=-1)
        self.assertRaises(ValueError, self.get_huey, aging_step=1,
                          aging_max_boost=-1)

    def test_invalid_priority_rejected(self):
        self.assertRaises((TypeError, ValueError), self.low, 'x',
                          priority=float('nan'))


class TestMemoryAgingHuey(AgingHueyTestMixin, unittest.TestCase):
    def get_huey(self, **kwargs):
        kwargs.setdefault('utc', False)
        kwargs.setdefault('aging_step', 10.0)
        return MemoryHuey(**kwargs)


class TestSqliteAgingHuey(AgingHueyTestMixin, unittest.TestCase):
    db_path = 'test_aging_api.db'

    def tearDown(self):
        import os
        try:
            self.huey.storage.close()
        except Exception:
            pass
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        for suffix in ('-wal', '-shm'):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.unlink(path)

    def get_huey(self, **kwargs):
        from huey.api import SqliteHuey
        kwargs.setdefault('utc', False)
        kwargs.setdefault('aging_step', 10.0)
        kwargs['filename'] = self.db_path
        return SqliteHuey(**kwargs)

    def test_disabled_by_default(self):
        from huey.api import SqliteHuey
        if __import__('os').path.exists(self.db_path):
            __import__('os').unlink(self.db_path)
        huey = SqliteHuey(filename=self.db_path, utc=False)
        self.assertFalse(huey.storage.aging)
