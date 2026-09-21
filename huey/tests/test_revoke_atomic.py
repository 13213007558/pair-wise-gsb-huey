import datetime
import os
import threading
import time
import unittest

from huey.api import MemoryHuey
from huey.api import SqliteHuey
from huey.constants import EmptyData
from huey.storage import MemoryStorage
from huey.storage import SqliteStorage
from huey.tests.base import BaseTestCase


class BarrierStorageMixin(object):
    # Storage wrapper that inserts a controllable barrier into the read path
    # used by Huey.is_revoked(), so tests can deterministically place two
    # workers inside the revocation check before either one attempts to
    # atomically consume the flag.
    read_barrier = None

    def peek_many(self, keys):
        data = super(BarrierStorageMixin, self).peek_many(keys)
        if self.read_barrier is not None:
            self.read_barrier.wait(timeout=10)
        return data


class BarrierMemoryStorage(BarrierStorageMixin, MemoryStorage):
    pass


class BarrierSqliteStorage(BarrierStorageMixin, SqliteStorage):
    pass


class RevokeAtomicityTests(object):
    # Verify that consuming a one-shot revocation is atomic with respect to
    # the execute/don't-execute decision, using real storage backends and
    # concurrent workers synchronized on a barrier.
    huey_class = None
    storage_class = None
    huey_kwargs = {}

    def get_huey(self):
        return self.huey_class(storage_class=self.storage_class,
                               utc=False, **self.huey_kwargs)

    def run_concurrent(self, task, nworkers=2):
        # Arm the barrier so every worker reads the revocation flag before
        # any of them proceeds to consume it, then execute the same task.
        self.huey.storage.read_barrier = threading.Barrier(nworkers)
        outcomes = {}
        outcomes_lock = threading.Lock()

        def worker(n):
            outcome = self.huey.execute(task)
            with outcomes_lock:
                outcomes[n] = outcome

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(nworkers)]
        for thread in threads:
            thread.start()
        try:
            for thread in threads:
                thread.join(15)
                self.assertFalse(thread.is_alive())
        finally:
            self.huey.storage.read_barrier = None
        return outcomes

    def test_revoke_once_consumed_by_exactly_one_worker(self):
        executed = []
        executed_lock = threading.Lock()

        @self.huey.task()
        def task_a(n):
            with executed_lock:
                executed.append(n)
            return n + 1

        r = task_a(1)
        task = self.huey.dequeue()
        self.huey.revoke(task, revoke_once=True)

        # Two workers race to execute the same task (e.g. after a duplicate
        # delivery or requeue). Both read the flag before either consumes it.
        outcomes = self.run_concurrent(task)

        # Exactly one worker observed the revocation and skipped; the other
        # executed the task normally.
        skipped = sum(1 for outcome in outcomes.values() if outcome is None)
        self.assertEqual(skipped, 1)
        self.assertEqual(executed, [1])
        self.assertEqual(r.get(), 2)

        # The flag was consumed exactly once -- it is gone now.
        self.assertFalse(self.huey.is_revoked(task))
        self.assertTrue(self.huey.storage.peek_data(task.revoke_id)
                        is EmptyData)

    def test_revoke_until_active_blocks_all_workers(self):
        executed = []

        @self.huey.task()
        def task_a(n):
            executed.append(n)
            return n + 1

        task_a(1)
        task = self.huey.dequeue()
        until = datetime.datetime.now() + datetime.timedelta(hours=1)
        self.huey.revoke(task, revoke_until=until)

        outcomes = self.run_concurrent(task)

        # A time-bound revocation is not consumed: all workers skip and the
        # flag remains in place.
        self.assertEqual(list(outcomes.values()), [None, None])
        self.assertEqual(executed, [])
        self.assertTrue(self.huey.is_revoked(task))

    def test_expired_revoke_until_never_blocks(self):
        executed = []
        executed_lock = threading.Lock()

        @self.huey.task()
        def task_a(n):
            with executed_lock:
                executed.append(n)
            return n + 1

        task_a(1)
        task = self.huey.dequeue()
        until = datetime.datetime.now() - datetime.timedelta(seconds=1)
        self.huey.revoke(task, revoke_until=until)

        outcomes = self.run_concurrent(task)

        # An expired flag is not a revocation: every worker executes, and
        # exactly one of them atomically cleans up the expired flag.
        self.assertEqual(sorted(outcomes.values()), [2, 2])
        self.assertEqual(sorted(executed), [1, 1])
        self.assertTrue(self.huey.storage.peek_data(task.revoke_id)
                        is EmptyData)

    def test_requeued_task_revoked_once(self):
        executed = []

        @self.huey.task()
        def task_a(n):
            executed.append(n)
            return n + 1

        task_a(1)
        task = self.huey.dequeue()

        # The same task id ends up enqueued twice (requeue / redelivery).
        self.huey.enqueue(task)
        self.huey.enqueue(task)
        self.huey.revoke(task, revoke_once=True)

        # The one-shot revocation cancels exactly one of the two deliveries.
        self.assertTrue(self.huey.execute(self.huey.dequeue()) is None)
        self.assertEqual(self.huey.execute(self.huey.dequeue()), 2)
        self.assertEqual(executed, [1])

    def test_crash_after_consume_does_not_block(self):
        executed = []

        @self.huey.task()
        def task_a(n):
            executed.append(n)
            return n + 1

        task_a(1)
        task = self.huey.dequeue()
        self.huey.revoke(task, revoke_once=True)

        # Simulate a worker that atomically consumes the revocation flag and
        # then crashes before finishing. Consumption is a single atomic
        # storage operation -- no lock or lease is held -- so nothing is
        # left dangling after the crash.
        self.assertTrue(self.huey.is_revoked(task, peek=False))
        del task  # Worker "crashes" here.

        # A subsequent delivery of the same task id is not blocked: the
        # one-shot revocation was already consumed by the crashed worker.
        task_a(1)
        task = self.huey.dequeue()
        self.assertEqual(self.huey.execute(task), 2)
        self.assertEqual(executed, [1])

    def test_crash_before_consume_preserves_revocation(self):
        executed = []

        @self.huey.task()
        def task_a(n):
            executed.append(n)
            return n + 1

        task_a(1)
        task = self.huey.dequeue()
        self.huey.revoke(task, revoke_once=True)

        # Simulate a worker that dequeues the task and crashes before the
        # revocation check runs. The flag is still in place, so the next
        # worker to execute the task is revoked -- exactly once.
        self.assertTrue(self.huey.execute(task) is None)
        self.assertEqual(executed, [])

        # The revocation was consumed by that check, so a redelivery of the
        # same task id executes normally. No permanent blocking.
        self.huey.enqueue(task)
        self.assertEqual(self.huey.execute(self.huey.dequeue()), 2)
        self.assertEqual(executed, [1])

    def test_retry_rechecks_revocation(self):
        executed = []

        @self.huey.task(retries=2)
        def task_a(n):
            executed.append(n)
            if len(executed) == 1:
                raise Exception('first attempt fails')
            return n + 1

        task_a(1)
        task = self.huey.dequeue()

        # First attempt runs and fails, and is requeued for retry.
        self.assertTrue(self.huey.execute(task) is None)
        self.assertEqual(executed, [1])
        self.assertEqual(len(self.huey), 1)

        # The retried task is revoked once before its second attempt.
        retry_task = self.huey.dequeue()
        self.huey.revoke(retry_task, revoke_once=True)
        self.assertTrue(self.huey.execute(retry_task) is None)
        self.assertEqual(executed, [1])

        # The one-shot revocation was consumed by the skipped attempt, so a
        # subsequent redelivery executes normally.
        self.huey.enqueue(retry_task)
        self.assertEqual(self.huey.execute(self.huey.dequeue()), 2)
        self.assertEqual(executed, [1, 1])

    def test_consumer_duplicate_delivery(self):
        executed = []
        executed_lock = threading.Lock()

        @self.huey.task()
        def task_a(n):
            with executed_lock:
                executed.append(n)
            return n + 1

        r = task_a(1)
        task = self.huey.dequeue()
        self.huey.enqueue(task)
        self.huey.enqueue(task)
        self.huey.revoke(task, revoke_once=True)

        with self.consumer_context(workers=2):
            self.assertEqual(r.get(blocking=True, timeout=5), 2)
            deadline = time.time() + 5
            while time.time() < deadline and len(self.huey) > 0:
                time.sleep(0.01)

        # Across both workers and both deliveries, the task ran exactly once.
        self.assertEqual(executed, [1])
        self.assertFalse(self.huey.is_revoked(task))


class TestMemoryRevokeAtomicity(RevokeAtomicityTests, BaseTestCase):
    huey_class = MemoryHuey
    storage_class = BarrierMemoryStorage


class TestSqliteRevokeAtomicity(RevokeAtomicityTests, BaseTestCase):
    huey_class = SqliteHuey
    storage_class = BarrierSqliteStorage
    db_file = 'huey_revoke_atomic.db'
    huey_kwargs = {'filename': db_file}

    def tearDown(self):
        super(TestSqliteRevokeAtomicity, self).tearDown()
        self.huey.storage.close()
        if os.path.exists(self.db_file):
            os.unlink(self.db_file)
