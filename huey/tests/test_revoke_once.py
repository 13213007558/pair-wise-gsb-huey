"""
Tests for the atomic consumption of one-shot (``revoke_once``) markers when
multiple consumers race against the same storage.

The threaded race is made deterministic with a barrier placed inside the
storage's compare-and-delete primitive, which guarantees every worker has
observed the marker *before* any worker is allowed to consume it.
"""
import datetime
import multiprocessing
import os
import shutil
import tempfile
import threading
import unittest

from huey.api import Huey
from huey.api import MemoryHuey
from huey.api import SqliteHuey
from huey.constants import EmptyData
from huey.signals import SIGNAL_REVOKED

try:
    from huey.api import RedisHuey
    from redis import Redis
except ImportError:
    RedisHuey = Redis = None

def redis_available():
    if Redis is None:
        return False
    try:
        Redis().ping()
    except Exception:
        return False
    return True


# A task function shared by every Huey instance created in these tests. It is
# registered under a stable name so tasks produced by one Huey instance can be
# executed by another (simulating distinct workers sharing one storage).
def shared_task(worker, n):
    return (worker, n)


FLAKY_ATTEMPTS = []


def tracked_flaky_task(worker, n):
    FLAKY_ATTEMPTS.append((worker, n))
    raise ValueError('boom')


# Registry name is ``module.class-name``; the test module may be imported
# under different dotted names depending on the test runner.
_probe = MemoryHuey(results=False, store_errors=False)
_wrapper = _probe.task()(shared_task)
TASK_NAME = _probe._registry.task_to_string(_wrapper.task_class)


class BarrierStorage(object):
    """
    Storage wrapper that blocks every consumer inside the compare-and-delete
    primitive until all of them have reached it. With the barrier in place
    every worker provably performs its non-destructive peek of the revocation
    marker before any worker is permitted to consume it.
    """
    def __init__(self, storage, barrier):
        self._storage = storage
        self._barrier = barrier

    def delete_if_value(self, key, value):
        self._barrier.wait()
        return self._storage.delete_if_value(key, value)

    def __getattr__(self, name):
        return getattr(self._storage, name)


class ProcessBarrierStorage(BarrierStorage):
    # Same behavior; named distinctly so the spawned child processes import
    # a top-level, picklable wrapper.
    pass


def _sqlite_race_worker(idx, storage_name, db_path, barrier, queue):
    huey = SqliteHuey(name=storage_name, filename=db_path, timeout=10,
                      utc=False, results=False)
    huey.storage = ProcessBarrierStorage(huey.storage, barrier)
    huey.task()(shared_task)
    fired = []
    huey.signal(SIGNAL_REVOKED)(lambda *a: fired.append(1))
    task = huey.dequeue()
    value = huey.execute(task)
    queue.put((idx, value, bool(fired)))


class RevokeOnceConcurrencyTests(object):
    storage_name = 'huey-test-revoke-once'
    n_workers = 8

    def setUp(self):
        self._storage = self.create_shared_storage()

    # Subclasses provide a storage instance shared by every worker Huey.
    # Real backends return an instance whose each new handle/connection still
    # points at the same backing database.
    def create_shared_storage(self):
        raise NotImplementedError

    def make_storage(self):
        # By default every worker reuses the same storage object (memory
        # backend). Backends whose handle is per-process/connection override
        # this to open a fresh connection to the same backing store.
        return self._storage

    def make_worker_huey(self, barrier=None):
        storage = self.make_storage()
        if barrier is not None:
            storage = BarrierStorage(storage, barrier)
        huey = MemoryHuey(name=self.storage_name, utc=False, results=False,
                          store_errors=False)
        huey.storage = storage
        huey.task()(shared_task)
        return huey

    def run_race(self, revoke):
        """Execute one queued task per worker, returning per-worker tuples
        of ``(worker_id, value, revoked_signal_fired)``."""
        producer = self.make_worker_huey()
        task_cls = producer._registry.string_to_task(TASK_NAME)

        # Each worker gets its own task (distinct id) but they all share a
        # task-class revocation, so they race over a single marker.
        for i in range(self.n_workers):
            producer.storage.enqueue(
                producer.serializer.serialize(
                    producer._registry.create_message(task_cls((i, i)))))

        if revoke:
            producer.revoke_all(task_cls, revoke_once=True)

        barrier = threading.Barrier(self.n_workers)
        outcomes = []
        outcomes_lock = threading.Lock()

        def worker(idx):
            huey = self.make_worker_huey(barrier)
            fired = []
            huey.signal(SIGNAL_REVOKED)(lambda *a: fired.append(idx))
            task = huey.dequeue()
            value = huey.execute(task)
            with outcomes_lock:
                outcomes.append((idx, value, bool(fired)))

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(self.n_workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
        outcomes.sort()
        return outcomes

    def test_revoke_once_consumed_by_single_worker(self):
        outcomes = self.run_race(revoke=True)
        revoked_workers = [idx for idx, value, fired in outcomes if fired]
        executed = [(idx, value) for idx, value, fired in outcomes
                    if not fired]

        # Exactly one worker consumed the marker: exactly one revoked signal,
        # exactly one un-executed task, and all other dequeued tasks ran.
        self.assertEqual(len(revoked_workers), 1, outcomes)
        self.assertEqual(len(executed), self.n_workers - 1, outcomes)
        values = sorted(value for _, value in executed)
        self.assertEqual(len(set(values)), self.n_workers - 1, values)
        # Every executed task reports the argument it carried, which matches
        # one of the enqueued tasks exactly.
        for value in values:
            self.assertEqual(value, (value[0], value[0]))
        # The single skipped task is the revoked worker's dequeued task.
        skipped_values = [value for idx, value, fired in outcomes if fired]
        self.assertEqual(skipped_values, [None])

        # Marker was consumed: subsequent tasks of this class are not revoked.
        verifier = self.make_worker_huey()
        self.assertFalse(verifier.is_revoked(
            verifier._registry.string_to_task(TASK_NAME)))

    def test_no_revoke_all_tasks_execute(self):
        # Control group: without a marker every worker executes exactly once.
        outcomes = self.run_race(revoke=False)
        self.assertEqual(len(outcomes), self.n_workers)
        values = []
        for idx, value, fired in outcomes:
            self.assertFalse(fired)
            values.append(value)
        # Each distinct enqueued task ran exactly once despite races.
        self.assertEqual(sorted(values), [(i, i) for i in
                                          range(self.n_workers)])

    def test_delete_if_value_primitive(self):
        storage = self.make_storage()
        storage.put_data(b'k', b'v1')
        # A missing key or a mismatched value deletes nothing.
        self.assertFalse(storage.delete_if_value(b'kx', b'v'))
        self.assertFalse(storage.delete_if_value(b'k', b'other'))
        self.assertEqual(storage.peek_data(b'k'), b'v1')
        # An exact match deletes exactly once.
        self.assertTrue(storage.delete_if_value(b'k', b'v1'))
        self.assertEqual(storage.peek_data(b'k'), EmptyData)
        self.assertFalse(storage.delete_if_value(b'k', b'v1'))

    def make_task_huey(self):
        return self.make_worker_huey()

    def test_crash_after_dequeue_does_not_block_following_tasks(self):
        # Simulate a worker crash: the task is dequeued, but before anything
        # runs the worker dies and the task is never executed.
        huey = self.make_task_huey()
        task_cls = huey._registry.string_to_task(TASK_NAME)
        huey.revoke_all(task_cls, revoke_once=True)
        t1 = task_cls((0, 1))
        huey.storage.enqueue(huey.serializer.serialize(
            huey._registry.create_message(t1)))
        self.assertIsNotNone(huey.dequeue())  # "Crash" -- task lost.

        # A replacement task (e.g. re-enqueued after the crash) consumes the
        # pending revocation and is skipped exactly once.
        t2 = task_cls((0, 2))
        huey.storage.enqueue(huey.serializer.serialize(
            huey._registry.create_message(t2)))
        self.assertIsNone(huey.execute(huey.dequeue()))

        # The marker is gone; the following task runs normally.
        t3 = task_cls((0, 3))
        huey.storage.enqueue(huey.serializer.serialize(
            huey._registry.create_message(t3)))
        self.assertEqual(huey.execute(huey.dequeue()), (0, 3))

    def test_failure_with_retries_does_not_reconsume_marker(self):
        huey = self.make_task_huey()
        def unique_flaky(worker, n):
            return tracked_flaky_task(worker, n)

        wrapper = huey.task(retries=1)(unique_flaky)
        task_cls = wrapper.task_class
        FLAKY_ATTEMPTS[:] = []
        huey.revoke_all(task_cls, revoke_once=True)

        t1 = task_cls((0, 1))
        huey.storage.enqueue(huey.serializer.serialize(
            huey._registry.create_message(t1)))
        # The first occurrence is revoked -- the function never runs and the
        # marker is consumed exactly once.
        self.assertIsNone(huey.execute(huey.dequeue()))
        self.assertEqual(FLAKY_ATTEMPTS, [])
        self.assertFalse(huey.is_revoked(task_cls))

        # A later occurrence with one retry allowed fails its first run and
        # is automatically rescheduled. The retry is a new execution
        # decision, but the revoke marker is already gone, so it is not
        # blocked either on the failed run or on the retry.
        t2 = task_cls((0, 2), retries=1)
        huey.storage.enqueue(huey.serializer.serialize(
            huey._registry.create_message(t2)))
        self.assertIsNone(huey.execute(huey.dequeue()))  # Schedules retry.
        self.assertEqual(FLAKY_ATTEMPTS, [(0, 2)])
        retry_task = huey.dequeue()
        self.assertIsNotNone(retry_task)
        # The consumer swallows the terminal error (it is logged/stored), so
        # execute() returns None; the side-effect proves the body ran.
        self.assertIsNone(huey.execute(retry_task))
        self.assertEqual(FLAKY_ATTEMPTS, [(0, 2), (0, 2)])

    def test_same_task_reenqueued_runs_after_single_skip(self):
        huey = self.make_task_huey()
        task_cls = huey._registry.string_to_task(TASK_NAME)
        huey.revoke_all(task_cls, revoke_once=True)
        t1 = task_cls((0, 7))
        huey.storage.enqueue(huey.serializer.serialize(
            huey._registry.create_message(t1)))

        self.assertIsNone(huey.execute(huey.dequeue()))

        # Re-enqueueing the *same* task object (same id) must now execute.
        huey.storage.enqueue(huey.serializer.serialize(
            huey._registry.create_message(t1)))
        self.assertEqual(huey.execute(huey.dequeue()), (0, 7))

    def test_expired_once_marker_is_swept_without_skipping(self):
        huey = self.make_task_huey()
        task_cls = huey._registry.string_to_task(TASK_NAME)
        cutoff = datetime.datetime(2000, 1, 1)
        huey.revoke_all(task_cls, revoke_until=cutoff)

        t1 = task_cls((0, 1))
        huey.storage.enqueue(huey.serializer.serialize(
            huey._registry.create_message(t1)))
        # Expired marker: the task executes and stale metadata is removed.
        self.assertEqual(huey.execute(huey.dequeue(), timestamp=cutoff),
                         (0, 1))
        self.assertFalse(huey.is_revoked(task_cls, timestamp=cutoff))
        self.assertEqual(huey.storage.result_store_size(), 0)

    def test_fresh_revoke_wins_lost_compare_and_delete_race(self):
        huey = self.make_task_huey()
        task_cls = huey._registry.string_to_task(TASK_NAME)
        key = huey._task_key(task_cls, 'rt')
        huey.revoke_all(task_cls, revoke_once=True)
        original = huey.storage.peek_data(key)
        self.assertNotEqual(original, EmptyData)

        # Another writer replaces the marker with a *different* value after
        # we read it (a persistent revocation replaces the one-shot). The
        # compare-and-delete must fail, so the stale value is not treated as
        # consumed; the decision loop re-reads and honors the new marker.
        huey.revoke_all(task_cls)
        self.assertFalse(huey.storage.delete_if_value(key, original))

        t1 = task_cls((0, 1))
        huey.storage.enqueue(huey.serializer.serialize(
            huey._registry.create_message(t1)))
        self.assertIsNone(huey.execute(huey.dequeue()))

        # The persistent marker remains in effect.
        self.assertTrue(huey.is_revoked(task_cls))
        huey.restore_all(task_cls)

        t2 = task_cls((0, 2))
        huey.storage.enqueue(huey.serializer.serialize(
            huey._registry.create_message(t2)))
        self.assertEqual(huey.execute(huey.dequeue()), (0, 2))

    def test_persistent_revoke_is_never_consumed(self):
        huey = self.make_task_huey()
        task_cls = huey._registry.string_to_task(TASK_NAME)
        huey.revoke_all(task_cls)  # Persistent revocation.

        for n in range(3):
            task = task_cls((0, n))
            huey.storage.enqueue(huey.serializer.serialize(
                huey._registry.create_message(task)))
            self.assertIsNone(huey.execute(huey.dequeue()))
            self.assertTrue(huey.is_revoked(task_cls))

        huey.restore_all(task_cls)
        self.assertFalse(huey.is_revoked(task_cls))

    def test_class_level_consume_path(self):
        # is_revoked(TaskClass, peek=False) must consume the class marker
        # without requiring a task instance.
        huey = self.make_task_huey()
        task_cls = huey._registry.string_to_task(TASK_NAME)
        huey.revoke_all(task_cls, revoke_once=True)
        self.assertTrue(huey.is_revoked(task_cls, peek=False))
        self.assertFalse(huey.is_revoked(task_cls, peek=False))


class TestMemoryRevokeOnce(RevokeOnceConcurrencyTests, unittest.TestCase):
    storage_name = 'huey-test-revoke-once-memory'

    def create_shared_storage(self):
        self._memory_huey = MemoryHuey(name=self.storage_name, utc=False,
                                       results=False, store_errors=False)
        return self._memory_huey.storage


class TestSqliteRevokeOnce(RevokeOnceConcurrencyTests, unittest.TestCase):
    storage_name = 'huey-test-revoke-once-sqlite'

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='huey-revoke-once-')
        self.db_path = os.path.join(self.tmpdir, 'storage.db')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def create_shared_storage(self):
        # The first instance creates the schema; later instances open their
        # own connections to the same file.
        return SqliteHuey(name=self.storage_name, filename=self.db_path,
                          timeout=10).storage

    def make_storage(self):
        return SqliteHuey(name=self.storage_name, filename=self.db_path,
                          timeout=10).storage

    def test_revoke_once_race_across_processes(self):
        # Genuine multi-process race against the same SQLite file. Each
        # process dequeues and executes its own task; exactly one process
        # may consume the shared revoke_once marker.
        producer = SqliteHuey(name=self.storage_name,
                              filename=self.db_path, timeout=10,
                              utc=False, results=False)
        producer.task()(shared_task)
        task_cls = producer._registry.string_to_task(TASK_NAME)
        n = 4
        for i in range(n):
            producer.storage.enqueue(
                producer.serializer.serialize(
                    producer._registry.create_message(task_cls((i, i)))))
        producer.revoke_all(task_cls, revoke_once=True)

        ctx = multiprocessing.get_context('spawn')
        queue = ctx.Queue()
        barrier = ctx.Barrier(n)
        args = (self.storage_name, self.db_path, barrier, queue)
        procs = [ctx.Process(target=_sqlite_race_worker, args=(i,) + args)
                 for i in range(n)]
        for proc in procs:
            proc.start()
        outcomes = [queue.get(timeout=60) for _ in range(n)]
        for proc in procs:
            proc.join(timeout=30)
            self.assertEqual(proc.exitcode, 0)

        outcomes.sort()
        revoked = [i for i, value, fired in outcomes if fired]
        self.assertEqual(len(revoked), 1, outcomes)
        executed = sorted(value for _, value, fired in outcomes if not fired)
        self.assertEqual(len(executed), n - 1, outcomes)
        self.assertEqual(len(set(executed)), n - 1, executed)
        for value in executed:
            self.assertEqual(value, (value[0], value[0]))
        self.assertEqual([value for _, value, fired in outcomes if fired],
                         [None])


@unittest.skipUnless(redis_available(), 'requires a running redis server')
class TestRedisRevokeOnce(RevokeOnceConcurrencyTests, unittest.TestCase):
    storage_name = 'hueytestrevokeonce'

    def create_shared_storage(self):
        return RedisHuey(name=self.storage_name, utc=False, results=False,
                         store_errors=False, blocking=False).storage

    def make_storage(self):
        return self.create_shared_storage()

    def setUp(self):
        self.make_storage().flush_all()

    def tearDown(self):
        self.make_storage().flush_all()


if __name__ == '__main__':
    unittest.main()
