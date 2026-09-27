import calendar
import datetime
import json
import os
import shutil
import threading
import time
import unittest

try:
    from redis import Redis
except ImportError:
    Redis = None

from huey import signals as S
from huey.api import MemoryHuey
from huey.api import SqliteHuey
from huey.exceptions import ConfigurationError
from huey.storage import FileStorage
from huey.storage import MemoryStorage
from huey.storage import RedisStorage
from huey.storage import SqliteStorage
from huey.tests.base import BaseTestCase


def redis_available():
    if Redis is None:
        return False
    try:
        Redis().ping()
    except Exception:
        return False
    return True


requires_redis = unittest.skipUnless(redis_available(),
                                     'requires redis server')

# Virtual-time epoch. Chosen so it is aligned to 60-second windows (the
# unix timestamp is a multiple of 60), which makes window-boundary math
# easy to follow in the tests.
EPOCH = datetime.datetime(2030, 1, 1)
EPOCH_UNIX = calendar.timegm(EPOCH.utctimetuple())
assert EPOCH_UNIX % 60 == 0


def ts(offset):
    # Virtual timestamp 'offset' seconds after the epoch.
    return EPOCH + datetime.timedelta(seconds=offset)


class RateLimitTestCase(BaseTestCase):
    def get_huey(self):
        return MemoryHuey(utc=True, results=False)

    def run_worker_loop(self, huey, timestamp):
        # Mirrors consumer.Worker.loop(): dequeue and execute until the
        # queue is drained, using the given (virtual) timestamp.
        while True:
            task = huey.dequeue()
            if task is None:
                break
            huey.execute(task, timestamp)

    def run_scheduler_loop(self, huey, timestamp):
        # Mirrors consumer.Scheduler.loop(): move due tasks from the
        # schedule back onto the queue.
        for task in huey.read_schedule(timestamp):
            huey.enqueue(task)


class TestRateLimitConfig(RateLimitTestCase):
    def test_invalid_config_rejected(self):
        self.assertRaises(ConfigurationError,
                          self.huey.task, rate_limit=0)
        self.assertRaises(ConfigurationError,
                          self.huey.task, rate_limit=-2)
        self.assertRaises(ConfigurationError,
                          self.huey.task, rate_limit=1.5)
        self.assertRaises(ConfigurationError,
                          self.huey.task, rate_limit=1, rate_limit_window=0)
        self.assertRaises(ConfigurationError,
                          self.huey.task, rate_limit=1, rate_limit_window=-5)
        self.assertRaises(ConfigurationError,
                          self.huey.task, rate_limit=1, rate_limit_key=123)

    def test_valid_config_accepted(self):
        @self.huey.task(rate_limit=5, rate_limit_window=30,
                        rate_limit_key='k')
        def t1():
            pass
        self.assertEqual(t1.task_class.default_rate_limit, 5)
        self.assertEqual(t1.task_class.default_rate_limit_window, 30)
        self.assertEqual(t1.task_class.default_rate_limit_key, 'k')

        @self.huey.task(rate_limit=1,
                        rate_limit_key=lambda task: task.args[0])
        def t2(x):
            pass
        self.assertTrue(callable(t2.task_class.default_rate_limit_key))

    def test_unlimited_tasks_do_not_touch_rate_limiter(self):
        # Ordinary tasks are fully backwards-compatible: the storage
        # rate-limit hook is never invoked for them.
        calls = []
        storage = self.huey.storage
        orig = storage.acquire_rate_limit
        def spy(*args, **kwargs):
            calls.append(args)
            return orig(*args, **kwargs)
        storage.acquire_rate_limit = spy

        executed = []

        @self.huey.task()
        def plain(n):
            executed.append(n)

        for i in range(50):
            plain(i)
        self.run_worker_loop(self.huey, ts(1))
        self.assertEqual(len(executed), 50)
        self.assertEqual(calls, [])
        self.assertEqual(self.huey.scheduled_count(), 0)


class TestWindowBoundary(RateLimitTestCase):
    def test_window_boundary_and_recovery(self):
        executed = []

        @self.huey.task(rate_limit=2, rate_limit_window=60)
        def limited(n):
            executed.append(n)

        for i in range(3):
            limited(i)

        # Window 0: two tasks run, the third is deferred.
        self.run_worker_loop(self.huey, ts(10))
        self.assertEqual(sorted(executed), [0, 1])
        self.assertEqual(self.huey.scheduled_count(), 1)

        # The deferred task is scheduled exactly at the window boundary,
        # i.e. it recovers automatically when the window expires.
        scheduled, = self.huey.scheduled()
        self.assertEqual(scheduled.eta, ts(60))

        # Not yet due just before the boundary.
        self.run_scheduler_loop(self.huey, ts(59.999))
        self.assertEqual(self.huey.pending_count(), 0)

        # At the boundary the task becomes due and runs in the new window.
        self.run_scheduler_loop(self.huey, ts(60))
        self.assertEqual(self.huey.pending_count(), 1)
        self.run_worker_loop(self.huey, ts(60))
        self.assertEqual(sorted(executed), [0, 1, 2])
        self.assertEqual(self.huey.scheduled_count(), 0)

    def test_retry_eta_is_monotonic(self):
        etas = []
        executed = []

        @self.huey.signal(S.SIGNAL_RATE_LIMITED)
        def on_rate_limited(signal, task, *args, **kwargs):
            etas.append(task.eta)

        @self.huey.task(rate_limit=1, rate_limit_window=60)
        def hog(n):
            executed.append(n)

        hog('w0')
        hog('deferred')
        self.run_worker_loop(self.huey, ts(10))
        self.assertEqual(executed, ['w0'])
        self.assertEqual(etas, [ts(60)])

        # Another task arrives and takes the window-1 slot first.
        hog('w1')
        self.run_worker_loop(self.huey, ts(60))
        self.assertEqual(executed, ['w0', 'w1'])

        # The deferred task becomes due, but the slot is already gone, so
        # it is deferred again -- to a later (never earlier) eta.
        self.run_scheduler_loop(self.huey, ts(60.5))
        self.run_worker_loop(self.huey, ts(60.5))
        self.assertEqual(executed, ['w0', 'w1'])
        self.assertEqual(etas, [ts(60), ts(120)])
        self.assertTrue(all(b >= a for a, b in zip(etas, etas[1:])))

        # It eventually runs in window 2.
        self.run_scheduler_loop(self.huey, ts(120))
        self.run_worker_loop(self.huey, ts(120))
        self.assertEqual(executed, ['w0', 'w1', 'deferred'])

    def test_deferral_does_not_consume_retries(self):
        @self.huey.task(rate_limit=1, rate_limit_window=60, retries=3)
        def limited():
            pass

        limited()
        limited()
        self.run_worker_loop(self.huey, ts(5))
        scheduled, = self.huey.scheduled()
        # Rate-limit deferral is not a failure: retry budget untouched.
        self.assertEqual(scheduled.retries, 3)


class TestRateLimitKeys(RateLimitTestCase):
    def test_default_key_is_per_task_class(self):
        executed = []

        @self.huey.task(rate_limit=1, rate_limit_window=60)
        def task_a():
            executed.append('a')

        @self.huey.task(rate_limit=1, rate_limit_window=60)
        def task_b():
            executed.append('b')

        task_a()
        task_b()
        self.run_worker_loop(self.huey, ts(1))
        # Each task class has its own independent window.
        self.assertEqual(sorted(executed), ['a', 'b'])

    def test_callable_key_uses_task_arguments(self):
        executed = []

        @self.huey.task(rate_limit=1, rate_limit_window=60,
                        rate_limit_key=lambda task: 'user:%s' % task.args[0])
        def sync_user(user):
            executed.append(user)

        sync_user('u1')
        sync_user('u1')
        sync_user('u2')
        self.run_worker_loop(self.huey, ts(1))
        # u1 and u2 have independent limits; the second u1 task defers.
        self.assertEqual(sorted(executed), ['u1', 'u2'])
        self.assertEqual(self.huey.scheduled_count(), 1)

    def test_static_key_shared_across_task_classes(self):
        executed = []

        @self.huey.task(rate_limit=1, rate_limit_window=60,
                        rate_limit_key='shared-resource')
        def task_a():
            executed.append('a')

        @self.huey.task(rate_limit=1, rate_limit_window=60,
                        rate_limit_key='shared-resource')
        def task_b():
            executed.append('b')

        task_a()
        task_b()
        self.run_worker_loop(self.huey, ts(1))
        # Both classes draw from the same budget.
        self.assertEqual(len(executed), 1)
        self.assertEqual(self.huey.scheduled_count(), 1)
class TestConcurrentAcquire(RateLimitTestCase):
    def hammer(self, storage, key, limit, window, now, nthreads):
        # All threads are released simultaneously by the barrier and
        # race to acquire slots in the same window.
        barrier = threading.Barrier(nthreads)
        results = []
        lock = threading.Lock()

        def worker():
            barrier.wait(timeout=10)
            acquired, _ = storage.acquire_rate_limit(key, limit, window, now)
            with lock:
                results.append(acquired)

        threads = [threading.Thread(target=worker) for _ in range(nthreads)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertEqual(len(results), nthreads)
        return results

    def test_memory_storage_no_overissue(self):
        storage = MemoryStorage('test')
        results = self.hammer(storage, 'k', 10, 60, 1000.0, 50)
        self.assertEqual(sum(results), 10)

    def test_sqlite_storage_no_overissue(self):
        filename = '/tmp/huey-rl-concurrency.db'
        storage = SqliteStorage('test', filename=filename)
        try:
            results = self.hammer(storage, 'k', 10, 60, 1000.0, 50)
            self.assertEqual(sum(results), 10)
        finally:
            storage.close()
            for suffix in ('', '-wal', '-shm'):
                try:
                    os.unlink(filename + suffix)
                except OSError:
                    pass

    def test_multiple_consumers_no_overissue(self):
        # 8 consumer threads drain a queue of 20 tasks with a limit of 5
        # per window: exactly 5 execute, the rest are deferred.
        executed = []
        lock = threading.Lock()

        @self.huey.task(rate_limit=5, rate_limit_window=60)
        def limited(n):
            with lock:
                executed.append(n)

        for i in range(20):
            limited(i)

        barrier = threading.Barrier(8)

        def consumer():
            barrier.wait(timeout=10)
            while True:
                task = self.huey.dequeue()
                if task is None:
                    break
                self.huey.execute(task, ts(10))

        threads = [threading.Thread(target=consumer) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)

        self.assertEqual(len(executed), 5)
        self.assertEqual(self.huey.scheduled_count(), 15)


class TestFailureRetry(RateLimitTestCase):
    def test_failed_task_retry_respects_window(self):
        attempts = []

        @self.huey.task(rate_limit=1, rate_limit_window=60, retries=1)
        def flaky():
            attempts.append(1)
            if len(attempts) == 1:
                raise Exception('boom')

        flaky()

        # First attempt runs and fails; the task is requeued immediately
        # (retry_delay=0).
        self.run_worker_loop(self.huey, ts(5))
        self.assertEqual(len(attempts), 1)
        # The requeued retry is picked up in the same worker pass, but
        # cannot run in the same window: the failed attempt consumed the
        # only slot. It is deferred to the next window instead of
        # over-issuing.
        scheduled, = self.huey.scheduled()
        self.assertEqual(scheduled.eta, ts(60))
        self.assertEqual(scheduled.retries, 0)

        # In the next window the retry runs and succeeds.
        self.run_scheduler_loop(self.huey, ts(60))
        self.run_worker_loop(self.huey, ts(60))
        self.assertEqual(len(attempts), 2)
        self.assertEqual(self.huey.scheduled_count(), 0)


class SqliteRateLimitTestCase(RateLimitTestCase):
    filename = '/tmp/huey-rl-test.db'

    def get_huey(self):
        return SqliteHuey(utc=True, results=False, filename=self.filename)

    def setUp(self):
        self._remove_db()
        super(SqliteRateLimitTestCase, self).setUp()

    def tearDown(self):
        self.huey.storage.close()
        self._remove_db()
        super(SqliteRateLimitTestCase, self).tearDown()

    def _remove_db(self):
        for suffix in ('', '-wal', '-shm'):
            try:
                os.unlink(self.filename + suffix)
            except OSError:
                pass

    def make_task(self, huey, executed, limit=2):
        @huey.task(rate_limit=limit, rate_limit_window=60)
        def limited(n):
            executed.append(n)
        return limited


class TestWorkerRestart(SqliteRateLimitTestCase):
    def test_counts_survive_worker_restart(self):
        executed = []
        limited = self.make_task(self.huey, executed)
        for i in range(3):
            limited(i)

        self.run_worker_loop(self.huey, ts(10))
        self.assertEqual(sorted(executed), [0, 1])
        self.assertEqual(self.huey.scheduled_count(), 1)

        # Simulate a worker restart: a brand-new Huey + storage instance
        # over the same database file. In-flight window counts persist,
        # so the restarted worker keeps enforcing the current window.
        self.huey.storage.close()
        huey2 = SqliteHuey(utc=True, results=False, filename=self.filename)
        limited2 = self.make_task(huey2, executed)
        try:
            self.assertEqual(huey2.scheduled_count(), 1)
            limited2(3)
            self.run_worker_loop(huey2, ts(20))
            # Still no over-issue after the restart.
            self.assertEqual(sorted(executed), [0, 1])
            self.assertEqual(huey2.scheduled_count(), 2)

            # The window expires and both deferred tasks recover.
            self.run_scheduler_loop(huey2, ts(60))
            self.run_worker_loop(huey2, ts(60))
            self.assertEqual(sorted(executed), [0, 1, 2, 3])
        finally:
            huey2.storage.close()

    def test_two_consumers_share_window(self):
        executed = []
        t1 = self.make_task(self.huey, executed, limit=4)
        huey2 = SqliteHuey(utc=True, results=False, filename=self.filename)
        self.make_task(huey2, executed, limit=4)

        for i in range(12):
            t1(i)

        barrier = threading.Barrier(4)

        def consume(huey):
            barrier.wait(timeout=10)
            while True:
                task = huey.dequeue()
                if task is None:
                    break
                huey.execute(task, ts(10))

        try:
            threads = [threading.Thread(target=consume, args=(huey,))
                       for huey in (self.huey, self.huey, huey2, huey2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(10)

            # Two independent consumer processes share one window budget.
            self.assertEqual(len(executed), 4)
            self.assertEqual(self.huey.scheduled_count(), 8)
        finally:
            huey2.storage.close()
class TestHighCardinalityKeys(RateLimitTestCase):
    def test_memory_prune_expired_keys(self):
        storage = MemoryStorage('test')
        for i in range(1000):
            acquired, _ = storage.acquire_rate_limit(
                'user:%d' % i, 1, 60, now=1000.0)
            self.assertTrue(acquired)
        self.assertEqual(len(storage._rate_limits), 1000)

        # Window expires at t=1020: nothing is pruned before then.
        self.assertEqual(storage.prune_rate_limits(now=1019.9), 0)
        self.assertEqual(len(storage._rate_limits), 1000)
        self.assertEqual(storage.prune_rate_limits(now=1020.0), 1000)
        self.assertEqual(storage._rate_limits, {})

    def test_memory_rollover_replaces_entry(self):
        storage = MemoryStorage('test')
        storage.acquire_rate_limit('k', 1, 60, now=1000.0)
        # Same key in a later window reuses a single entry.
        acquired, _ = storage.acquire_rate_limit('k', 1, 60, now=2000.0)
        self.assertTrue(acquired)
        self.assertEqual(len(storage._rate_limits), 1)

    def test_sqlite_prune_expired_keys(self):
        storage = SqliteStorage('test', filename='/tmp/huey-rl-prune.db')
        self.addCleanup(self._cleanup_prune_db)
        for i in range(1000):
            acquired, _ = storage.acquire_rate_limit(
                'user:%d' % i, 1, 60, now=1000.0)
            self.assertTrue(acquired)
        count = storage.sql('select count(*) from ratelimit',
                            results=True)[0][0]
        self.assertEqual(count, 1000)

        self.assertEqual(storage.prune_rate_limits(now=1019.9), 0)
        self.assertEqual(storage.prune_rate_limits(now=1020.0), 1000)
        count = storage.sql('select count(*) from ratelimit',
                            results=True)[0][0]
        self.assertEqual(count, 0)

    def _cleanup_prune_db(self):
        for suffix in ('', '-wal', '-shm'):
            try:
                os.unlink('/tmp/huey-rl-prune.db' + suffix)
            except OSError:
                pass


class TestStorageCapabilities(RateLimitTestCase):
    def test_cross_process_flags(self):
        # The in-memory storage must not masquerade as being consistent
        # across processes; the shared backends may.
        self.assertFalse(MemoryStorage.cross_process)
        self.assertFalse(MemoryStorage('x').cross_process)
        self.assertTrue(SqliteStorage.cross_process)
        self.assertTrue(RedisStorage.cross_process)

    def test_memory_storage_is_process_local(self):
        # Two in-process instances do not share counters, demonstrating
        # why the memory backend cannot provide cross-process limits.
        first = MemoryStorage('same-name')
        second = MemoryStorage('same-name')
        acquired, _ = first.acquire_rate_limit('k', 1, 60, now=1000.0)
        self.assertTrue(acquired)
        acquired, _ = second.acquire_rate_limit('k', 1, 60, now=1000.0)
        self.assertTrue(acquired)  # Not shared: would be False if it were.

    def test_unsupported_storage_raises(self):
        path = '/tmp/huey-rl-filestorage'
        shutil.rmtree(path, ignore_errors=True)
        storage = FileStorage('test', path)
        try:
            self.assertRaises(NotImplementedError,
                              storage.acquire_rate_limit, 'k', 1, 60, 1000.0)
        finally:
            shutil.rmtree(path, ignore_errors=True)

    def test_flush_all_clears_rate_limits(self):
        storage = self.huey.storage
        storage.acquire_rate_limit('k', 1, 60, now=1000.0)
        storage.flush_all()
        # After flushing, the same key acquires again in the same window.
        acquired, _ = storage.acquire_rate_limit('k', 1, 60, now=1000.0)
        self.assertTrue(acquired)


class TestObservables(RateLimitTestCase):
    def test_no_overissue_and_monotonic_retry_eta(self):
        # Scenario: 10 tasks, limit 3 per 60s window, one task fails once
        # and is retried with a delay. Record per-window execution counts
        # and every (re)schedule eta, then verify the two invariants:
        #   1. no window ever executes more than 'limit' tasks
        #   2. for each task, successive etas are monotonically
        #      non-decreasing
        clock = {'now': None}
        executed = []
        task_etas = {}
        lock = threading.Lock()
        attempts = {}

        # Drive Huey's internal clock with the same virtual time, so
        # the failure-retry path (which reads the clock itself) stays
        # consistent with the timestamps passed to execute().
        self.huey._get_timestamp = lambda: ts(clock['now'] or 0)

        @self.huey.signal(S.SIGNAL_SCHEDULED)
        def record_eta(signal, task, *args, **kwargs):
            with lock:
                task_etas.setdefault(task.id, []).append(task.eta)

        @self.huey.task(rate_limit=3, rate_limit_window=60, retries=2,
                        retry_delay=30)
        def job(n):
            with lock:
                executed.append((n, clock['now']))
                attempts[n] = attempts.get(n, 0) + 1
            if n == 7 and attempts[7] == 1:
                raise Exception('transient failure')

        for i in range(10):
            job(i)

        for window in range(6):
            offset = 60 * window + 1
            clock['now'] = offset
            self.run_scheduler_loop(self.huey, ts(offset))
            self.run_worker_loop(self.huey, ts(offset))

        # All 10 tasks ran (task 7 needed one extra attempt).
        self.assertEqual(sorted(n for n, _ in executed),
                         [0, 1, 2, 3, 4, 5, 6, 7, 7, 8, 9])

        # Invariant 1: no window over-issues.
        per_window = {}
        for _, offset in executed:
            per_window[offset // 60] = per_window.get(offset // 60, 0) + 1
        overissued = {w: c for w, c in per_window.items() if c > 3}
        self.assertEqual(overissued, {})

        # Invariant 2: retry etas are monotonic per task.
        monotonic = all(
            all(b >= a for a, b in zip(etas, etas[1:]))
            for etas in task_etas.values())
        self.assertTrue(monotonic)

        # Observable record of the run.
        summary = {
            'limit_per_window': 3,
            'executions_per_window': {str(w): c for w, c in
                                      sorted(per_window.items())},
            'overissued': overissued,
            'retry_eta_monotonic': monotonic,
            'retry_etas': {tid: [eta.isoformat() for eta in etas]
                           for tid, etas in sorted(task_etas.items())},
        }
        print('\nrate-limit observables: ' + json.dumps(
            summary, indent=2, sort_keys=True))


class TestConsumerIntegration(RateLimitTestCase):
    def test_consumer_enforces_window_and_recovers(self):
        # End-to-end with a real consumer (threads, real time): a short
        # 1s window, limit 2, 4 tasks. All tasks run, and no aligned
        # 1-second window ever contains more than 2 executions.
        executed = []
        lock = threading.Lock()
        window = 1.0

        @self.huey.task(rate_limit=2, rate_limit_window=window)
        def limited(n):
            with lock:
                executed.append((n, time.time()))

        for i in range(4):
            limited(i)

        with self.consumer_context(workers=2, scheduler_interval=1):
            deadline = time.time() + 15
            while len(executed) < 4 and time.time() < deadline:
                time.sleep(0.05)

        self.assertEqual(len(executed), 4)
        per_window = {}
        for _, ts_executed in executed:
            wid = int(ts_executed // window)
            per_window[wid] = per_window.get(wid, 0) + 1
        self.assertTrue(all(c <= 2 for c in per_window.values()),
                        per_window)


@requires_redis
class TestRedisRateLimit(unittest.TestCase):
    def setUp(self):
        self.storage = RedisStorage('rltest')
        self.storage.flush_rate_limits()

    def tearDown(self):
        self.storage.flush_rate_limits()

    def test_window_acquire_and_rollover(self):
        storage = self.storage
        self.assertEqual(
            storage.acquire_rate_limit('k', 2, 60, now=1000.0), (True, 20.0))
        self.assertEqual(
            storage.acquire_rate_limit('k', 2, 60, now=1000.0), (True, 20.0))
        acquired, retry_after = storage.acquire_rate_limit(
            'k', 2, 60, now=1000.0)
        self.assertFalse(acquired)
        self.assertEqual(retry_after, 20.0)

        # The next window opens up again automatically.
        acquired, _ = storage.acquire_rate_limit('k', 2, 60, now=1020.0)
        self.assertTrue(acquired)

    def test_keys_expire_via_ttl(self):
        storage = self.storage
        storage.acquire_rate_limit('ephemeral', 1, 0.2, now=time.time())
        keys = list(storage.conn.scan_iter(
            match=storage.rate_limit_prefix + '*'))
        self.assertEqual(len(keys), 1)
        ttl_ms = storage.conn.pttl(keys[0])
        self.assertTrue(0 < ttl_ms <= 400)

        # Redis itself reaps the idle key (high-cardinality cleanup).
        time.sleep(0.5)
        keys = list(storage.conn.scan_iter(
            match=storage.rate_limit_prefix + '*'))
        self.assertEqual(keys, [])

    def test_flush_rate_limits(self):
        storage = self.storage
        storage.acquire_rate_limit('a', 1, 60, now=1000.0)
        storage.acquire_rate_limit('b', 1, 60, now=1000.0)
        storage.flush_rate_limits()
        keys = list(storage.conn.scan_iter(
            match=storage.rate_limit_prefix + '*'))
        self.assertEqual(keys, [])
