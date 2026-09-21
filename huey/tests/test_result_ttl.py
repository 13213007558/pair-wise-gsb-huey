import os
import shutil
import tempfile
import unittest

from huey.api import BlackHoleHuey
from huey.api import FileHuey
from huey.api import MemoryHuey
from huey.api import SqliteHuey
from huey.api import chord
from huey.constants import EmptyData
from huey.exceptions import ConfigurationError
from huey.exceptions import ResultTimeout
from huey.exceptions import TaskException
from huey.tests.base import BaseTestCase
from huey.utils import Error


class Clock(object):
    """Controllable clock used to verify exact expiration boundaries."""
    def __init__(self, now=1000000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class TestResultTtlConfig(BaseTestCase):
    def test_default_is_none_and_never_expires(self):
        huey = MemoryHuey(utc=False)
        self.assertTrue(huey.result_ttl is None)
        clock = Clock()
        huey.storage._now = clock
        huey.put_result('k1', 'v1')
        clock.advance(10 ** 9)
        self.assertEqual(huey.get('k1', peek=True), 'v1')
        self.assertEqual(huey.result_count(), 1)

    def test_zero_ttl_expires_immediately(self):
        # A TTL of zero means results are not retained: they are expired as
        # soon as they are written.
        huey = MemoryHuey(result_ttl=0, utc=False)

        @huey.task()
        def add(a, b):
            return a + b

        res = add(1, 2)
        self.execute_next_on(huey)
        self.assertTrue(huey.get(res.id, peek=True) is None)
        self.assertFalse(res.is_ready())
        self.assertEqual(huey.result_count(), 0)

    def execute_next_on(self, huey):
        task = huey.dequeue()
        self.assertTrue(task is not None)
        return huey.execute(task)

    def test_negative_ttl_rejected(self):
        self.assertRaises(ConfigurationError, MemoryHuey, result_ttl=-1)
        self.assertRaises(ConfigurationError, MemoryHuey, result_ttl=-0.5)

    def test_invalid_ttl_rejected(self):
        self.assertRaises(ConfigurationError, MemoryHuey, result_ttl='10')
        self.assertRaises(ConfigurationError, MemoryHuey, result_ttl=True)

    def test_unsupported_storage_rejected(self):
        # Storages that do not implement result expiration fail clearly when
        # the feature is enabled.
        self.assertRaises(ConfigurationError, BlackHoleHuey, result_ttl=10)

        path = tempfile.mkdtemp()
        try:
            self.assertRaises(ConfigurationError, FileHuey, path=path,
                              result_ttl=10)
        finally:
            shutil.rmtree(path, ignore_errors=True)

    def test_unsupported_storage_methods_raise(self):
        path = tempfile.mkdtemp()
        try:
            storage = FileHuey(path=path).storage
            self.assertRaises(NotImplementedError, storage.put_data,
                              b'k', b'v', ttl=1)
            self.assertRaises(NotImplementedError, storage.expire_results)
        finally:
            shutil.rmtree(path, ignore_errors=True)


class ResultTtlTests(object):
    """Shared test-cases for storages supporting result TTLs."""
    def setUp(self):
        super(ResultTtlTests, self).setUp()
        self.clock = Clock()
        self.huey.storage._now = self.clock

    def get_task(self, fn, **kwargs):
        return self.huey.task(**kwargs)(fn)

    def test_exact_expiry_boundary(self):
        # Written at t=1000000 with ttl=10 -> expires at t=1000010.
        self.huey.put_result('k1', 'v1')

        # Just before the boundary the result is still available.
        self.clock.advance(10 - 1e-6)
        self.assertEqual(self.huey.get('k1', peek=True), 'v1')
        self.assertTrue(self.huey.storage.has_data_for_key('k1'))

        # At exactly the expiration timestamp it is unavailable.
        self.clock.advance(1e-6)
        self.assertTrue(self.huey.get('k1', peek=True) is None)
        self.assertFalse(self.huey.storage.has_data_for_key('k1'))
        self.assertEqual(self.huey.storage.peek_data('k1'), EmptyData)
        self.assertEqual(self.huey.storage.pop_data('k1'), EmptyData)
        self.assertEqual(self.huey.storage.peek_many(['k1']), {})

    def test_reads_do_not_renew_ttl(self):
        # Reading a result (even repeatedly) must not extend its lifetime.
        self.huey.put_result('k1', 'v1')
        for _ in range(9):
            self.clock.advance(1)
            self.assertEqual(self.huey.get('k1', peek=True), 'v1')
            self.assertTrue(self.huey.storage.has_data_for_key('k1'))

        self.clock.advance(1)  # t == write-time + ttl.
        self.assertTrue(self.huey.get('k1', peek=True) is None)

    def test_preserve_reads(self):
        add = self.get_task(lambda a, b: a + b)
        res = add(1, 2)
        self.execute_next()

        # preserve=True performs a non-destructive read.
        self.assertEqual(res.get(preserve=True), 3)
        res.reset()
        self.assertEqual(res.get(preserve=True), 3)
        self.assertTrue(self.huey.storage.has_data_for_key(res.id))

        # A destructive read removes the result.
        self.assertEqual(self.huey.get(res.id), 3)
        self.assertFalse(self.huey.storage.has_data_for_key(res.id))

        # After the TTL elapses, preserve reads see nothing.
        res2 = add(3, 4)
        self.execute_next()
        self.clock.advance(11)
        self.assertTrue(res2.get(preserve=True) is None)
        self.assertFalse(res2.is_ready())

    def test_blocking_wait_treats_expired_as_unavailable(self):
        add = self.get_task(lambda a, b: a + b)
        res = add(1, 2)
        self.execute_next()

        # Sanity check: blocking read works before expiration.
        self.assertEqual(res.get(blocking=True, timeout=1), 3)

        # Once expired, a blocking wait does not see the stale result and
        # times out, even though a row may still physically exist.
        res2 = add(3, 4)
        self.execute_next()
        self.clock.advance(11)
        self.assertFalse(self.huey.storage.wait_result(res2.id, timeout=0.05))
        self.assertRaises(ResultTimeout, res2.get, blocking=True, timeout=0.05)

    def test_none_result(self):
        self.huey.store_none = True
        none_task = self.get_task(lambda: None)
        res = none_task()
        self.execute_next()

        # A stored None is distinguishable from a missing result.
        self.assertTrue(res.is_ready())
        self.assertTrue(res.get() is None)

        # After expiration the None result is gone.
        res.reset()
        self.clock.advance(11)
        self.assertFalse(res.is_ready())
        self.assertTrue(res.get() is None)

    def test_error_result_and_retry(self):
        state = {'calls': 0}

        def fail():
            state['calls'] += 1
            raise Exception('boom')

        fail_task = self.get_task(fail, retries=1)
        res = fail_task()

        # First execution fails and is requeued; the intermediate error is
        # stored with the same TTL as any other result.
        self.execute_next()
        self.assertEqual(state['calls'], 1)
        self.assertRaises(TaskException, res.get)

        # Retry fails as well; the error result is still stored.
        res.reset()
        self.execute_next()
        self.assertEqual(state['calls'], 2)
        self.assertRaises(TaskException, res.get)

        # After the TTL elapses the error result is gone.
        res.reset()
        self.clock.advance(11)
        self.assertFalse(res.is_ready())
        self.assertTrue(self.huey.get(res.id, peek=True) is None)

    def test_expire_results_cleanup(self):
        for i in range(5):
            self.huey.put_result('k%d' % i, i)
        self.huey.put('meta', 'x')  # Metadata is not subject to the TTL.
        self.huey.storage.put_data(b'legacy', b'v')  # No TTL -> never expires.

        self.clock.advance(11)

        # Bounded cleanup removes at most "limit" expired results.
        self.assertEqual(self.huey.expire_results(limit=2), 2)
        self.assertEqual(self.huey.expire_results(), 3)
        self.assertEqual(self.huey.expire_results(), 0)

        # Metadata and legacy (TTL-less) values are untouched.
        self.assertEqual(self.huey.get('meta', peek=True), 'x')
        self.assertEqual(self.huey.storage.peek_data(b'legacy'), b'v')
        self.assertEqual(self.huey.result_count(), 2)

    def test_result_store_excludes_expired(self):
        self.huey.put_result('k1', 'v1')
        self.huey.put_result('k2', 'v2')
        self.clock.advance(5)
        self.huey.put_result('k3', 'v3')
        self.clock.advance(6)  # k1 and k2 expired, k3 still valid.

        self.assertEqual(self.huey.result_count(), 1)
        self.assertEqual(self.huey.all_results().keys(), set(['k3']))
        self.assertEqual(
            self.huey.storage.peek_many(['k1', 'k2', 'k3']).keys(),
            set(['k3']))

    def test_legacy_data_without_expiry(self):
        # Values written without TTL information (e.g. by older versions of
        # huey) remain readable and never expire.
        self.huey.storage.put_data(b'old', b'legacy-value')
        self.huey.put('old-task', {'a': 1})
        self.clock.advance(10 ** 6)
        self.assertEqual(self.huey.storage.peek_data(b'old'), b'legacy-value')
        self.assertEqual(self.huey.get('old-task', peek=True), {'a': 1})

    def test_revoke_flags_not_expired(self):
        task = self.get_task(lambda: None).s()
        self.huey.revoke(task)
        self.clock.advance(100)
        self.assertTrue(self.huey.is_revoked(task))

    def test_locks_not_expired(self):
        lock = self.huey.lock_task('my-lock')
        self.assertTrue(lock.acquire())
        self.clock.advance(100)
        self.assertTrue(self.huey.is_locked('my-lock'))
        self.assertTrue(lock.is_locked())

    def test_chord_coordination_not_expired(self):
        def prod(n):
            return n + 1

        def agg(ns):
            if any(isinstance(n, Error) for n in ns):
                return -1
            return sum(ns)

        prod_task = self.get_task(prod)
        agg_task = self.get_task(agg)

        c = chord([prod_task.s(i) for i in range(3)], agg_task.s())
        r = self.huey.enqueue(c)
        self.assertEqual([self.execute_next() for _ in range(3)], [1, 2, 3])

        # Advance well past the TTL. Chord coordination data is internal
        # bookkeeping and must survive so the callback fires correctly.
        self.clock.advance(100)
        self.assertEqual(self.execute_next(), 6)
        self.assertEqual(r.get(), 6)


class TestMemoryResultTtl(ResultTtlTests, BaseTestCase):
    def get_huey(self):
        return MemoryHuey(result_ttl=10, utc=False)


class TestSqliteResultTtl(ResultTtlTests, BaseTestCase):
    filename = 'huey_ttl_test.db'

    def get_huey(self):
        return SqliteHuey(filename=self.filename, result_ttl=10, utc=False)

    def tearDown(self):
        self.huey.storage.close()
        for suffix in ('', '-wal', '-shm'):
            if os.path.exists(self.filename + suffix):
                os.unlink(self.filename + suffix)
        super(TestSqliteResultTtl, self).tearDown()

    def make_other(self, clock):
        other = SqliteHuey(filename=self.filename, result_ttl=10, utc=False)
        other.storage._now = clock
        return other

    def test_two_instances_interleaved(self):
        # Two sqlite-backed huey instances sharing one database file, each
        # with its own (controlled) clock, interleaving reads, writes and
        # cleanup.
        clock_b = Clock(self.clock.now)
        huey_b = self.make_other(clock_b)
        try:
            # A writes; B reads it before expiration.
            self.huey.put_result('a1', 'A')  # Expires at t0 + 10.
            self.clock.advance(5)
            clock_b.advance(5)
            self.assertEqual(huey_b.get('a1', peek=True), 'A')

            # B writes; A reads it before expiration.
            huey_b.put_result('b1', 'B')  # Expires at t0 + 15.
            self.assertEqual(self.huey.get('b1', peek=True), 'B')

            # Advance past a1's expiration, but not b1's.
            self.clock.advance(6)
            clock_b.advance(6)  # t0 + 11.
            self.assertTrue(self.huey.get('a1', peek=True) is None)
            self.assertEqual(self.huey.get('b1', peek=True), 'B')

            # B runs cleanup; only a1's expired row is reclaimed.
            self.assertEqual(huey_b.expire_results(), 1)
            self.assertEqual(self.huey.result_count(), 1)
            self.assertEqual(self.huey.get('b1', peek=True), 'B')

            # A writes another result, then everything expires.
            self.huey.put_result('a2', 'A2')  # Expires at t0 + 21.
            self.clock.advance(20)
            clock_b.advance(20)  # t0 + 31.

            # Bounded cleanup from A, then B reclaims the rest.
            self.assertEqual(self.huey.expire_results(limit=1), 1)
            self.assertEqual(huey_b.expire_results(), 1)
            self.assertEqual(self.huey.result_count(), 0)
            self.assertEqual(huey_b.result_count(), 0)
        finally:
            huey_b.storage.close()

    def test_legacy_database_rows(self):
        # Simulate rows written by an older version of huey: plain
        # serialized values with no expiration header.
        raw = self.huey.serializer.serialize({'old': 'data'})
        self.huey.storage.sql(
            'insert into kv (queue, key, value) values (?, ?, ?)',
            (self.huey.name, 'legacy-key', raw), commit=True)

        self.clock.advance(10 ** 6)
        self.assertEqual(self.huey.get('legacy-key', peek=True),
                         {'old': 'data'})
        self.assertTrue(self.huey.storage.has_data_for_key('legacy-key'))
        self.assertEqual(self.huey.expire_results(), 0)
        self.assertEqual(self.huey.get('legacy-key'), {'old': 'data'})


if __name__ == '__main__':
    unittest.main()
