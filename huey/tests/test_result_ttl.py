import os
import sqlite3
import tempfile

from huey.api import Huey
from huey.api import MemoryHuey
from huey.api import SqliteHuey
from huey.api import chord
from huey.constants import EmptyData
from huey.exceptions import ConfigurationError
from huey.exceptions import ResultTimeout
from huey.storage import MemoryStorage
from huey.storage import SqliteStorage
from huey.tests.base import BaseTestCase


class FakeClock(object):
    """Controllable clock shared by one or more storage instances."""

    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def memory_huey(result_ttl=10, clock=None, store_none=False):
    clock = clock or FakeClock()
    storage = MemoryStorage('huey', result_ttl=result_ttl,
                            time_function=clock)
    return clock, Huey('huey', storage_class=lambda *a, **k: storage,
                       store_none=store_none, utc=False)


class MemoryResultTTLStorageTests(BaseTestCase):
    def get_huey(self):
        self.clock, huey = memory_huey()
        return huey

    def test_exact_expiry_boundary(self):
        s = self.huey.storage
        s.put_data(b'k', b'v', is_result=True)

        # Available strictly before the deadline.
        self.clock.advance(9.999)
        self.assertEqual(s.peek_data(b'k'), b'v')
        self.assertTrue(s.has_data_for_key(b'k'))

        # Exactly at the deadline the result is unavailable.
        self.clock.advance(0.001)
        self.assertTrue(s.peek_data(b'k') is EmptyData)
        self.assertFalse(s.has_data_for_key(b'k'))
        self.assertTrue(s.pop_data(b'k') is EmptyData)

    def test_reads_do_not_renew(self):
        s = self.huey.storage
        s.put_data(b'k', b'v', is_result=True)

        self.clock.advance(6)
        self.assertEqual(s.peek_data(b'k'), b'v')
        self.assertEqual(s.peek_many([b'k']), {b'k': b'v'})
        self.assertTrue(s.wait_result(b'k', timeout=0.01))
        self.clock.advance(5)
        self.assertTrue(s.peek_data(b'k') is EmptyData)
        self.assertEqual(s.peek_many([b'k']), {})
        self.assertFalse(s.wait_result(b'k', timeout=0.01))

    def test_ttl_zero_immediately_unavailable(self):
        s = MemoryStorage('z', result_ttl=0,
                          time_function=FakeClock())
        s.put_data(b'k', b'v', is_result=True)
        self.assertTrue(s.peek_data(b'k') is EmptyData)
        self.assertTrue(s.pop_data(b'k') is EmptyData)
        self.assertFalse(s.has_data_for_key(b'k'))
        self.assertEqual(s.cleanup_results(), 0)
        # Metadata is still readable when ttl=0 is configured.
        s.put_data(b'meta', b'm')
        self.assertEqual(s.peek_data(b'meta'), b'm')

    def test_metadata_not_expired(self):
        s = self.huey.storage
        # Revocation markers and locks are stored without is_result and so
        # are unaffected by the result TTL.
        s.put_data(b'r:task', b'1')
        s.put_if_empty(b'huey.lock.x', b'1', ttl=100)
        self.clock.advance(50)
        self.assertEqual(s.peek_data(b'r:task'), b'1')
        self.assertTrue(s.has_data_for_key(b'huey.lock.x'))
        self.clock.advance(51)
        self.assertEqual(s.peek_data(b'r:task'), b'1')
        # Result cleanup never removes a lock, even one whose own ttl elapsed;
        # the lock is reclaimed lazily when it is next read.
        self.assertEqual(s.cleanup_results(), 0)
        self.assertEqual(s.peek_data(b'r:task'), b'1')
        self.assertFalse(s.has_data_for_key(b'huey.lock.x'))

    def test_cleanup_bounded(self):
        s = self.huey.storage
        s.put_data(b'a', b'1', is_result=True)
        self.clock.advance(5)
        s.put_data(b'b', b'2', is_result=True)
        self.clock.advance(6)  # a elapsed, b has 4s remaining.

        self.assertEqual(s.cleanup_results(limit=1), 1)
        self.assertTrue(s.peek_data(b'a') is EmptyData)
        self.assertEqual(s.peek_data(b'b'), b'2')
        self.assertEqual(s.cleanup_results(), 0)

        self.clock.advance(5)
        self.assertEqual(s.cleanup_results(), 1)

        s.put_data(b'meta', b'x')
        self.clock.advance(100)
        self.assertEqual(s.cleanup_results(), 0)
        self.assertEqual(s.peek_data(b'meta'), b'x')

    def test_result_items_exclude_expired(self):
        s = self.huey.storage
        s.put_data(b'a', b'1', is_result=True)
        s.put_data(b'meta', b'x')
        self.clock.advance(11)
        self.assertEqual(s.result_items(), {b'meta': b'x'})
        self.assertEqual(s.result_store_size(), 1)


class HueyResultTTLTests(BaseTestCase):
    def get_huey(self):
        self.clock, huey = memory_huey(store_none=True)
        return huey

    def _execute(self):
        return self.huey.execute(self.huey.dequeue())

    def test_success_result_expires(self):
        @self.huey.task()
        def add(a, b):
            return a + b

        r = add(2, 3)
        self.assertEqual(self._execute(), 5)
        self.assertEqual(r.get(), 5)
        self.clock.advance(10)
        # A fresh handle reads from storage; a handle that already returned
        # the value keeps it cached, matching the Result contract.
        self.assertIsNone(self.huey.result(r.id))
        self.assertEqual(self.huey.result_count(), 0)

    def test_none_result_expires(self):
        @self.huey.task()
        def noop():
            return None

        r = noop()
        self.assertIsNone(self._execute())
        # None is stored (store_none=True) and is distinguishable from a
        # missing result while alive.
        self.assertIsNone(r.get(preserve=True))
        self.assertTrue(r.is_ready())
        self.clock.advance(10)
        self.assertIsNone(self.huey.result(r.id, preserve=True))

    def test_preserve_read_no_renew(self):
        @self.huey.task()
        def add(a, b):
            return a + b

        r = add(1, 1)
        self._execute()
        self.clock.advance(6)
        self.assertEqual(r(preserve=True), 2)
        # A fresh handle performs a non-destructive read against storage.
        self.assertEqual(self.huey.result(r.id, preserve=True), 2)
        self.clock.advance(5)
        self.assertIsNone(self.huey.result(r.id, preserve=True))

    def test_blocking_wait_expired(self):
        @self.huey.task()
        def add(a, b):
            return a + b

        r = add(1, 1)
        self._execute()
        self.clock.advance(10)
        with self.assertRaises(ResultTimeout):
            r.get(blocking=True, timeout=0.01)

    def test_failure_and_retry_ttl(self):
        state = {'fail_forever': True, 'n': 0}

        @self.huey.task(retries=1)
        def flaky():
            state['n'] += 1
            if state['fail_forever']:
                raise ValueError('boom %d' % state['n'])
            return 'ok'

        # A failing task: the intermediate error (retries remaining) and the
        # terminal error (retries exhausted) both honor the result TTL.
        r = flaky()
        self.huey.execute(self.huey.dequeue())  # Attempt 1, retries -> 0.
        exc = self.trap_exception(lambda: r.get())
        self.assertIn('boom 1', str(exc))
        self.clock.advance(10)
        self.assertIsNone(self.huey.result(r.id))  # Intermediate expired.

        self.huey.execute(self.huey.dequeue())  # Attempt 2, terminal error.
        self.clock.advance(5)
        self.trap_exception(lambda: r.get())
        self.clock.advance(6)
        self.assertIsNone(self.huey.result(r.id))

        # A new run that succeeds on the (single) retry stores a result.
        r2 = flaky()
        # Attempt 1 fails before the flag is flipped, exercising a retry...
        self.huey.execute(self.huey.dequeue())
        # ...then the task succeeds on its second attempt.
        state['fail_forever'] = False
        self.huey.execute(self.huey.dequeue())
        self.assertEqual(r2.get(), 'ok')

    def test_per_task_ttl_override(self):
        @self.huey.task(result_ttl=2)
        def short():
            return 'short'

        @self.huey.task(result_ttl=20)
        def long():
            return 'long'

        @self.huey.task()
        def inherited():
            return 'inherited'

        rs, rl, ri = short(), long(), inherited()
        self._execute(); self._execute(); self._execute()
        self.clock.advance(5)
        self.assertIsNone(rs.get())
        self.assertEqual(rl.get(preserve=True), 'long')
        self.assertEqual(ri.get(preserve=True), 'inherited')
        self.clock.advance(6)  # 11 total: inherited (10) elapsed.
        self.assertIsNone(self.huey.result(ri.id, preserve=True))
        self.assertEqual(rl.get(preserve=True), 'long')

    def test_per_invocation_ttl(self):
        @self.huey.task()
        def add(a, b):
            return a + b

        task = add.s(1, 2, result_ttl=3)
        r = self.huey.enqueue(task)
        self._execute()
        self.clock.advance(4)
        self.assertIsNone(self.huey.result(r.id, preserve=True))

    def test_chord_coordination_not_expired(self):
        @self.huey.task()
        def add(a, b):
            return a + b

        @self.huey.task()
        def total(items):
            return sum(items)

        c = chord([add.s(1, 1), add.s(2, 2)], total.s())
        cr = self.huey.enqueue(c)
        self._execute()  # Member 1 -> 2.
        self.clock.advance(15)  # Past the result TTL.
        self._execute()  # Member 2 -> 4; chord still completes.
        self._execute()  # Callback.
        self.assertEqual(cr.get(), 6)

    def test_revocation_and_lock_unaffected(self):
        @self.huey.task()
        def task_a():
            return 'a'

        task_a.revoke()
        with self.huey.lock_task('l'):
            self.clock.advance(50)
            self.assertTrue(task_a.is_revoked())
            self.assertTrue(self.huey.is_locked('l'))
        self.assertTrue(task_a.restore())

    def test_ttl_config_semantics(self):
        # result_ttl=0: results expire immediately, but metadata is kept.
        clock = FakeClock()
        storage = MemoryStorage('z', result_ttl=0, time_function=clock)
        h0 = Huey('z', storage_class=lambda *a, **k: storage, utc=False)
        @h0.task()
        def t0():
            return 0
        r0 = t0()
        h0.execute(h0.dequeue())
        self.assertIsNone(r0.get())
        t0.revoke()
        self.assertTrue(t0.is_revoked())

        # result_ttl=None: results never expire.
        hn = MemoryHuey(utc=False)
        @hn.task()
        def tn():
            return 'n'
        rn = tn()
        hn.execute(hn.dequeue())
        clock.advance(10 ** 6)
        self.assertEqual(rn.get(preserve=True), 'n')

        # Negative huey-level value is rejected at configuration time.
        with self.assertRaises(ValueError):
            MemoryStorage('bad', result_ttl=-1)

        @self.huey.task()
        def ok_task():
            return True
        with self.assertRaises(ValueError):
            ok_task.s(result_ttl=-5)

    def test_unsupported_storage_errors(self):
        from huey.storage import FileStorage
        d = tempfile.mkdtemp()
        with self.assertRaises(ConfigurationError):
            Huey('bad', storage_class=FileStorage, path=d, result_ttl=10)
        with self.assertRaises(ConfigurationError):
            FileStorage('bad', d, result_ttl=10)

    def test_immediate_mode_ttl(self):
        clock = FakeClock()
        storage = MemoryStorage('imm', result_ttl=10, time_function=clock)
        h = Huey('imm', storage_class=lambda *a, **k: storage, immediate=True,
                 immediate_use_memory=False, utc=False)

        @h.task()
        def add(a, b):
            return a + b

        r = add(1, 2)  # Executed synchronously.
        self.assertEqual(r.get(), 3)
        clock.advance(10)
        self.assertIsNone(h.result(r.id))


class SqliteResultTTLTests(BaseTestCase):
    db_file = '/tmp/huey-ttl-test.db'

    def get_huey(self):
        self.clock = FakeClock()
        if os.path.exists(self.db_file):
            os.unlink(self.db_file)
        return SqliteHuey(filename=self.db_file, result_ttl=10,
                          time_function=self.clock, utc=False,
                          store_none=True)

    def tearDown(self):
        super(SqliteResultTTLTests, self).tearDown()
        self.huey.storage.close()
        if os.path.exists(self.db_file):
            os.unlink(self.db_file)

    def _execute(self):
        return self.huey.execute(self.huey.dequeue())

    def test_exact_expiry_boundary_storage(self):
        s = self.huey.storage
        s.put_data('k', b'v', is_result=True)
        self.clock.advance(10)
        self.assertTrue(s.peek_data('k') is EmptyData)

        s.put_data('k2', b'v2', is_result=True)
        self.clock.advance(9.999)
        self.assertEqual(s.peek_data('k2'), b'v2')
        self.assertEqual(s.pop_data('k2'), b'v2')

    def test_reads_do_not_renew(self):
        s = self.huey.storage
        s.put_data('k', b'v', is_result=True)
        self.clock.advance(6)
        self.assertEqual(s.peek_data('k'), b'v')
        self.assertEqual(s.peek_many(['k']), {'k': b'v'})
        self.clock.advance(5)
        self.assertTrue(s.peek_data('k') is EmptyData)
        self.assertEqual(s.peek_many(['k']), {})

    def test_none_and_preserve(self):
        @self.huey.task()
        def noop():
            return None

        r = noop()
        self._execute()
        self.assertIsNone(r.get(preserve=True))
        self.clock.advance(10)
        self.assertIsNone(r.get())
        self.assertEqual(self.huey.result_count(), 0)

    def test_cleanup_bounded(self):
        s = self.huey.storage
        s.put_data('a', b'1', is_result=True)
        self.clock.advance(5)
        s.put_data('b', b'2', is_result=True)
        s.put_data('meta', b'm')
        self.clock.advance(6)

        self.assertEqual(s.cleanup_results(limit=1), 1)
        self.assertEqual(s.peek_data('b'), b'2')
        self.assertEqual(s.peek_data('meta'), b'm')
        self.clock.advance(4)  # b reaches its 10s deadline at t=15.
        self.assertEqual(s.cleanup_results(), 1)
        self.clock.advance(100)
        self.assertEqual(s.cleanup_results(), 0)
        self.assertEqual(s.peek_data('meta'), b'm')

    def test_locks_excluded_from_cleanup(self):
        s = self.huey.storage
        # A lock carries its own lease ttl but is not a task result.
        self.assertTrue(s.put_if_empty('huey.lock.x', b'1', ttl=5))
        s.put_data('r', b'1')  # A revocation marker, never expires.
        self.clock.advance(6)
        self.assertEqual(s.cleanup_results(), 0)
        # The expired lock row still physically exists until lazily read,
        # which reports it unavailable; revocation data is untouched.
        self.assertFalse(s.has_data_for_key('huey.lock.x'))
        self.assertEqual(s.peek_data('r'), b'1')

    def test_two_instances_interleaved(self):
        # Two storage instances sharing the same file use the injected shared
        # clock and absolute timestamps, so expiration is visible to both.
        s1 = self.huey.storage
        s2 = SqliteStorage('huey', filename=self.db_file,
                           result_ttl=10, time_function=self.clock)
        try:
            s1.put_data('a', b'1', is_result=True)
            self.clock.advance(5)
            self.assertEqual(s2.peek_data('a'), b'1')  # Cross-instance read.
            s2.put_data('b', b'2', is_result=True)
            self.clock.advance(6)  # a elapsed; b has 4s.

            # Instance 1 actively reclaims a bounded number of rows.
            self.assertEqual(s1.cleanup_results(limit=1), 1)
            self.assertTrue(s2.peek_data('a') is EmptyData)
            self.assertEqual(s2.peek_data('b'), b'2')

            # Instance 2 lazily sees b expire and removes it.
            self.clock.advance(4)  # b reaches its deadline at t=15.
            self.assertFalse(s2.has_data_for_key('b'))
            self.assertEqual(s1.result_store_size(), 0)

            # New writes from s2 are immediately visible to s1.
            s2.put_data('c', b'3', is_result=True, ttl=2)
            self.assertEqual(s1.peek_data('c'), b'3')
            self.clock.advance(3)
            self.assertTrue(s1.peek_data('c') is EmptyData)
        finally:
            s2.close()

    def test_legacy_rows_without_expiry_remain_readable(self):
        # Simulate an old database (created before result TTL support): the
        # kv table has no expires column and contains result/metadata rows.
        self.huey.storage.close()
        if os.path.exists(self.db_file):
            os.unlink(self.db_file)

        conn = sqlite3.connect(self.db_file)
        try:
            conn.execute('create table kv (queue text not null, key text '
                         'not null, value blob not null, primary key(queue, '
                         'key))')
            conn.execute("insert into kv (queue, key, value) values "
                         "('huey', 'old-result', x'0102')")
            conn.execute("insert into kv (queue, key, value) values "
                         "('huey', 'r:old-task', x'0304')")
            conn.commit()
        finally:
            conn.close()

        # Opening a TTL-enabled storage migrates the schema but preserves and
        # keeps readable all pre-existing rows. Old rows carry no expiry (and
        # are not marked as volatile results), so they are never reclaimed.
        migrated = SqliteHuey(filename=self.db_file, result_ttl=10,
                              time_function=self.clock, utc=False)
        try:
            s = migrated.storage
            self.assertEqual(s.peek_data('old-result'), b'\x01\x02')
            self.assertEqual(s.peek_data('r:old-task'), b'\x03\x04')
            self.clock.advance(100)
            # Old rows have no expiry information and must never be reclaimed.
            self.assertEqual(s.cleanup_results(), 0)
            self.assertEqual(s.peek_data('old-result'), b'\x01\x02')
            self.assertEqual(s.peek_data('r:old-task'), b'\x03\x04')
            self.assertEqual(s.result_store_size(), 2)

            # New rows expire normally while old rows remain.
            s.put_data('new', b'x', is_result=True)
            self.clock.advance(11)
            self.assertEqual(s.cleanup_results(), 1)
            self.assertTrue(s.peek_data('new') is EmptyData)
            self.assertEqual(s.peek_data('old-result'), b'\x01\x02')
        finally:
            migrated.storage.close()

    def test_migrate_without_ttl_enabled(self):
        # A plain SqliteHuey also migrates old databases transparently.
        self.huey.storage.close()
        if os.path.exists(self.db_file):
            os.unlink(self.db_file)

        conn = sqlite3.connect(self.db_file)
        conn.execute('create table kv (queue text not null, key text '
                     'not null, value blob not null, primary key(queue, key))')
        conn.execute("insert into kv values ('huey', 'k', x'aa')")
        conn.commit()
        conn.close()

        huey = SqliteHuey(filename=self.db_file, utc=False)
        try:
            self.assertEqual(huey.storage.peek_data('k'), b'\xaa')
            huey.storage.put_data('k2', b'\xbb')
            self.assertEqual(huey.storage.peek_data('k2'), b'\xbb')
        finally:
            huey.storage.close()
