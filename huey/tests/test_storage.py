import datetime
import hashlib
import itertools
import os
import random
import shutil
import sqlite3
import struct
import threading
import time
import multiprocessing
import tempfile
import unittest
import uuid
from queue import Queue

try:
    import cysqlite
except ImportError:
    cysqlite = None

try:
    from redis.connection import ConnectionPool
    from redis import Redis
    from redis.exceptions import ConnectionError as RedisConnectionError
except ImportError:
    ConnectionPool = Redis = RedisConnectionError = None

from huey.api import CySqliteHuey
from huey.api import Huey
from huey.api import MemoryHuey
from huey.api import PriorityRedisHuey
from huey.api import RedisExpireHuey
from huey.api import RedisHuey
from huey.api import SqliteHuey
from huey.api import crontab
from huey.api import chord
from huey.consumer import Scheduler
from huey.constants import EmptyData
from huey.exceptions import ConfigurationError
from huey.storage import FileStorage
from huey.storage import SqliteStorage
from huey.tests.base import BaseTestCase
from huey.tests.base import CI
from huey.tests.base import slow_test


def get_redis_version():
    # Major version of the locally-running redis server, or 0 if the redis
    # client library or server are not available.
    if Redis is None:
        return 0
    try:
        info = Redis().info()
    except Exception:
        return 0
    return int(info['redis_version'].split('.', 1)[0])

REDIS_VERSION = get_redis_version()
requires_redis = unittest.skipIf(REDIS_VERSION == 0, 'requires redis server')


def _crash_after_schedule_insert(filename, queue_name):
    from huey.storage import SqliteStorage

    storage = SqliteStorage(queue_name, filename=filename, timeout=1)
    insert_task = storage._insert_task

    def insert_and_exit(cursor, data, priority=None):
        insert_task(cursor, data, priority)
        os._exit(17)

    storage._insert_task = insert_and_exit
    storage.enqueue_schedule(
        datetime.datetime(2000, 1, 1), lambda data: (data, 0))


class StorageTests(object):
    destructive_reads = True
    supports_ttl = True

    def setUp(self):
        super(StorageTests, self).setUp()
        self.s = self.huey.storage
        self.s.flush_all()

    def test_peek_many(self):
        self.s.put_data('k1', b'v1')
        self.s.put_data('k2', b'v2')
        self.assertEqual(self.s.peek_many(['k1', 'kx', 'k2']),
                         {'k1': b'v1', 'k2': b'v2'})
        self.assertEqual(self.s.peek_many(['kx']), {})

    def test_result_items_str_keys(self):
        # Task ids are stored as str, and every backend returns them as str.
        self.s.put_data('k1', b'v1')
        self.s.put_data('k2', b'v2')
        self.assertEqual(self.s.result_items(), {'k1': b'v1', 'k2': b'v2'})

    @slow_test()
    def test_put_if_empty_ttl(self):
        if not self.supports_ttl:
            self.assertRaises(NotImplementedError, self.s.put_if_empty,
                              b'k1', b'v1', 1)
            return

        self.assertTrue(self.s.put_if_empty(b'k1', b'v1', ttl=1))
        self.assertFalse(self.s.put_if_empty(b'k1', b'v2', ttl=1))
        self.assertEqual(self.s.peek_data(b'k1'), b'v1')
        time.sleep(1.1)
        self.assertFalse(self.s.has_data_for_key(b'k1'))
        self.assertTrue(self.s.put_if_empty(b'k1', b'v3'))
        self.assertFalse(self.s.put_if_empty(b'k1', b'v4'))
        self.assertEqual(self.s.peek_data(b'k1'), b'v3')

    def tearDown(self):
        super(StorageTests, self).tearDown()
        self.s.flush_all()

    def test_queue_methods(self):
        for i in range(3):
            self.s.enqueue(b'item-%d' % i)

        # A limit returns exactly the next-N items to be dequeued.
        self.assertEqual(self.s.enqueued_items(2), [b'item-0', b'item-1'])

        # Remove two items (this API is not used, but we'll test it anyways).
        self.assertEqual(self.s.dequeue(), b'item-0')
        self.assertEqual(self.s.queue_size(), 2)
        self.assertEqual(self.s.enqueued_items(), [b'item-1', b'item-2'])
        self.assertEqual(self.s.dequeue(), b'item-1')
        self.assertEqual(self.s.dequeue(), b'item-2')
        self.assertTrue(self.s.dequeue() is None)

        self.assertEqual(self.s.queue_size(), 0)

        # Test flushing the queue.
        self.s.enqueue(b'item-3')
        self.assertEqual(self.s.queue_size(), 1)
        self.s.flush_queue()
        self.assertEqual(self.s.queue_size(), 0)

    def test_schedule_methods(self):
        timestamp = datetime.datetime(2000, 1, 2, 3, 4, 5)
        second = datetime.timedelta(seconds=1)

        items = ((b'p1', timestamp + second),
                 (b'p0', timestamp),
                 (b'n1', timestamp - second),
                 (b'p2', timestamp + second + second))
        for data, ts in items:
            self.s.add_to_schedule(data, ts)

        self.assertEqual(self.s.schedule_size(), 4)

        # A limit returns exactly limit items, soonest first.
        self.assertEqual(self.s.scheduled_items(2), [b'n1', b'p0'])

        # Read from the schedule up-to the "p0" timestamp.
        sched = self.s.read_schedule(timestamp)
        self.assertEqual(sched, [b'n1', b'p0'])

        self.assertEqual(self.s.scheduled_items(), [b'p1', b'p2'])
        self.assertEqual(self.s.schedule_size(), 2)
        sched = self.s.read_schedule(datetime.datetime.now())
        self.assertEqual(sched, [b'p1', b'p2'])
        self.assertEqual(self.s.schedule_size(), 0)
        self.assertEqual(self.s.read_schedule(datetime.datetime.now()), [])

    def test_result_store_methods(self):
        # Put and peek at data. Verify missing keys return EmptyData sentinel.
        self.s.put_data(b'k1', b'v1')
        self.s.put_data(b'k2', b'v2')
        self.assertEqual(self.s.peek_data(b'k2'), b'v2')
        self.assertEqual(self.s.peek_data(b'k1'), b'v1')
        self.assertTrue(self.s.peek_data(b'kx') is EmptyData)
        self.assertEqual(self.s.result_store_size(), 2)

        # Verify we can overwrite existing keys and that pop will remove the
        # key/value pair. Subsequent pop on missing key will return EmptyData.
        self.s.put_data(b'k1', b'v1-x')
        self.assertEqual(self.s.peek_data(b'k1'), b'v1-x')
        self.assertEqual(self.s.pop_data(b'k1'), b'v1-x')
        if self.destructive_reads:
            self.assertTrue(self.s.pop_data(b'k1') is EmptyData)
        else:
            self.assertEqual(self.s.pop_data(b'k1'), b'v1-x')
            self.assertTrue(self.s.delete_data(b'k1'))

        self.assertFalse(self.s.has_data_for_key(b'k1'))
        self.assertTrue(self.s.has_data_for_key(b'k2'))
        self.assertEqual(self.s.result_store_size(), 1)

        # Test put-if-empty.
        self.assertTrue(self.s.put_if_empty(b'k1', b'v1-y'))
        self.assertFalse(self.s.put_if_empty(b'k1', b'v1-z'))
        self.assertEqual(self.s.peek_data(b'k1'), b'v1-y')

        # Test deletion.
        self.assertTrue(self.s.put_if_empty(b'k3', b'v3'))
        self.assertTrue(self.s.delete_data(b'k3'))
        self.assertFalse(self.s.delete_data(b'k3'))

        # Test introspection.
        state = self.s.result_items()  # Normalize keys to unicode strings.
        clean = {k.decode('utf8') if isinstance(k, bytes) else k: v
                 for k, v in state.items()}
        self.assertEqual(clean, {'k1': b'v1-y', 'k2': b'v2'})
        self.s.flush_results()
        self.assertEqual(self.s.result_store_size(), 0)
        self.assertEqual(self.s.result_items(), {})

    def test_wait_result(self):
        key = str(uuid.uuid4())
        self.assertFalse(self.s.wait_result(key, timeout=0.1))
        self.s.put_data(key, b'v1', is_result=True)
        self.assertTrue(self.s.wait_result(key, timeout=1))

    def test_priority(self):
        if not self.s.priority:
            raise unittest.SkipTest('priority support required')

        priorities = (1, None, 5, None, 3, None, 9, None, 7, 0)
        for i, priority in enumerate(priorities):
            item = 'i%s-%s' % (i, priority)
            self.s.enqueue(item.encode('utf8'), priority)

        expected = [b'i6-9', b'i8-7', b'i2-5', b'i4-3', b'i0-1',
                    b'i1-None', b'i3-None', b'i5-None', b'i7-None', b'i9-0']
        self.assertEqual([self.s.dequeue() for _ in range(10)], expected)

    def test_counter(self):
        self.assertEqual(self.s.incr('k1'), 1)
        self.assertEqual(self.s.incr('k1'), 2)
        self.assertEqual(self.s.incr('k2', amount=10), 10)
        self.assertEqual(self.s.incr('k2', amount=-5), 5)
        self.assertEqual(self.s.incr('k2', amount=0), 5)
        self.assertEqual(self.s.incr('k3', 0), 0)

        self.s.delete_counter('k1')
        self.s.delete_counter('kx')
        self.assertEqual(self.s.incr('k1'), 1)
        self.assertEqual(self.s.incr('k2', amount=2), 7)

        self.s.flush_counters()
        self.assertEqual(self.s.incr('k1'), 1)
        self.assertEqual(self.s.incr('k2'), 1)

        # Ensure we don't collide w/result store.
        self.assertEqual(self.s.incr('k1'), 2)
        self.assertFalse(self.s.has_data_for_key('k1'))
        self.s.put_data('k1', b'test')
        self.assertEqual(self.s.incr('k1'), 3)
        self.assertEqual(self.s.pop_data('k1'), b'test')
        self.assertEqual(self.s.incr('k1'), 4)

    @slow_test()
    def test_consumer_integration(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        @self.huey.task()
        def total(ns):
            return sum(ns)

        with self.consumer_context():
            r1 = task_a(1)
            r2 = task_a(2)
            r3 = task_a(3)

            c = chord([task_a.s(1), task_a.s(2)], total)
            cr = self.huey.enqueue(c)

            self.assertEqual(r1.get(blocking=True, timeout=5), 2)
            self.assertEqual(r2.get(blocking=True, timeout=5), 3)
            self.assertEqual(r3.get(blocking=True, timeout=5), 4)
            self.assertEqual(cr.get(blocking=True, timeout=5), 5)
            self.assertEqual(cr.results(), [2, 3])

            task_a.revoke()
            self.assertTrue(task_a.is_revoked())
            self.assertTrue(task_a.restore())


class TestMemoryStorage(StorageTests, BaseTestCase):
    def get_huey(self):
        return MemoryHuey(utc=False)

    def test_put_if_empty_concurrent(self):
        nthreads, nkeys = 8, 20
        barrier = threading.Barrier(nthreads)
        winners = []

        def run(n):
            barrier.wait()
            for i in range(nkeys):
                if self.s.put_if_empty(b'k-%d' % i, b'%d' % n):
                    winners.append(i)

        threads = [threading.Thread(target=run, args=(n,))
                   for n in range(nthreads)]
        for t in threads: t.start()
        for t in threads: t.join()

        # Each key was acquired by exactly one thread.
        self.assertEqual(sorted(winners), list(range(nkeys)))


@requires_redis
class TestRedisStorage(StorageTests, BaseTestCase):
    def get_huey(self):
        return RedisHuey(utc=False)

    @property
    def supports_ttl(self):
        return self.s.supports_hash_ttl

    def test_put_if_empty_ttl_unsupported(self):
        self.s.supports_hash_ttl = False
        self.assertRaises(NotImplementedError, self.s.put_if_empty,
                          b'k1', b'v1', 1)
        self.assertFalse(self.s.has_data_for_key(b'k1'))
        self.assertTrue(self.s.put_if_empty(b'k1', b'v1'))
        self.assertEqual(self.s.peek_data(b'k1'), b'v1')

    def test_conflicting_init_args(self):
        options = {'host': 'localhost', 'url': 'redis://localhost',
                   'connection_pool': ConnectionPool()}
        combinations = itertools.combinations(options.items(), 2)
        for kwargs in (dict(item) for item in combinations):
            self.assertRaises(ConfigurationError, lambda: RedisHuey(**kwargs))

        # None values are fine, however.
        RedisHuey(host=None, port=None, db=None, url='redis://localhost')

    def test_name_not_mangled(self):
        s3 = RedisHuey('app-v1', utc=False).storage
        self.assertEqual(s3.name, 'appv1')
        s1 = RedisHuey('app-v1', utc=False, clean_name=False).storage
        s2 = RedisHuey('appv1', utc=False).storage
        self.assertEqual(s1.name, 'app-v1')
        self.assertNotEqual(s1.queue_key, s2.queue_key)
        s1.flush_all()
        s2.flush_all()
        s1.enqueue(b'i1')
        self.assertEqual(s2.queue_size(), 0)
        self.assertEqual(s1.dequeue(), b'i1')
        s1.flush_all()

    def test_empty_value(self):
        self.s.put_data(b'k1', b'')
        self.assertEqual(self.s.peek_data(b'k1'), b'')
        self.assertEqual(self.s.pop_data(b'k1'), b'')


class TestRedisStorageWaitResult(TestRedisStorage):
    def get_huey(self):
        return RedisHuey(utc=False, notify_result=True, notify_result_ttl=30)

    def test_notify_ttl(self):
        key = str(uuid.uuid4())
        self.s.put_data(key, b'v1', is_result=True)
        ttl = self.s.conn.ttl(self.s.notify_prefix + key)
        self.assertTrue(0 < ttl <= 30)
        self.s.pop_data(key)
        self.assertTrue(self.s.wait_result(key, timeout=1))
        self.assertFalse(self.s.conn.exists(self.s.notify_prefix + key))

    def test_wait_result_multiple_waiters(self):
        key = str(uuid.uuid4())
        q = Queue()
        def waiter():
            q.put(self.s.wait_result(key, timeout=2))
        threads = [threading.Thread(target=waiter) for _ in range(2)]
        for t in threads: t.start()
        time.sleep(0.2)
        self.s.put_data(key, b'v1', is_result=True)
        for t in threads: t.join()
        self.assertEqual([q.get(), q.get()], [True, True])


@requires_redis
class TestRedisExpireStorage(StorageTests, BaseTestCase):
    # Note that this does not subclass the StorageTests. This is partly because
    # the functionality should already be covered by the TestRedisStorage, as
    # the RedisExpireStorage is a subclass of RedisStorage -- but also because
    # the way the result store functions is fundamentally different, relying on
    # the database to handle result removal via expiration.
    destructive_reads = False

    def get_huey(self):
        return RedisExpireHuey(expire_time=3600, utc=False, blocking=False)

    def test_expire_results(self):
        self.s.put_data(b'k1', b'v1')
        self.s.put_data(b'k2', b'v2', is_result=True)

        conn = self.s.conn  # Underlying Redis client.

        # By default the put_data() API treats keys as being persistent. If we
        # specifically included the "is_result=True" flag, then the key will be
        # given a TTL.
        self.assertEqual(conn.ttl(self.s.result_key(b'k1')), -1)
        self.assertTrue(3580 <= conn.ttl(self.s.result_key(b'k2')) <= 3600)

        # Non-existent keys return -2. See redis docs for TTL command.
        self.assertEqual(conn.ttl(self.s.result_key(b'k3')), -2)

        # Non-expired keys return -1.
        conn.set(self.s.result_key(b'k3'), b'v3')
        self.assertEqual(conn.ttl(self.s.result_key(b'k3')), -1)

        # Verify behavior of put_if_empty and has_data_for_key.
        self.assertTrue(self.s.has_data_for_key(b'k2'))
        self.assertFalse(self.s.put_if_empty(b'k2', b'v2-x'))
        self.assertFalse(self.s.has_data_for_key(b'k4'))
        self.assertTrue(self.s.put_if_empty(b'k4', b'v4'))

        # Verify behavior of delete.
        self.assertTrue(self.s.delete_data(b'k2'))
        self.assertFalse(self.s.delete_data(b'k2'))

        # Counters expire.
        self.assertEqual(self.s.incr(b'c1'), 1)
        self.assertTrue(3580 <= conn.ttl(self.s.counter_key(b'c1')) <= 3600)

        # Check the result items.
        self.assertEqual(self.s.result_items(), {
            'k1': b'v1',
            'k3': b'v3',
            'k4': b'v4'})
        self.assertEqual(self.s.result_store_size(), 3)

    def test_integration_2(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        r1, r2, r3 = [task_a(i) for i in (1, 2, 3)]
        r2.revoke()
        self.assertTrue(r2.is_revoked())
        self.assertEqual(self.huey.result_count(), 1)  # Revoke key.

        self.assertEqual(self.execute_next(), 2)
        self.assertEqual(self.huey.result_count(), 2)  # Revoke key and r1.

        self.assertTrue(self.execute_next() is None)
        self.assertEqual(self.huey.result_count(), 1)  # Just r1 now.
        self.assertFalse(r2.is_revoked())

        self.assertEqual(self.execute_next(), 4)
        self.assertEqual(self.huey.result_count(), 2)  # r1 and r3.

        for _ in range(3):
            self.assertEqual(r1(), 2)
            self.assertEqual(r3(), 4)
            r1.reset()
            r3.reset()
        self.assertEqual(self.huey.result_count(), 2)  # r1 and r3 still there.


@unittest.skipIf(REDIS_VERSION < 5, 'Requires Redis >= 5.0')
class TestPriorityRedisStorage(TestRedisStorage):
    def get_huey(self):
        return PriorityRedisHuey(utc=False)


@unittest.skipIf(REDIS_VERSION < 5, 'Requires Redis >= 5.0')
class TestPriorityRedisStorageNotBlocking(TestRedisStorage):
    def get_huey(self):
        return PriorityRedisHuey(utc=False, blocking=False)


@unittest.skipIf(Redis is None, 'requires redis python module')
class TestRedisStorageOffline(BaseTestCase):
    # Verify client-side behavior using a stub client -- these tests do not
    # require a live redis server.
    def get_huey(self):
        return RedisHuey(utc=False)

    def test_dequeue_error_handling(self):
        s = self.huey.storage

        class StubConnErr(object):
            def brpop(self, key, timeout=None):
                raise RedisConnectionError('cannot connect')
        s.conn = StubConnErr()

        # Connection errors propagate, so the worker logs the error and
        # applies backoff rather than treating it as an empty queue.
        self.assertRaises(RedisConnectionError, s.dequeue)

        class StubEmpty(object):
            def brpop(self, key, timeout=None):
                return None  # BRPOP timed out, queue is empty.
        s.conn = StubEmpty()
        self.assertTrue(s.dequeue() is None)

    def test_wait_result_timeout_clamp(self):
        huey = RedisHuey(utc=False, notify_result=True)
        s = huey.storage
        captured = []

        class StubConn(object):
            def hexists(self, key, k):
                return False
            def blpop(self, key, timeout=None):
                captured.append(timeout)
                return None
        s.conn = StubConn()

        s.redis_version = (5, 0, 0)
        self.assertFalse(s.wait_result('k1', timeout=30))
        self.assertFalse(s.wait_result('k1', timeout=0.5))

        s.redis_version = (7, 4, 0)
        self.assertFalse(s.wait_result('k1', timeout=0.5))

        # For redis < 6 the timeout is coerced to an int >= 1 (rather than
        # being clamped *down* to at-most 1 second, or, for sub-second floats,
        # truncated to zero -- which blocks indefinitely). Newer servers
        # receive the timeout unmodified.
        self.assertEqual(captured, [30, 1, 0.5])

    def test_enqueued_items_limit(self):
        s = self.huey.storage
        calls = []

        class StubConn(object):
            def lrange(self, key, start, stop):
                calls.append((start, stop))
                # Head of the redis list is the most-recently enqueued item.
                return [b'i2', b'i1', b'i0']
        s.conn = StubConn()

        self.assertEqual(s.enqueued_items(), [b'i0', b'i1', b'i2'])
        self.assertEqual(s.enqueued_items(3), [b'i0', b'i1', b'i2'])

        # With a limit, items are read from the consumption end of the list,
        # e.g. the next-N items to be dequeued.
        self.assertEqual(calls, [(0, -1), (-3, -1)])


class TestSqliteStorage(StorageTests, BaseTestCase):
    supports_ttl = False

    def tearDown(self):
        super(TestSqliteStorage, self).tearDown()
        if os.path.exists('huey_storage.db'):
            os.unlink('huey_storage.db')

    def get_huey(self):
        return SqliteHuey(filename='huey_storage.db', timeout=3)

    def test_create_tables(self):
        huey = SqliteHuey(filename='huey_ct.db', create_tables=False)
        try:
            self.assertRaises(sqlite3.OperationalError, huey.pending_count)
            huey.storage.initialize_schema()
            self.assertEqual(huey.pending_count(), 0)
        finally:
            huey.storage.close()
            if os.path.exists('huey_ct.db'):
                os.unlink('huey_ct.db')

    def test_timeout(self):
        self.assertEqual(self.s._timeout, 3)
        curs = self.s.conn.execute('pragma busy_timeout')
        self.assertEqual(curs.fetchone(), (3000,))

    def test_read_schedule_large(self):
        n = 5000
        base = datetime.datetime(2000, 1, 1)
        order = list(range(n))
        random.Random(1).shuffle(order)
        for i in order:
            self.s.add_to_schedule(b'%d' % i,
                                   base + datetime.timedelta(seconds=i))
        self.assertEqual(self.s.schedule_size(), n)
        sched = self.s.read_schedule(base + datetime.timedelta(seconds=n))
        self.assertEqual(sched, [b'%d' % i for i in range(n)])
        self.assertEqual(self.s.schedule_size(), 0)

    def _due_schedule_storage(self, other=None, **kwargs):
        kwargs.setdefault('filename', self.s.filename)
        kwargs.setdefault('timeout', 3)
        return SqliteStorage(other or self.s.name, **kwargs)

    def test_enqueue_schedule_same_eta_two_connections(self):
        due = datetime.datetime(2000, 1, 1)
        count = 100
        for i in range(count):
            self.s.add_to_schedule(b'%03d' % i, due)

        first = self._due_schedule_storage()
        second = self._due_schedule_storage()
        barrier = sqlite3.connect(self.s.filename, timeout=3)
        barrier.execute('begin immediate')
        errors = []

        results = {}

        def move(storage, label, started_event=None):
            try:
                storage.schedule_batch_size = 1
                if started_event is not None:
                    started_event.set()
                moved = storage.enqueue_schedule(due, lambda data: (data, 0))
            except Exception as exc:
                errors.append(exc)
            else:
                results[label] = moved

        first_started = threading.Event()
        second_started = threading.Event()
        thread = threading.Thread(target=move, args=(first, 'first',
                                                     first_started))
        thread.start()
        first_started.wait(timeout=3)
        time.sleep(0.1)
        second_thread = threading.Thread(target=move,
                                         args=(second, 'second',
                                              second_started))
        second_thread.start()
        second_started.wait(timeout=3)
        time.sleep(0.1)
        barrier.commit()
        thread.join(timeout=3)
        second_thread.join(timeout=3)
        barrier.close()

        self.assertFalse(errors)
        self.assertIn(results['first'], range(count + 1))
        self.assertIn(results['second'], range(count + 1))
        self.assertEqual(results['first'] + results['second'], count)
        self.assertEqual(first.queue_size(), count)
        self.assertEqual(second.queue_size(), count)
        self.assertEqual(first.schedule_size(), 0)
        self.assertEqual(second.schedule_size(), 0)
        self.assertEqual(first.enqueue_schedule(due, lambda data: (data, 0)),
                         0)
        self.assertEqual(second.enqueue_schedule(due, lambda data: (data, 0)),
                         0)
        queued = set(self.s.enqueued_items())
        self.assertEqual(len(queued), count)
        self.assertEqual(queued, {b'%03d' % i for i in range(count)})
        first.close()
        second.close()

    def test_enqueue_schedule_rolls_back_deserialize_enqueue_commit(self):
        due = datetime.datetime(2000, 1, 1)
        for i in range(4):
            self.s.add_to_schedule(b'good-%d' % i, due)
        self.s.add_to_schedule(b'broken', due)
        other = self._due_schedule_storage()
        try:
            def deserialize(data):
                if data == b'broken':
                    raise ValueError('bad payload')
                return data, 0

            with self.assertRaises(ValueError):
                other.enqueue_schedule(due, deserialize)
            self.assertEqual(other.queue_size(), 0)
            self.assertEqual(other.schedule_size(), 5)

            original_insert = other._insert_task

            def fail_enqueue(cursor, data, priority=None):
                if fail_enqueue.failed:
                    return original_insert(cursor, data, priority)
                fail_enqueue.failed = True
                raise sqlite3.OperationalError('injected enqueue failure')

            fail_enqueue.failed = False
            other._insert_task = fail_enqueue
            with self.assertRaises(sqlite3.OperationalError):
                other.enqueue_schedule(due, lambda data: (data, 0))
            self.assertEqual(other.queue_size(), 0)
            self.assertEqual(other.schedule_size(), 5)

            def fail_commit(conn):
                conn.rollback()
                raise sqlite3.OperationalError('injected commit failure')

            original_commit = other._commit_connection
            other._commit_connection = fail_commit
            with self.assertRaises(sqlite3.OperationalError):
                other.enqueue_schedule(due, lambda data: (data, 0))
            other._commit_connection = original_commit
            self.assertEqual(other.queue_size(), 0)
            self.assertEqual(other.schedule_size(), 5)

            self.assertEqual(other.enqueue_schedule(due,
                                                     lambda data: (data, 0)),
                             5)
            self.assertEqual(other.schedule_size(), 0)
            self.assertEqual(sorted(other.enqueued_items()), [
                b'broken', b'good-0', b'good-1', b'good-2', b'good-3'])
        finally:
            other.close()

    def test_enqueue_schedule_huey_deserialize_rollback_recovery(self):
        other = SqliteHuey(name=self.s.name, filename=self.s.filename,
                           timeout=3, utc=False)
        try:
            @other.task()
            def scheduled_task(value):
                return value

            due = datetime.datetime(2000, 1, 1)
            task = scheduled_task.s(42)
            message = other.serialize_task(task)
            self.s.add_to_schedule(message, due)

            original_deserialize = other.deserialize_task

            def fail_once(payload):
                if not fail_once.failed:
                    fail_once.failed = True
                    raise ValueError('injected deserialize failure')
                return original_deserialize(payload)

            fail_once.failed = False
            other.deserialize_task = fail_once

            with self.assertRaises(ValueError):
                other.enqueue_schedule(due)
            self.assertEqual(other.pending_count(), 0)
            self.assertEqual(other.scheduled_count(), 1)

            other.deserialize_task = original_deserialize
            recovered, = other.enqueue_schedule(due)
            self.assertEqual(recovered.id, task.id)
            self.assertEqual(other.scheduled_count(), 0)
            self.assertEqual(other.pending_count(), 1)
        finally:
            other.storage.close()

    def test_enqueue_schedule_immediate_huey(self):
        executed = []
        huey = SqliteHuey(filename=self.s.filename, timeout=3, utc=False,
                          immediate=True, immediate_use_memory=False)
        try:
            @huey.task()
            def scheduled_task(value):
                executed.append(value)

            due = datetime.datetime(2000, 1, 1)
            huey.storage.add_to_schedule(huey.serialize_task(
                scheduled_task.s(7)), due)
            task, = huey.enqueue_schedule(due)
            self.assertEqual([value for value in executed], [7])
            self.assertEqual(huey.scheduled_count(), 0)
            self.assertEqual(huey.pending_count(), 1)
            self.assertEqual(task.args, (7,))
        finally:
            huey.storage.close()

    def test_scheduler_handles_due_and_periodic_sqlite_tasks(self):
        @self.huey.periodic_task(crontab(minute='*'))
        def periodic_task():
            return 'periodic'

        @self.huey.task()
        def regular_task(value):
            return value

        due = datetime.datetime(2000, 1, 1)
        self.huey.storage.add_to_schedule(
            self.huey.serialize_task(regular_task.s(3)), due)

        class NoSleepScheduler(Scheduler):
            def sleep_for_interval(self, current, interval):
                pass

        scheduler = NoSleepScheduler(self.huey, threading.Event(),
                                    interval=1, periodic=True)
        scheduler._next_loop = time.monotonic() + 60
        scheduler._next_periodic = time.monotonic() - 60
        scheduler.loop(due)

        self.assertEqual(self.huey.scheduled_count(), 0)
        self.assertEqual(self.huey.pending_count(), 2)
        queued = self.huey.pending()
        self.assertEqual({type(task).__name__ for task in queued},
                         {'periodic_task', 'regular_task'})

    def test_enqueue_schedule_process_crash_recovery(self):
        fd, filename = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.unlink(filename)
        queue = 'crash-test'
        try:
            setup = self._due_schedule_storage(queue, filename=filename)
            due = datetime.datetime(2000, 1, 1)
            setup.add_to_schedule(b'stays-scheduled', due)
            setup.close()

            ctx = multiprocessing.get_context('spawn')
            proc = ctx.Process(target=_crash_after_schedule_insert,
                               args=(filename, queue))
            proc.start()
            proc.join(timeout=5)
            self.assertEqual(proc.exitcode, 17)

            recovered = self._due_schedule_storage(queue, filename=filename)
            self.assertEqual(recovered.queue_size(), 0)
            self.assertEqual(recovered.schedule_size(), 1)
            self.assertEqual(recovered.scheduled_items(), [b'stays-scheduled'])
            self.assertEqual(
                recovered.enqueue_schedule(due, lambda data: (data, 0)), 1)
            self.assertEqual(recovered.enqueued_items(), [b'stays-scheduled'])
            self.assertEqual(recovered.schedule_size(), 0)
            recovered.close()
        finally:
            for suffix in ('', '-wal', '-shm'):
                path = filename + suffix
                if os.path.exists(path):
                    os.unlink(path)

    def test_enqueue_schedule_clock_rollback(self):
        eta1 = datetime.datetime(2000, 1, 1, 0, 0, 10)
        eta2 = datetime.datetime(2000, 1, 1, 0, 0, 2)
        eta3 = datetime.datetime(2000, 1, 1, 0, 0, 5)
        self.s.add_to_schedule(b'late', eta1)
        self.s.add_to_schedule(b'earliest', eta2)
        self.s.add_to_schedule(b'middle', eta3)

        now = datetime.datetime(2000, 1, 1, 0, 0, 4)
        self.assertEqual(self.s.enqueue_schedule(now, lambda data: (data, 0)),
                         1)
        self.assertEqual(self.s.enqueued_items(), [b'earliest'])

        earlier = datetime.datetime(2000, 1, 1, 0, 0, 3)
        self.assertEqual(self.s.enqueue_schedule(earlier,
                                                 lambda data: (data, 0)), 0)
        self.assertEqual(self.s.enqueued_items(), [b'earliest'])
        self.assertEqual(self.s.scheduled_items(), [b'middle', b'late'])

        due_all = datetime.datetime(2000, 1, 2)
        self.assertEqual(self.s.enqueue_schedule(due_all,
                                                 lambda data: (data, 0)), 2)
        self.assertEqual(self.s.enqueued_items(), [
            b'earliest', b'middle', b'late'])

    def test_enqueue_schedule_large_paginated_batch(self):
        n = 5000
        base = datetime.datetime(2000, 1, 1)
        for i in range(n):
            self.s.add_to_schedule(b'%05d' % i, base)

        self.s.schedule_batch_size = 123
        self.assertEqual(self.s.enqueue_schedule(base,
                                                 lambda data: (data, 0)), n)
        self.assertEqual(self.s.schedule_size(), 0)
        self.assertEqual(self.s.queue_size(), n)
        self.assertEqual(self.s.enqueued_items(3), [
            b'00000', b'00001', b'00002'])

    def test_enqueue_schedule_isolation_levels(self):
        due = datetime.datetime(2000, 1, 1)
        for isolation_level in (None, '', 'DEFERRED', 'IMMEDIATE',
                                'EXCLUSIVE'):
            fd, filename = tempfile.mkstemp(suffix='.db')
            os.close(fd)
            storage = SqliteStorage('isolation', filename=filename,
                                    timeout=0.25,
                                    isolation_level=isolation_level)
            try:
                self.assertEqual(storage.conn.isolation_level,
                                 isolation_level)
                self.assertEqual(
                    storage.conn.execute('pragma busy_timeout').fetchone()[0],
                    250)
                storage.add_to_schedule(b'due', due)
                self.assertEqual(storage.enqueue_schedule(
                    due, lambda data: (data, 0)), 1)
                self.assertEqual(storage.enqueued_items(), [b'due'])
                self.assertEqual(storage.schedule_size(), 0)
            finally:
                storage.close()
                for suffix in ('', '-wal', '-shm'):
                    if os.path.exists(filename + suffix):
                        os.unlink(filename + suffix)

    def test_shared_file_queues(self):
        other = SqliteHuey(name='other', filename='huey_storage.db',
                           timeout=3).storage
        try:
            self.s.enqueue(b'a1')
            other.enqueue(b'b1', priority=5)
            self.s.enqueue(b'a2', priority=3)
            other.enqueue(b'b2')
            self.assertEqual(self.s.queue_size(), 2)
            self.assertEqual(other.queue_size(), 2)
            self.assertEqual(self.s.enqueued_items(), [b'a2', b'a1'])
            self.assertEqual(other.enqueued_items(), [b'b1', b'b2'])
            self.assertEqual(self.s.dequeue(), b'a2')
            self.assertEqual(other.dequeue(), b'b1')
            self.assertEqual(self.s.dequeue(), b'a1')
            self.assertTrue(self.s.dequeue() is None)
            self.assertEqual(other.queue_size(), 1)
            other.flush_queue()
            self.assertTrue(other.dequeue() is None)
        finally:
            other.close()

    def test_dequeue_multithreaded(self):
        nthreads, ntasks = 8, 50
        for i in range(nthreads * ntasks):
            self.s.enqueue(b'%d' % i)

        out_q = Queue()

        def dequeue_tasks():
            while True:
                data = self.s.dequeue()
                if data is None:
                    break
                out_q.put(int(data))

        threads = [threading.Thread(target=dequeue_tasks)
                   for _ in range(nthreads)]
        for t in threads: t.start()
        for t in threads: t.join(timeout=10.)

        self.assertEqual(self.s.queue_size(), 0)
        seen = sorted(out_q.get() for _ in range(out_q.qsize()))
        self.assertEqual(seen, list(range(nthreads * ntasks)))

    def test_task_index(self):
        curs = self.s.conn.execute('select name from sqlite_master where '
                                   'type = ? and tbl_name = ?',
                                   ('index', 'task'))
        self.assertEqual([r[0] for r in curs.fetchall()],
                         ['task_queue_priority_id'])


@unittest.skipIf(cysqlite is None, 'requires cysqlite')
class TestCySqliteStorage(StorageTests, BaseTestCase):
    supports_ttl = False

    def tearDown(self):
        super(TestCySqliteStorage, self).tearDown()
        if os.path.exists('huey_storage.db'):
            os.unlink('huey_storage.db')

    def get_huey(self):
        return CySqliteHuey(filename='huey_storage.db', timeout=3, pragmas={
            'mmap_size': 1024 * 1024 * 32,
            'synchronous': 1,
        })

    def test_pragmas_preserved(self):
        conn = self.s.conn
        self.assertEqual(conn.pragma('mmap_size'), 1024 * 1024 * 32)
        self.assertEqual(conn.pragma('synchronous'), 1)
        self.assertEqual(conn.pragma('journal_mode'), 'wal')

    def test_sqlite_params_normalized(self):
        pragmas = {'mmap_size': 4096}
        huey = CySqliteHuey(filename='huey_np.db', pragmas=pragmas,
                            cache_mb=4, fsync=True, journal_mode='truncate')
        try:
            conn = huey.storage.conn
            self.assertEqual(conn.pragma('mmap_size'), 4096)
            self.assertEqual(conn.pragma('cache_size'), -4000)
            self.assertEqual(conn.pragma('synchronous'), 2)
            self.assertEqual(conn.pragma('journal_mode'), 'truncate')
            self.assertEqual(pragmas, {'mmap_size': 4096})
        finally:
            huey.storage.close()
            if os.path.exists('huey_np.db'):
                os.unlink('huey_np.db')

    def test_create_tables(self):
        huey = CySqliteHuey(filename='huey_ct.db', create_tables=False)
        try:
            self.assertRaises(cysqlite.OperationalError, huey.pending_count)
            huey.storage.initialize_schema()
            self.assertEqual(huey.pending_count(), 0)
        finally:
            huey.storage.close()
            if os.path.exists('huey_ct.db'):
                os.unlink('huey_ct.db')

    def test_timeout(self):
        self.assertEqual(self.s._timeout, 3)
        self.assertEqual(self.s.conn.pragma('busy_timeout'), 3000)


class TestFileStorageMethods(StorageTests, BaseTestCase):
    path = '/tmp/test-huey-storage'
    result_path = '/tmp/test-huey-storage/results'
    queue_path = '/tmp/test-huey-storage/queue'
    supports_ttl = False

    def tearDown(self):
        super(TestFileStorageMethods, self).tearDown()
        for path in (self.path, self.path + '-other'):
            if os.path.exists(path):
                shutil.rmtree(path)

    def test_float_priority(self):
        # Fractional priorities are truncated rather than raising.
        self.s.enqueue(b'low', priority=1.5)
        self.s.enqueue(b'high', priority=2.5)
        self.assertEqual(self.s.dequeue(), b'high')
        self.assertEqual(self.s.dequeue(), b'low')

    def get_huey(self):
        return Huey('test-file-storage', storage_class=FileStorage,
                    path=self.path, levels=2, use_thread_lock=True)

    def test_filesystem_result_store(self):
        s = self.huey.storage
        self.assertEqual(s.result_items(), {})

        keys = (b'k1', b'k2', b'kx')
        for key in keys:
            checksum = hashlib.md5(key).hexdigest()
            b0, b1 = checksum[0], checksum[1]
            # Default is to use two levels.
            key_path = os.path.join(self.result_path, b0, b1)
            key_filename = os.path.join(key_path, checksum)

            self.assertFalse(os.path.exists(key_filename))
            self.assertFalse(os.path.exists(key_path))

            s.put_data(key, b'test-%s' % key)
            self.assertTrue(os.path.exists(key_path))
            self.assertTrue(os.path.exists(key_filename))

            self.assertEqual(s.pop_data(key), b'test-%s' % key)
            self.assertTrue(os.path.exists(key_path))
            self.assertFalse(os.path.exists(key_filename))

        # Flushing the results blows away everything.
        s.flush_results()
        self.assertTrue(os.path.exists(self.result_path))
        self.assertEqual(os.listdir(self.result_path), [])

    def test_filesystem_result_items_truncated(self):
        s = self.huey.storage
        s.put_data(b'k1', b'v1')

        filename = s.path_for_key(b'k2')
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'wb') as fh:
            fh.write(struct.pack('>I', 8) + b'k2')

        self.assertEqual(s.result_items(), {'k1': b'v1'})

    def test_fs_multithreaded(self):
        l = threading.Lock()

        def create_tasks(t, n, q):
            for i in range(n):
                with l:
                    message = str((t * n) + i)
                    self.huey.storage.enqueue(message.encode('utf8'))
                    q.put(message)

        def dequeue_tasks(q):
            while True:
                with l:
                    data = self.huey.storage.dequeue()
                    if data is None:
                        break
                    q.put(data.decode('utf8'))

        nthreads = 10
        ntasks = 50
        in_q = Queue()
        threads = []
        for i in range(nthreads):
            t = threading.Thread(target=create_tasks, args=(i, ntasks, in_q))
            t.daemon = True
            threads.append(t)

        for t in threads: t.start()
        for t in threads: t.join(timeout=10.)

        self.assertEqual(self.huey.pending_count(), nthreads * ntasks)

        out_q = Queue()
        threads = []
        for i in range(nthreads):
            t = threading.Thread(target=dequeue_tasks, args=(out_q,))
            t.daemon = True
            threads.append(t)

        for t in threads: t.start()
        for t in threads: t.join(timeout=10.)

        self.assertEqual(out_q.qsize(), nthreads * ntasks)
        self.assertEqual(self.huey.pending_count(), 0)

        # Ensure that the order in which tasks were enqueued is the order in
        # which they are dequeued.
        for i in range(nthreads * ntasks):
            self.assertEqual(in_q.get(), out_q.get())

    def test_fs_threaded_file_lock(self):
        # Regression: FileLock shared one fd across threads. Contention
        # clobbered it, leaking a held flock and wedging every process.
        storage = FileStorage('lock-test', path=self.path)
        done = []

        def spin(n):
            for i in range(n):
                storage.enqueue(b'x')
                storage.dequeue()
            done.append(n)

        threads = []
        for i in range(4):
            t = threading.Thread(target=spin, args=(100,))
            t.daemon = True
            threads.append(t)

        for t in threads: t.start()
        for t in threads: t.join(timeout=15.)
        self.assertEqual(done, [100, 100, 100, 100])

    @unittest.skipIf(CI, 'skipping test that is flaky on CI')
    def test_consumer_integration(self):
        return super(TestFileStorageMethods, self).test_consumer_integration()


try:
    from huey.contrib.valkey_glide import ValkeyGlideHuey
except ImportError:
    ValkeyGlideHuey = None


@unittest.skipIf(ValkeyGlideHuey is None, 'valkey-glide-sync not installed')
class TestValkeyGlideStorage(StorageTests, BaseTestCase):
    supports_ttl = False

    def get_huey(self):
        return ValkeyGlideHuey(utc=False)
