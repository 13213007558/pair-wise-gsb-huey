import datetime
import hashlib
import math
import os
import sqlite3
import struct
import threading

from huey.api import MemoryHuey
from huey.storage import RedisStorage
from huey.storage import PriorityRedisStorage
from huey.tests.base import BaseTestCase


class FakeClock(object):
    def __init__(self, value=0.0):
        self.value = float(value)

    def __call__(self):
        return self.value


class PriorityAgingTests(object):
    def make_storage(self, name='huey', **kwargs):
        raise NotImplementedError

    def test_aging_threshold_preserves_fifo_and_isolation(self):
        clock = FakeClock()
        storage = self.make_storage(priority_aging=10, clock=clock)
        other = self.make_storage(name='other', priority_aging=10,
                                  clock=clock)

        storage.enqueue(b'low', 0)
        storage.enqueue(b'same-1', 0)
        other.enqueue(b'other-low', 0)
        clock.value = 7
        storage.enqueue(b'high-1', 1)
        storage.enqueue(b'high-2', 1)
        other.enqueue(b'other-high', 1)

        clock.value = 16
        self.assertEqual(storage.dequeue(), b'low')
        self.assertEqual(storage.dequeue(), b'same-1')
        self.assertEqual(storage.dequeue(), b'high-1')
        self.assertEqual(storage.dequeue(), b'high-2')
        self.assertTrue(storage.dequeue() is None)

        self.assertEqual(other.dequeue(), b'other-low')
        self.assertEqual(other.dequeue(), b'other-high')

    def test_order_is_derived_from_wait_time_without_rewrite(self):
        clock = FakeClock()
        storage = self.make_storage(priority_aging=10, clock=clock)
        storage.enqueue(b'low', 0)
        clock.value = 7
        storage.enqueue(b'high', 1)

        self.assertEqual(storage.enqueued_items(), [b'high', b'low'])
        self.assertEqual(storage.queue_size(), 2)
        clock.value = 16
        self.assertEqual(storage.enqueued_items(), [b'low', b'high'])
        self.assertEqual(storage.queue_size(), 2)
        self.assertEqual(storage.dequeue(), b'low')
        self.assertEqual(storage.dequeue(), b'high')

    def test_clock_rollback_and_long_wait_are_bounded(self):
        clock = FakeClock(100)
        storage = self.make_storage(priority_aging=10,
                                    priority_aging_max=2, clock=clock)
        storage.enqueue(b'low', 0)
        clock.value = 0
        storage.enqueue(b'high', 1)
        self.assertEqual(storage.dequeue(), b'high')
        self.assertEqual(storage.dequeue(), b'low')

        clock.value = 0
        storage.enqueue(b'capped-low', 0)
        storage.enqueue(b'still-high', 3)
        clock.value = 100000
        self.assertEqual(storage.dequeue(), b'still-high')
        self.assertEqual(storage.dequeue(), b'capped-low')

    def test_invalid_configuration_and_priority(self):
        for value in (0, -1, '10'):
            self.assertRaises(ValueError, self.make_storage,
                              priority_aging=value)
        self.assertRaises(ValueError, self.make_storage,
                          priority_aging=10, priority_aging_step=0)
        self.assertRaises(ValueError, self.make_storage,
                          priority_aging=10, priority_aging_max=-1)

        storage = self.make_storage(priority_aging=10)
        for priority in ('x', math.nan, math.inf, -math.inf):
            self.assertRaises(ValueError, storage.enqueue, b'bad', priority)

    def test_concurrent_consumers_each_task_once(self):
        storage = self.make_storage(priority_aging=10)
        total = 50
        consumers = 8
        for idx in range(total):
            storage.enqueue(('item-%s' % idx).encode('utf8'), idx % 3)

        results = []
        errors = []
        start = threading.Barrier(consumers + 1)
        lock = threading.Lock()

        def consume():
            start.wait()
            try:
                while True:
                    item = storage.dequeue()
                    if item is None:
                        return
                    with lock:
                        results.append(item)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=consume)
                   for _ in range(consumers)]
        for thread in threads:
            thread.start()
        start.wait()
        for thread in threads:
            thread.join(10)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), total)
        self.assertEqual(len(set(results)), total)


class TestMemoryPriorityAging(PriorityAgingTests, BaseTestCase):
    def make_storage(self, name='huey', **kwargs):
        from huey.storage import MemoryStorage
        return MemoryStorage(name, **kwargs)

    def test_default_remains_pure_priority(self):
        storage = self.make_storage(priority_aging=None)
        storage.enqueue(b'old-low', 0)
        storage.enqueue(b'new-high', 10)
        self.assertEqual(storage.enqueued_items(),
                         [b'new-high', b'old-low'])
        self.assertEqual(storage.dequeue(), b'new-high')
        self.assertEqual(storage.dequeue(), b'old-low')


class TestSqlitePriorityAging(PriorityAgingTests, BaseTestCase):
    def setUp(self):
        super(TestSqlitePriorityAging, self).setUp()
        self.filename = 'test-priority-aging-%s.db' % id(self)

    def tearDown(self):
        super(TestSqlitePriorityAging, self).tearDown()
        for filename in (self.filename, self.filename + '-wal',
                         self.filename + '-shm'):
            if os.path.exists(filename):
                os.unlink(filename)

    def make_storage(self, name='huey', **kwargs):
        from huey.storage import SqliteStorage
        return SqliteStorage(name, filename=self.filename, cache_mb=0,
                             **kwargs)

    def test_persistence_and_restart_use_enqueued_timestamp(self):
        clock = FakeClock()
        storage = self.make_storage(priority_aging=10, clock=clock)
        storage.enqueue(b'low', 0)
        clock.value = 7
        storage.enqueue(b'high', 1)
        storage.close()

        clock.value = 16
        storage = self.make_storage(priority_aging=10, clock=clock)
        self.assertEqual(storage.dequeue(), b'low')
        self.assertEqual(storage.dequeue(), b'high')
        storage.close()

    def test_existing_schema_is_migrated(self):
        conn = sqlite3.connect(self.filename)
        conn.execute('create table task (id integer primary key, queue text, '
                     'data blob, priority real)')
        conn.commit()
        conn.close()

        storage = self.make_storage(priority_aging=10)
        storage.enqueue(b'migrated', 0)
        self.assertEqual(storage.dequeue(), b'migrated')
        storage.close()


class TestHueyPriorityAgingSemantics(BaseTestCase):
    def get_huey(self):
        return MemoryHuey(utc=False)

    def test_schedule_wait_does_not_count_before_ready(self):
        clock = FakeClock(100)
        huey = MemoryHuey(utc=False, priority_aging=10, clock=clock)

        @huey.task()
        def low(value):
            return value

        @huey.task(priority=1)
        def high(value):
            return value

        low_task = low.s('low', eta=datetime.datetime.fromtimestamp(100))
        huey.storage.add_to_schedule(
            huey.serialize_task(low_task),
            datetime.datetime.fromtimestamp(100))
        high('high-1')
        high('high-2')

        moved = huey.read_schedule(datetime.datetime.fromtimestamp(100))
        self.assertEqual(len(moved), 1)
        clock.value = 109
        huey.enqueue(moved[0])
        self.assertEqual(huey.dequeue().args, ('high-1',))
        self.assertEqual(huey.dequeue().args, ('high-2',))
        self.assertEqual(huey.dequeue().args, ('low',))

    def test_immediate_retry_resets_queue_wait(self):
        clock = FakeClock()
        huey = MemoryHuey(utc=False, priority_aging=10, clock=clock)
        state = []

        @huey.task(retries=1)
        def low():
            state.append('low')
            raise ValueError('retry once')

        @huey.task(priority=3)
        def high():
            return 'high'

        low()
        clock.value = 10
        retry_task = huey.dequeue()
        huey.execute(retry_task)
        high()
        self.assertEqual(huey.dequeue().name, 'high')
        retry_task = huey.dequeue()
        self.assertEqual(retry_task.name, 'low')
        huey.execute(retry_task)


class FakeScript(object):
    def __init__(self, script, conn):
        self.script = script
        self.sha = hashlib.sha1(script.encode('utf8')).hexdigest()
        self.conn = conn

    def __call__(self, keys=None, args=None):
        return self.conn.evalsha(
            self.sha, len(keys or []),
            *(list(keys or []) + list(args or [])))


class FakePipeline(object):
    def __init__(self, conn):
        self.conn = conn
        self.commands = []

    def zadd(self, key, mapping):
        self.commands.append(('zadd', key, dict(mapping)))

    def rpush(self, key, value):
        self.commands.append(('rpush', key, value))

    def execute(self):
        results = []
        for command, key, value in self.commands:
            results.append(getattr(self.conn, command)(key, value))
        return results


class FakeRedisConnection(object):
    def __init__(self):
        self.scripts = {}
        self.zsets = {}
        self.lists = {}
        self.counters = {}
        self.calls = []

    def client_setname(self, name):
        self.calls.append(('client_setname', name))

    def register_script(self, script):
        fake = FakeScript(script, self)
        self.scripts[fake.sha] = script
        return fake

    def incr(self, key):
        self.calls.append(('incr', key))
        value = self.counters.get(key, 0) + 1
        self.counters[key] = value
        return value

    def pipeline(self):
        return FakePipeline(self)

    def zadd(self, key, mapping):
        self.calls.append(('zadd', key, list(mapping)))
        self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            self.zsets[key][member] = float(score)
        return 1

    def rpush(self, key, value):
        self.calls.append(('rpush', key))
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    def blpop(self, key, timeout=None):
        values = self.lists.get(key, [])
        self.calls.append(('blpop', key, timeout, bool(values)))
        if values:
            return key, values.pop(0)

    def zcard(self, key):
        return len(self.zsets.get(key, {}))

    def zrange(self, key, start, end, withscores=False):
        members = list(self.zsets.get(key, {}).items())
        if end != -1:
            members = members[start:end + 1]
        elif start:
            members = members[start:]
        if withscores:
            return members
        return [member for member, _ in members]

    def delete(self, *keys):
        count = 0
        for key in keys:
            count += int(key in self.zsets) + int(key in self.lists)
            count += int(key in self.counters)
            self.zsets.pop(key, None)
            self.lists.pop(key, None)
            self.counters.pop(key, None)
        return count

    def evalsha(self, sha, numkeys, *args):
        self.calls.append(('evalsha', sha, numkeys, tuple(args)))
        key = args[0]
        now, aging, step, max_boost = map(float, args[1:])
        best = None
        for member, priority in self.zsets.get(key, {}).items():
            enqueued_at = struct.unpack('>Q', member[:8])[0]
            seq = struct.unpack('>Q', member[8:16])[0]
            boost = min(max_boost, max(
                0, math.floor((now - enqueued_at) / aging)))
            effective = priority + boost * step
            if (best is None or effective > best[0] or
                    (effective == best[0] and seq < best[1])):
                best = (effective, seq, member)
        if best is not None:
            del self.zsets[key][best[2]]
            return best[2]


class FakeRedisClient(object):
    def __init__(self, connection_pool=None):
        self.connection_pool = connection_pool

    def __getattr__(self, name):
        return getattr(self.connection_pool, name)


class TestRedisPriorityAgingProtocol(BaseTestCase):
    def get_huey(self):
        return MemoryHuey(utc=False)

    def make_storage(self, blocking=False, **kwargs):
        conn = FakeRedisConnection()
        old_client = RedisStorage.redis_client
        RedisStorage.redis_client = FakeRedisClient
        try:
            return RedisStorage(
                name='test', connection_pool=conn, blocking=blocking,
                **kwargs), conn
        finally:
            RedisStorage.redis_client = old_client

    def test_protocol_aging_enqueue_and_atomic_lua_dequeue(self):
        clock = FakeClock()
        storage, conn = self.make_storage(
            priority_aging=10, clock=clock)
        storage.enqueue(b'low', 0)
        clock.value = 7
        storage.enqueue(b'high', 1)

        queue_key = 'huey.redis.test'
        self.assertEqual([call for call in conn.calls if call[0] == 'incr'],
                         [('incr', 'huey.redis.queue-seq.test'),
                          ('incr', 'huey.redis.queue-seq.test')])
        members = list(conn.zsets[queue_key])
        self.assertTrue(all(len(member) >= 16 for member in members))
        self.assertEqual([conn.zsets[queue_key][m] for m in members],
                         [0.0, 1.0])
        self.assertEqual(conn.lists['huey.redis.queue-notify.test'], [1, 1])

        clock.value = 16
        self.assertEqual(storage.dequeue(), b'low')
        self.assertEqual(conn.calls[-1][0], 'evalsha')
        self.assertEqual(conn.calls[-1][2], 1)
        self.assertEqual(conn.zcard(queue_key), 1)
        self.assertEqual(storage.dequeue(), b'high')

    def test_protocol_same_priority_uses_fifo_sequence(self):
        clock = FakeClock()
        storage, conn = self.make_storage(
            priority_aging=10, clock=clock)
        storage.enqueue(b'a', 1)
        storage.enqueue(b'b', 1)

        self.assertEqual(storage.dequeue(), b'a')
        clock.value = 100
        self.assertEqual(storage.dequeue(), b'b')

    def test_blocking_protocol_waits_on_notification_key(self):
        clock = FakeClock()
        storage, conn = self.make_storage(
            blocking=True, read_timeout=7, priority_aging=10, clock=clock)
        self.assertTrue(storage.dequeue() is None)
        self.assertIn(
            ('blpop', 'huey.redis.queue-notify.test', 7, False),
            conn.calls)

        storage.enqueue(b'ready', 0)
        self.assertEqual(storage.dequeue(), b'ready')
        self.assertNotEqual(conn.calls[-1][0], 'blpop')

    def test_default_priority_redis_keeps_legacy_protocol(self):
        conn = FakeRedisConnection()
        old_client = PriorityRedisStorage.redis_client
        PriorityRedisStorage.redis_client = FakeRedisClient
        try:
            storage = PriorityRedisStorage(
                name='test', connection_pool=conn, blocking=False)
            storage.enqueue(b'task', 2)
        finally:
            PriorityRedisStorage.redis_client = old_client

        member, = conn.zsets['huey.redis.test']
        self.assertEqual(len(member) - len(b'task'), 8)
        self.assertEqual(conn.zsets['huey.redis.test'][member], -2.0)
        self.assertNotIn(
            ('incr', 'huey.redis.queue-seq.test'), conn.calls)
