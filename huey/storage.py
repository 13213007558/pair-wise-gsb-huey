from functools import cached_property
import contextlib
import hashlib
import heapq
import itertools
import os
import re
import shutil
try:
    import sqlite3
except ImportError:
    sqlite3 = None
import struct
import threading
import time

try:
    import cysqlite
except ImportError:
    cysqlite = None

try:
    from redis import ConnectionPool
    from redis import Redis
    from redis.exceptions import ConnectionError
    from redis.exceptions import TimeoutError
except ImportError:
    ConnectionPool = Redis = ConnectionError = TimeoutError = None

try:
    import psycopg
except ImportError:
    psycopg = None

from huey.constants import EmptyData
from huey.exceptions import ConfigurationError
from huey.utils import FileLock


class BaseStorage(object):
    """
    Base storage-layer interface. Subclasses should implement all methods.
    """
    blocking = False  # Does dequeue() block until ready, or should we poll?
    priority = True
    # Whether this storage supports applying a TTL (time-to-live) to task
    # result data. Storages that do not support this raise a clear error when
    # a result TTL is configured.
    supports_result_ttl = False

    def __init__(self, name='huey', result_ttl=None, **storage_kwargs):
        self.name = name
        self.configure_result_ttl(result_ttl)

    def configure_result_ttl(self, result_ttl=None):
        """
        Configure the default time-to-live (in seconds) for task result data.

        A value of ``None`` disables expiration (results are retained until
        explicitly consumed or flushed). A positive number marks task results
        with that lifetime. A value of ``0`` causes results to expire
        immediately. Negative values are not valid.

        Metadata like revocation keys, locks and chord coordination data is
        never expired by this setting.
        """
        if result_ttl is not None:
            if not self.supports_result_ttl:
                raise ConfigurationError(
                    '%s does not support a task result TTL. Use a storage '
                    'that supports result expiration (e.g. MemoryStorage or '
                    'SqliteStorage), or leave result_ttl unset.' %
                    type(self).__name__)
            if result_ttl < 0:
                raise ValueError('result_ttl must be a non-negative number '
                                 'or None, got %r.' % result_ttl)
        self.result_ttl = result_ttl

    def close(self):
        """
        Close or release any objects/handles used by storage layer.

        :returns: (optional) boolean indicating success
        """
        pass

    def enqueue(self, data, priority=None):
        """
        Given an opaque chunk of data, add it to the queue.

        :param bytes data: Task data.
        :param float priority: Priority, higher priorities processed first.
                               Defaults to 0.
        :return: No return value.

        Some storage may not implement support for priority. In that case, the
        storage may raise a NotImplementedError for non-None priority values.
        """
        raise NotImplementedError

    def dequeue(self):
        """
        Atomically remove data from the queue. If no data is available, no data
        is returned.

        :return: Opaque binary task data or None if queue is empty.
        """
        raise NotImplementedError

    def queue_size(self):
        """
        Return the length of the queue.

        :return: Number of tasks.
        """
        raise NotImplementedError

    def enqueued_items(self, limit=None):
        """
        Non-destructively read the given number of tasks from the queue. If no
        limit is specified, all tasks will be read.

        :param int limit: Restrict the number of tasks returned.
        :return: A list containing opaque binary task data.
        """
        raise NotImplementedError

    def flush_queue(self):
        """
        Remove all data from the queue.

        :return: No return value.
        """
        raise NotImplementedError

    def add_to_schedule(self, data, ts):
        """
        Add the given task data to the schedule, to be executed at the given
        timestamp.

        :param bytes data: Task data.
        :param datetime ts: Timestamp at which task should be executed.
        :return: No return value.
        """
        raise NotImplementedError

    def read_schedule(self, ts):
        """
        Read all tasks from the schedule that should be executed at or before
        the given timestamp. Once read, the tasks are removed from the
        schedule.

        :param datetime ts: Timestamp
        :return: List containing task data for tasks which should be executed
                 at or before the given timestamp.
        """
        raise NotImplementedError

    def schedule_size(self):
        """
        :return: The number of tasks currently in the schedule.
        """
        raise NotImplementedError

    def scheduled_items(self, limit=None):
        """
        Non-destructively read the given number of tasks from the schedule.

        :param int limit: Restrict the number of tasks returned.
        :return: List of tasks that are in schedule, in order from soonest to
                 latest.
        """
        raise NotImplementedError

    def flush_schedule(self):
        """
        Delete all scheduled tasks.

        :return: No return value.
        """
        raise NotImplementedError

    def put_data(self, key, value, is_result=False, ttl=None):
        """
        Store an arbitrary key/value pair, overwrites any existing value.

        :param bytes key: lookup key
        :param bytes value: value
        :param bool is_result: indicate if we are storing a (volatile) task
            result versus metadata like a task revocation key or lock.
        :param float ttl: when storing a task result, an optional per-result
            time-to-live in seconds, overriding any storage default.
        :return: No return value.
        """
        if ttl is not None and not self.supports_result_ttl:
            raise NotImplementedError(
                'per-result TTL is not supported by this storage.')
        raise NotImplementedError

    def peek_data(self, key):
        """
        Non-destructively read the value at the given key, if it exists.

        :param bytes key: Key to read.
        :return: Associated value, if key exists, or ``EmptyData``.
        """
        raise NotImplementedError

    def peek_many(self, keys):
        """
        Non-destructively read the values at the given keys.

        :param list keys: Keys to read.
        :return: Dictionary of key to value for the keys that exist.
        """
        accum = {}
        for key in keys:
            value = self.peek_data(key)
            if value is not EmptyData:
                accum[key] = value
        return accum

    def pop_data(self, key):
        """
        Destructively read the value at the given key, if it exists.

        :param bytes key: Key to read.
        :return: Associated value, if key exists, or ``EmptyData``.
        """
        raise NotImplementedError

    def wait_result(self, key, timeout=None, backoff=1.15, max_delay=1.0):
        """
        Block until a result is available for the given key, or until the
        timeout expires. Returns True if the result is available, or False if
        the timeout expired.

        The default implementation polls with exponential backoff, but Redis
        subclasses provide option to override with BLPOP for lower latency
        result notification (specify notify_result=True).
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        delay = 0.05
        while True:
            if self.has_data_for_key(key):
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(min(delay, max_delay))
            delay *= backoff

    def delete_data(self, key):
        """
        Delete the value at the given key, if it exists.

        :param bytes key: Key to delete.
        :return: boolean success or failure.
        """
        return self.pop_data(key) is not EmptyData

    def has_data_for_key(self, key):
        """
        Return whether there is data for the given key.

        :return: Boolean value.
        """
        raise NotImplementedError

    def put_if_empty(self, key, value, ttl=None):
        """
        Atomically write data only if the key is not already set.

        :param bytes key: Key to check/set.
        :param bytes value: Arbitrary data.
        :param int ttl: Seconds until the key expires. Supported by the memory
            and redis storages, others raise ``NotImplementedError``. With
            ``RedisStorage`` the server must support hash-field TTL (redis
            7.4+ or valkey 9+). ``RedisExpireStorage`` works on any version.
        :return: Boolean whether key/value was set.
        """
        if ttl is not None:
            raise NotImplementedError('ttl is not supported by this storage.')
        if self.has_data_for_key(key):
            return False
        self.put_data(key, value)
        return True

    def incr(self, key, amount=1):
        """
        Atomically increment a counter, returning the new value. If the key
        does not exist, it is assumed to be 0.
        """
        raise NotImplementedError

    def delete_counter(self, key):
        """
        Delete the counter at the given key.
        """
        raise NotImplementedError

    def result_store_size(self):
        """
        :return: Number of key/value pairs in the result store.
        """
        raise NotImplementedError

    def result_items(self):
        """
        Non-destructively read all the key/value pairs from the data-store.

        :return: Dictionary mapping all key/value pairs in the data-store.
            Keys written by huey are returned as unicode strings.
        """
        raise NotImplementedError

    def flush_results(self):
        """
        Delete all key/value pairs from the data-store.

        :return: No return value.
        """
        raise NotImplementedError

    def cleanup_results(self, limit=None):
        """
        Actively remove task results whose time-to-live has elapsed. Only
        result data is affected: revocation keys, locks and chord
        coordination data are stored without a TTL and are left untouched.

        :param int limit: optional bound on the maximum number of expired
            rows/keys removed by this call.
        :return: The number of expired items that were removed. Storages that
            do not support a result TTL return 0.
        """
        return 0

    def flush_counters(self):
        """
        Clear all counters.

        :return: No return value.
        """
        raise NotImplementedError

    def flush_all(self):
        """
        Remove all persistent or semi-persistent data.

        :return: No return value.
        """
        self.flush_queue()
        self.flush_schedule()
        self.flush_results()
        self.flush_counters()


class BlackHoleStorage(BaseStorage):
    def enqueue(self, data, priority=None): pass
    def dequeue(self): pass
    def queue_size(self): return 0
    def enqueued_items(self, limit=None): return []
    def flush_queue(self): pass
    def add_to_schedule(self, data, ts): pass
    def read_schedule(self, ts): return []
    def schedule_size(self): return 0
    def scheduled_items(self, limit=None): return []
    def flush_schedule(self): pass
    def put_data(self, key, value, is_result=False, ttl=None): pass
    def peek_data(self, key): return EmptyData
    def pop_data(self, key): return EmptyData
    def has_data_for_key(self, key): return False
    def put_if_empty(self, key, value, ttl=None): return True
    def incr(self, key, amount=1): return amount
    def delete_counter(self, key): pass
    def result_store_size(self): return 0
    def result_items(self): return {}
    def flush_results(self): pass
    def flush_counters(self): pass


class MemoryStorage(BaseStorage):
    supports_result_ttl = True

    def __init__(self, name='huey', result_ttl=None, time_function=None,
                 **kwargs):
        super(MemoryStorage, self).__init__(name, result_ttl, **kwargs)
        # ``time_function`` defaults to time.monotonic() and may be overridden
        # to enable deterministic, controlled-clock testing of TTL behavior.
        self.time = time_function or time.monotonic
        self._c = 0  # Counter to ensure FIFO behavior for queue.
        self._queue = []
        self._results = {}  # key -> value (both results and metadata).
        self._result_keys = set()  # Keys stored as (volatile) task results.
        self._expires = {}  # key -> absolute expiry timestamp (results only).
        self._schedule = []
        self._counters = {}
        self._lock = threading.RLock()

    def enqueue(self, data, priority=None):
        with self._lock:
            self._c += 1
            priority = 0 if priority is None else -priority
            heapq.heappush(self._queue, (priority, self._c, data))

    def dequeue(self):
        with self._lock:
            try:
                _, _, data = heapq.heappop(self._queue)
            except IndexError:
                pass
            else:
                return data

    def queue_size(self):
        return len(self._queue)

    def enqueued_items(self, limit=None):
        items = [data for _, _, data in sorted(self._queue)]
        if limit:
            items = items[:limit]
        return items

    def flush_queue(self):
        self._queue = []

    def add_to_schedule(self, data, ts):
        with self._lock:
            heapq.heappush(self._schedule, (ts, data))

    def read_schedule(self, ts):
        with self._lock:
            accum = []
            while self._schedule:
                sts, data = heapq.heappop(self._schedule)
                if sts <= ts:
                    accum.append(data)
                else:
                    heapq.heappush(self._schedule, (sts, data))
                    break

        return accum

    def schedule_size(self):
        return len(self._schedule)

    def scheduled_items(self, limit=None):
        items = [data for _, data in sorted(self._schedule)]
        if limit:
            items = items[:limit]
        return items

    def flush_schedule(self):
        self._schedule = []

    def _is_expired(self, key, now=None):
        expires = self._expires.get(key)
        return expires is not None and expires <= (now or self.time())

    def _expire(self, key):
        # Lazily remove the key if its result TTL has elapsed. Reading data
        # never extends the lifetime of a stored result.
        if self._is_expired(key):
            self._expires.pop(key, None)
            self._result_keys.discard(key)
            self._results.pop(key, None)

    def _put(self, key, value, expires=None):
        self._results[key] = value
        if expires is None:
            self._expires.pop(key, None)
            self._result_keys.discard(key)
        else:
            self._expires[key] = expires
            self._result_keys.add(key)

    def put_data(self, key, value, is_result=False, ttl=None):
        with self._lock:
            if is_result and ttl is None:
                ttl = self.result_ttl
            expires = None
            if is_result and ttl is not None:
                expires = self.time() + ttl
            self._put(key, value, expires)

    def peek_data(self, key):
        with self._lock:
            self._expire(key)
            return self._results.get(key, EmptyData)

    def pop_data(self, key):
        with self._lock:
            self._expire(key)
            if key in self._results:
                self._expires.pop(key, None)
                self._result_keys.discard(key)
                return self._results.pop(key)
            return EmptyData

    def has_data_for_key(self, key):
        with self._lock:
            self._expire(key)
            return key in self._results

    def put_if_empty(self, key, value, ttl=None):
        with self._lock:
            self._expire(key)
            if key in self._results:
                return False
            # Conditional writes are used for locks: a ttl here is the lock's
            # own lease, not a result TTL, so the key is not tracked as a
            # result and is never removed by cleanup_results().
            self._results[key] = value
            if ttl is None:
                self._expires.pop(key, None)
            else:
                self._expires[key] = self.time() + ttl
            self._result_keys.discard(key)
            return True

    def cleanup_results(self, limit=None):
        removed = 0
        with self._lock:
            now = self.time()
            expired = [key for key in self._result_keys
                       if self._expires.get(key, float('inf')) <= now]
            for key in expired:
                if limit is not None and removed >= limit:
                    break
                self._results.pop(key, None)
                self._expires.pop(key, None)
                self._result_keys.discard(key)
                removed += 1
        return removed

    def incr(self, key, amount=1):
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + amount
        return self._counters[key]

    def delete_counter(self, key):
        with self._lock:
            self._counters.pop(key, None)

    def result_store_size(self):
        with self._lock:
            self.cleanup_results()
            return len(self._results)

    def result_items(self):
        with self._lock:
            self.cleanup_results()
            return dict(self._results)

    def flush_results(self):
        # All key/value data (task results, revocation keys and locks) shares
        # the kv namespace, so flushing clears everything, as before.
        with self._lock:
            self._results = {}
            self._expires = {}
            self._result_keys = set()

    def flush_counters(self):
        self._counters = {}


# A custom lua script to pass to redis that will read tasks from the schedule
# and atomically pop them from the sorted set and return them. It won't return
# anything if it isn't able to remove the items it reads.
SCHEDULE_POP_LUA = """\
local unix_ts = tonumber(ARGV[1])
local res = redis.call('zrangebyscore', KEYS[1], '-inf', unix_ts)
if #res and redis.call('zremrangebyscore', KEYS[1], '-inf', unix_ts) == #res then
    return res
end"""


class RedisStorage(BaseStorage):
    priority = False  # Use PriorityRedisStorage instead. Requires Redis>=5.0.
    redis_client = Redis

    def __init__(self, name='huey', blocking=True, read_timeout=1,
                connection_pool=None, url=None, client_name=None,
                notify_result=False, notify_result_ttl=60,
                clean_name=True, result_ttl=None, **connection_params):

        if Redis is None:
            raise ConfigurationError('"redis" python module not found, cannot '
                                     'use Redis storage backend. Run "pip '
                                     'install redis" to install.')

        # Drop common empty values from the connection_params.
        for p in ('host', 'port', 'db'):
            if p in connection_params and connection_params[p] is None:
                del connection_params[p]

        if sum(1 for p in (url, connection_pool, connection_params) if p) > 1:
            raise ConfigurationError(
                'The connection configuration is over-determined. '
                'Please specify only one of the following: '
                '"url", "connection_pool", or "connection_params"')

        if url:
            connection_pool = ConnectionPool.from_url(url)
        elif connection_pool is None:
            connection_pool = ConnectionPool(**connection_params)

        self.pool = connection_pool
        self.conn = self.redis_client(connection_pool=connection_pool)
        self.connection_params = connection_params
        self._pop = self.conn.register_script(SCHEDULE_POP_LUA)

        self.name = self.clean_name(name) if clean_name else name
        self.queue_key = 'huey.redis.%s' % self.name
        self.schedule_key = 'huey.schedule.%s' % self.name
        self.result_key = 'huey.results.%s' % self.name
        self.counter_key = 'huey.counters.%s' % self.name
        self.notify_prefix = 'huey.notify.%s.' % self.name
        self.notify_result = notify_result  # Use result notification.
        self.notify_result_ttl = notify_result_ttl

        if client_name is not None:
            self.conn.client_setname(client_name)

        self.blocking = blocking
        self.read_timeout = read_timeout
        self.configure_result_ttl(result_ttl)

    @cached_property
    def redis_version(self):
        # Server version, used only to clamp BLPOP timeouts for redis < 6.
        try:
            version = str(self.conn.info()['redis_version'])
        except Exception:
            version = '0.0.0'  # Assume old, int timeouts always work.
        return tuple(int(i) if i.isdigit() else 999
                     for i in version.split('.'))

    @cached_property
    def supports_hash_ttl(self):
        # HEXPIRE needs redis 7.4+ or valkey 9+. Valkey reports redis_version
        # 7.2, so its own version field is checked separately.
        if not hasattr(self.conn, 'hexpire'):
            return False
        info = self.conn.info('server')
        for key, minver in (('redis_version', (7, 4)),
                            ('valkey_version', (9,))):
            try:
                version = tuple(int(p) for p in str(info[key]).split('.')[:2])
            except (KeyError, ValueError):
                continue
            if version >= minver:
                return True
        return False

    def clean_name(self, name):
        return re.sub('[^A-Za-z0-9_]', '', name)

    def convert_ts(self, ts):
        return time.mktime(ts.timetuple()) + (ts.microsecond * 1e-6)

    def enqueue(self, data, priority=None):
        if priority:
            raise NotImplementedError('Task priorities are not supported by '
                                      'this storage.')
        self.conn.lpush(self.queue_key, data)

    def dequeue(self):
        if self.blocking:
            try:
                return self.conn.brpop(
                    self.queue_key,
                    timeout=self.read_timeout)[1]
            except (TimeoutError, TypeError, IndexError):
                # Unfortunately, there is no way to differentiate a socket
                # timing out and a host being unreachable. ConnectionError is
                # allowed to propagate, however, so the worker logs the error
                # and applies backoff, rather than busy-looping silently.
                return None
        else:
            return self.conn.rpop(self.queue_key)

    def queue_size(self):
        return self.conn.llen(self.queue_key)

    def enqueued_items(self, limit=None):
        if limit:
            # Take items from the consumption end of the list, e.g. the next
            # `limit` tasks to be dequeued.
            return self.conn.lrange(self.queue_key, -limit, -1)[::-1]
        return self.conn.lrange(self.queue_key, 0, -1)[::-1]

    def flush_queue(self):
        self.conn.delete(self.queue_key)

    def add_to_schedule(self, data, ts):
        self.conn.zadd(self.schedule_key, {data: self.convert_ts(ts)})

    def read_schedule(self, ts):
        unix_ts = self.convert_ts(ts)
        # invoke the redis lua script that will atomically pop off
        # all the tasks older than the given timestamp
        tasks = self._pop(keys=[self.schedule_key], args=[unix_ts])
        return [] if tasks is None else tasks

    def schedule_size(self):
        return self.conn.zcard(self.schedule_key)

    def scheduled_items(self, limit=None):
        stop = limit - 1 if limit else -1
        return self.conn.zrange(self.schedule_key, 0, stop, withscores=False)

    def flush_schedule(self):
        self.conn.delete(self.schedule_key)

    def _notify(self, key):
        if isinstance(key, bytes):
            key = key.decode('utf8')
        nkey = self.notify_prefix + key
        pipe = self.conn.pipeline()
        pipe.lpush(nkey, b'1')
        pipe.expire(nkey, self.notify_result_ttl)
        pipe.execute()

    def put_data(self, key, value, is_result=False, ttl=None):
        if ttl is not None:
            raise NotImplementedError(
                'per-result TTL is not supported by this storage.')
        self.conn.hset(self.result_key, key, value)
        if is_result and self.notify_result:
            self._notify(key)

    def peek_data(self, key):
        val = self.conn.hget(self.result_key, key)
        return EmptyData if val is None else val

    def peek_many(self, keys):
        values = self.conn.hmget(self.result_key, keys)
        return {k: v for k, v in zip(keys, values) if v is not None}

    def pop_data(self, key):
        pipe = self.conn.pipeline()
        pipe.hget(self.result_key, key)
        pipe.hdel(self.result_key, key)
        val, _ = pipe.execute()
        return EmptyData if val is None else val

    def delete_data(self, key):
        return self.conn.hdel(self.result_key, key) != 0

    def wait_result(self, key, timeout=None, backoff=1.15, max_delay=1.0):
        if not self.notify_result:
            return super(RedisStorage, self).wait_result(key, timeout,
                                                         backoff, max_delay)

        if self.has_data_for_key(key):
            return True
        nkey = self.notify_prefix + key
        timeout = timeout or 0
        if timeout > 0 and self.redis_version[0] < 6:
            timeout = max(1, int(timeout))  # Timeout must be int for R < 6.
        try:
            result = self.conn.blpop(nkey, timeout=timeout)
        except (ConnectionError, TimeoutError):
            return False

        if result is not None:
            self.conn.delete(nkey)
            return True

        return self.has_data_for_key(key)

    def has_data_for_key(self, key):
        return self.conn.hexists(self.result_key, key)

    def put_if_empty(self, key, value, ttl=None):
        if ttl is not None and not self.supports_hash_ttl:
            raise NotImplementedError('ttl requires hash-field TTL support '
                                      '(redis 7.4+ or valkey 9+).')
        if not self.conn.hsetnx(self.result_key, key, value):
            return False
        if ttl is not None:
            self.conn.hexpire(self.result_key, ttl, key)
        return True

    def incr(self, key, amount=1):
        return self.conn.hincrby(self.counter_key, key, amount)

    def delete_counter(self, key):
        self.conn.hdel(self.counter_key, key)

    def result_store_size(self):
        return self.conn.hlen(self.result_key)

    def result_items(self):
        return {key.decode('utf8'): value for key, value
                in self.conn.hgetall(self.result_key).items()}

    def flush_results(self):
        self.conn.delete(self.result_key)

    def flush_counters(self):
        self.conn.delete(self.counter_key)


class RedisExpireStorage(RedisStorage):
    # Redis storage subclass that adds expiration to task result values. Since
    # the Redis server handles deleting our results after the expiration time,
    # this storage layer will not delete the results when they are read.
    supports_result_ttl = True

    def __init__(self, name='huey', expire_time=86400, result_ttl=None,
                 *args, **kwargs):
        super(RedisExpireStorage, self).__init__(
            name, result_ttl=result_ttl, *args, **kwargs)

        # The legacy expire_time parameter is the default TTL for results; an
        # explicit result_ttl takes precedence.
        self._expire_time = self.result_ttl if result_ttl is not None \
            else expire_time

        self.result_prefix = rp = b'huey.r.%s.' % self.name.encode('utf8')
        self.counter_prefix = cp = b'huey.c.%s.' % self.name.encode('utf8')

        encode = lambda s: s if isinstance(s, bytes) else s.encode('utf8')
        self.result_key = lambda k: rp + encode(k)
        self.counter_key = lambda k: cp + encode(k)

    def put_data(self, key, value, is_result=False, ttl=None):
        if is_result:
            # We only want to expire task result data. If we are storing an
            # important metadata like a revocation key, we need to preserve it.
            ex = ttl if ttl is not None else self._expire_time
            self.conn.set(self.result_key(key), value, ex=ex)
            if self.notify_result:
                self._notify(key)
        else:
            self.conn.set(self.result_key(key), value)

    def peek_data(self, key):
        val = self.conn.get(self.result_key(key))
        return EmptyData if val is None else val

    def peek_many(self, keys):
        values = self.conn.mget([self.result_key(k) for k in keys])
        return {k: v for k, v in zip(keys, values) if v is not None}

    # Here we explicitly prevent result items from being removed by using the
    # same implementation for "pop" (get and delete) as we do for "peek"
    # (non-destructive read).
    pop_data = peek_data

    def delete_data(self, key):
        return self.conn.delete(self.result_key(key))

    def has_data_for_key(self, key):
        return self.conn.exists(self.result_key(key)) != 0

    def put_if_empty(self, key, value, ttl=None):
        kwargs = {'nx': True}
        if ttl is not None:
            kwargs['ex'] = ttl
        return bool(self.conn.set(self.result_key(key), value, **kwargs))

    def incr(self, key, amount=1):
        pipe = self.conn.pipeline()
        pipe.incr(self.counter_key(key), amount)
        pipe.expire(self.counter_key(key), self._expire_time)
        return pipe.execute()[0]

    def delete_counter(self, key):
        self.conn.delete(self.counter_key(key))

    def _result_keys(self):
        return self.conn.scan_iter(match=self.result_prefix + b'*')

    def result_store_size(self):
        return len(list(self._result_keys()))

    def result_items(self):
        keys = list(self._result_keys())
        accum = {}
        if keys:
            pfx_len = len(self.result_prefix)
            for key, value in zip(keys, self.conn.mget(keys)):
                accum[key[pfx_len:].decode('utf8')] = value
        return accum

    def _counter_keys(self):
        return self.conn.scan_iter(match=self.counter_prefix + b'*')

    def flush_results(self):
        keys = list(self._result_keys())
        if keys:
            self.conn.delete(*keys)

    def flush_counters(self):
        keys = list(self._counter_keys())
        if keys:
            self.conn.delete(*keys)


class RedisPriorityQueue(object):
    priority = True

    def enqueue(self, data, priority=None):
        priority = 0 if priority is None else -priority
        # Prefix the message with an encoded timestamp to ensure that messages
        # created with the same priority are stored in the correct order. Since
        # the underlying data-type is a sorted-set, this also prevents multiple
        # identical messages, except they are enqueued on the same microsecond,
        # from being treated as a single item.
        prefix = struct.pack('>Q', int(time.time() * 1e6))
        self.conn.zadd(self.queue_key, {prefix + data: priority})

    def dequeue(self):
        if self.blocking:
            try:
                # BZPOPMIN returns (key, data, score).
                _, res, _ = self.conn.bzpopmin(
                    self.queue_key,
                    timeout=self.read_timeout)
            except (TimeoutError, TypeError, IndexError):
                # Unfortunately, there is no way to differentiate a socket
                # timing out and a host being unreachable. ConnectionError is
                # allowed to propagate, however, so the worker logs the error
                # and applies backoff, rather than busy-looping silently.
                return
            else:
                return res[8:]
        else:
            # ZPOPMIN returns a list of (data, score) 2-tuples.
            items = self.conn.zpopmin(self.queue_key, count=1)
            if items:
                return items[0][0][8:]  # [(prefix+data, score)].

    def queue_size(self):
        return self.conn.zcard(self.queue_key)

    def enqueued_items(self, limit=None):
        items = self.conn.zrange(self.queue_key, 0, limit - 1 if limit else -1)
        return [item[8:] for item in items]  # Unprefix the data.


class PriorityRedisStorage(RedisPriorityQueue, RedisStorage): pass


class PriorityRedisExpireStorage(RedisPriorityQueue, RedisExpireStorage): pass


class BaseSqlStorage(BaseStorage):
    begin_sql = 'begin'
    ddl = []

    def __init__(self, *args, **kwargs):
        create_tables = kwargs.pop('create_tables', True)
        super(BaseSqlStorage, self).__init__(*args, **kwargs)
        self.lock = threading.Lock()
        self._conn = None
        if create_tables:
            self.initialize_schema()

    def close(self):
        if self._conn is None:
            return False
        with self.lock:
            self._conn.close()
            self._conn = None
        return True

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self._create_connection()
        return self._conn

    def _create_connection(self):
        raise NotImplementedError

    @contextlib.contextmanager
    def db(self, commit=False, close=False):
        with self.lock:
            conn = self.conn
            cursor = conn.cursor()
            try:
                if commit: cursor.execute(self.begin_sql)
                yield cursor
            except Exception:
                if commit: conn.rollback()
                raise
            else:
                if commit: conn.commit()
            finally:
                cursor.close()
                if close:
                    conn.close()
                    self._conn = None

    def initialize_schema(self):
        with self.db(commit=True, close=True) as curs:
            for sql in self.ddl:
                curs.execute(sql)

    def sql(self, query, params=None, commit=False, results=False):
        with self.db(commit=commit) as curs:
            curs.execute(query, params or ())
            if results:
                return curs.fetchall()


class SqliteStorage(BaseSqlStorage):
    begin_sql = 'begin exclusive'
    integrity_error = getattr(sqlite3, 'IntegrityError', None)
    sqlite_version_info = getattr(sqlite3, 'sqlite_version_info', None)
    supports_result_ttl = True
    table_kv = ('create table if not exists kv ('
                'queue text not null, key text not null, value blob not null, '
                'expires real, is_result integer not null default 0, '
                'primary key(queue, key))')
    # Databases created before result TTL support lack the kv.expires column.
    # Rows present after the migration have expires=NULL and are treated as
    # having no expiration, i.e. old result data remains readable.
    table_kv_migrate = 'alter table kv add column expires real'
    # Rows written by the first TTL-enabled release lacked the is_result
    # marker. After adding the column they default to 0 (metadata), which is
    # the safe choice: such rows are never reclaimed by result cleanup.
    table_kv_migrate_result_flag = 'alter table kv add column is_result '
    'integer not null default 0'
    table_sched = ('create table if not exists schedule ('
                   'id integer not null primary key, queue text not null, '
                   'data blob not null, timestamp real not null)')
    index_sched = ('create index if not exists schedule_queue_timestamp '
                   'on schedule (queue, timestamp)')
    table_task = ('create table if not exists task ('
                  'id integer not null primary key, queue text not null, '
                  'data blob not null, priority real not null default 0.0)')
    index_task = ('create index if not exists task_queue_priority_id on task '
                  '(queue, priority desc, id)')
    drop_index_task = 'drop index if exists task_priority_id'  # Old index.
    table_counter = ('create table if not exists counter ('
                     'queue text not null, key text not null, '
                     'value integer not null default 0, '
                     'primary key(queue, key))')
    ddl = [table_kv, table_sched, index_sched, table_task, index_task,
           table_counter, drop_index_task]

    def __init__(self, name='huey', filename='huey.db', cache_mb=8,
                 fsync=None, journal_mode='wal', timeout=5, strict_fifo=False,
                 create_tables=True, result_ttl=None, time_function=None,
                 **kwargs):
        self.filename = filename
        self._cache_mb = cache_mb
        self._fsync = fsync
        self._journal_mode = journal_mode
        self._timeout = timeout  # Busy timeout in seconds, default is 5.
        self._conn_kwargs = kwargs
        # Absolute wall-clock timestamps are stored so that TTLs work across
        # multiple storage instances sharing a database file. The clock may be
        # overridden for deterministic, controlled-clock testing.
        self.time = time_function or time.time

        # By default Sqlite may reuse rowids when rows are removed. This means
        # that SqliteHuey may not strictly be a FIFO. If strict FIFO ordering
        # is needed, then we will utilize Sqlite's AUTOINCREMENT functionality,
        # which prevents deleted rowids from being reused.
        # NOTE: changing an existing database is not supported, so you will
        # need to delete and re-create it to change this value.
        if strict_fifo:
            ddl = list(self.ddl)
            ddl[3] = self.table_task.replace(
                'primary key',
                'primary key autoincrement')
            self.ddl = ddl

        self.to_blob = memoryview

        super(SqliteStorage, self).__init__(
            name, result_ttl=result_ttl, create_tables=create_tables)

    def initialize_schema(self):
        with self.db(commit=True, close=True) as curs:
            for sql in self.ddl:
                curs.execute(sql)
            # Migrate kv tables created by older huey releases that do not
            # have the expires column. Existing rows keep NULL expiration.
            curs.execute('pragma table_info(kv)')
            columns = [row[1] for row in curs.fetchall()]
            if columns and 'expires' not in columns:
                curs.execute(self.table_kv_migrate)
            if columns and 'is_result' not in columns:
                curs.execute(self.table_kv_migrate_result_flag)

    def _create_connection(self):
        conn = sqlite3.connect(self.filename, timeout=self._timeout,
                               check_same_thread=False,
                               **self._conn_kwargs)
        conn.isolation_level = None  # Autocommit mode.
        conn.execute('pragma journal_mode="%s"' % self._journal_mode)
        if self._cache_mb:
            conn.execute('pragma cache_size=%s' % (-1000 * self._cache_mb))
        if self._fsync is not None:
            conn.execute('pragma synchronous=%s' % (2 if self._fsync else 0))
        return conn

    def enqueue(self, data, priority=None):
        self.sql('insert into task (queue, data, priority) values (?, ?, ?)',
                 (self.name, self.to_blob(data), priority or 0), commit=True)

    def dequeue(self):
        # Quick check without the full exclusive lock.
        if not self.sql('select 1 from task where queue = ? limit 1',
                        (self.name,), results=True):
            return

        with self.db(commit=True) as curs:
            curs.execute('select id, data from task where queue = ? '
                         'order by priority desc, id limit 1', (self.name,))
            result = curs.fetchone()
            if result is not None:
                tid, data = result
                curs.execute('delete from task where id = ?', (tid,))
                if curs.rowcount == 1:
                    return data

    def queue_size(self):
        return self.sql('select count(id) from task where queue=?',
                        (self.name,), results=True)[0][0]

    def enqueued_items(self, limit=None):
        sql = 'select data from task where queue=? order by priority desc, id'
        params = (self.name,)
        if limit is not None:
            sql += ' limit ?'
            params = (self.name, limit)

        return [i for i, in self.sql(sql, params, results=True)]

    def flush_queue(self):
        self.sql('delete from task where queue=?', (self.name,), commit=True)

    def add_to_schedule(self, data, ts):
        params = (self.name, self.to_blob(data), ts.timestamp())
        self.sql('insert into schedule (queue, data, timestamp) '
                 'values (?, ?, ?)', params, commit=True)

    def read_schedule(self, ts):
        with self.db(commit=True) as curs:
            params = (self.name, ts.timestamp())
            curs.execute('select id, data from schedule where '
                         'queue = ? and timestamp <= ? order by timestamp, id',
                         params)
            id_list, data = [], []
            for task_id, task_data in curs.fetchall():
                id_list.append(task_id)
                data.append(task_data)
            for i in range(0, len(id_list), 500):
                chunk = id_list[i:i + 500]
                curs.execute('delete from schedule where id in (%s)' %
                             ','.join('?' * len(chunk)), chunk)
            return data

    def schedule_size(self):
        return self.sql('select count(id) from schedule where queue=?',
                        (self.name,), results=True)[0][0]

    def scheduled_items(self, limit=None):
        sql = 'select data from schedule where queue=? order by timestamp'
        params = (self.name,)
        if limit is not None:
            sql += ' limit ?'
            params = (self.name, limit)

        return [i for i, in self.sql(sql, params, results=True)]

    def flush_schedule(self):
        self.sql('delete from schedule where queue = ?', (self.name,), True)

    def _expiry(self, is_result, ttl):
        if is_result and ttl is None:
            ttl = self.result_ttl
        if not is_result or ttl is None:
            return None
        return self.time() + ttl

    def put_data(self, key, value, is_result=False, ttl=None):
        expires = self._expiry(is_result, ttl)
        self.sql('insert or replace into kv (queue, key, value, expires, '
                 'is_result) values (?, ?, ?, ?, ?)',
                 (self.name, key, self.to_blob(value), expires,
                  1 if is_result else 0), True)

    def peek_data(self, key):
        res = self.sql('select value, expires from kv where queue = ? and '
                       'key = ?', (self.name, key), results=True)
        if not res:
            return EmptyData
        value, expires = res[0]
        if expires is not None and expires <= self.time():
            # Expired results are unavailable and reading them must not
            # renew their TTL, so remove the row.
            self.sql('delete from kv where queue = ? and key = ?',
                     (self.name, key), True)
            return EmptyData
        return value

    def peek_many(self, keys):
        accum = {}
        now = self.time()
        for i in range(0, len(keys), 500):
            chunk = keys[i:i + 500]
            rows = self.sql(
                'select key, value, expires from kv where queue = ? and '
                'key in (%s)' % ','.join('?' * len(chunk)),
                (self.name,) + tuple(chunk), results=True)
            for key, value, expires in rows:
                if expires is not None and expires <= now:
                    continue
                accum[key] = value
        return accum

    def pop_data(self, key):
        with self.db(commit=True) as curs:
            now = self.time()
            if self.sqlite_version_info >= (3, 35, 0):
                curs.execute('delete from kv where queue = ? and key = ? and '
                             '(expires is null or expires > ?) returning '
                             'value', (self.name, key, now))
                result = curs.fetchone()
                if result is not None:
                    return result[0]
                # Remove the expired row if it exists.
                curs.execute('delete from kv where queue = ? and key = ?',
                             (self.name, key))
            else:
                curs.execute('select value, expires from kv where '
                             'queue = ? and key = ?', (self.name, key))
                result = curs.fetchone()
                if result is not None:
                    value, expires = result
                    if expires is not None and expires <= now:
                        curs.execute('delete from kv where queue=? and key=?',
                                     (self.name, key))
                        return EmptyData
                    curs.execute('delete from kv where queue=? and key=?',
                                 (self.name, key))
                    if curs.rowcount == 1:
                        return value
            return EmptyData

    def has_data_for_key(self, key):
        return bool(self.sql('select 1 from kv where queue=? and key=? and '
                             '(expires is null or expires > ?)',
                             (self.name, key, self.time()), results=True))

    def put_if_empty(self, key, value, ttl=None):
        expires = None if ttl is None else self.time() + ttl
        try:
            with self.db(commit=True) as curs:
                # An expired row (e.g. an expired lock) must not block the
                # conditional insert.
                curs.execute('delete from kv where queue = ? and key = ? and '
                             'expires is not null and expires <= ?',
                             (self.name, key, self.time()))
                curs.execute('insert or abort into kv '
                             '(queue, key, value, expires, is_result) values'
                             ' (?, ?, ?, ?, 0)',
                             (self.name, key, self.to_blob(value), expires))
        except self.integrity_error:
            return False
        else:
            return True

    def incr(self, key, amount=1):
        with self.db(commit=True) as curs:
            if self.sqlite_version_info >= (3, 35, 0):
                curs.execute('insert into counter (queue, key, value) '
                             'values (?, ?, ?) on conflict (queue, key) '
                             'do update set value = value + ? '
                             'returning value',
                             (self.name, key, amount, amount))
                value, = curs.fetchone()
            elif self.sqlite_version_info >= (3, 24, 0):
                curs.execute('insert into counter (queue, key, value) '
                             'values (?, ?, ?) on conflict (queue, key) '
                             'do update set value = value + ?',
                             (self.name, key, amount, amount))
                curs.execute('select value from counter '
                             'where queue = ? and key = ?',
                             (self.name, key))
                value, = curs.fetchone()
            else:
                raise NotImplementedError('SQLite 3.24 or newer is required.')

        return value

    def delete_counter(self, key):
        self.sql('delete from counter where queue = ? and key = ?',
                 (self.name, key), commit=True)

    def result_store_size(self):
        self.cleanup_results()
        return self.sql('select count(*) from kv where queue=?', (self.name,),
                        results=True)[0][0]

    def result_items(self):
        self.cleanup_results()
        res = self.sql('select key, value from kv where queue=?', (self.name,),
                       results=True)
        return dict((k, v) for k, v in res)

    def cleanup_results(self, limit=None):
        # Expiration is tracked in the kv table and applies only to task
        # results; revocation keys, locks and chord data are written with a
        # NULL expires and are unaffected.
        now = self.time()
        if self.sqlite_version_info >= (3, 35, 0):
            with self.db(commit=True) as curs:
                if limit is None:
                    curs.execute('delete from kv where queue = ? and expires '
                                 'is not null and expires <= ? and is_result '
                                 '= 1 returning 1',
                                 (self.name, now))
                else:
                    curs.execute('delete from kv where rowid in (select '
                                 'rowid from kv where queue = ? and expires is'
                                 ' not null and expires <= ? and is_result ='
                                 ' 1 limit ?) '
                                 'returning 1', (self.name, now, limit))
                return len(curs.fetchall())
        with self.db(commit=True) as curs:
            query = ('select rowid from kv where queue = ? and expires is not'
                     ' null and expires <= ? and is_result = 1')
            params = [self.name, now]
            if limit is not None:
                query += ' limit ?'
                params.append(limit)
            curs.execute(query, params)
            rowids = [row[0] for row in curs.fetchall()]
            if rowids:
                for i in range(0, len(rowids), 500):
                    chunk = rowids[i:i + 500]
                    curs.execute('delete from kv where rowid in (%s)' %
                                 ','.join('?' * len(chunk)), chunk)
            return len(rowids)

    def flush_results(self):
        self.sql('delete from kv where queue=?', (self.name,), True)

    def flush_counters(self):
        self.sql('delete from counter where queue=?', (self.name,), True)


class CySqliteStorage(SqliteStorage):
    def __init__(self, name='huey', filename='huey.db', pragmas=None,
                 timeout=5, strict_fifo=False, create_tables=True,
                 result_ttl=None, time_function=None, **kwargs):
        if cysqlite is None:
            raise ConfigurationError('"cysqlite" not found. Run "pip install '
                                     'cysqlite" to install.')
        self.integrity_error = cysqlite.IntegrityError
        self.sqlite_version_info = cysqlite.sqlite_version_info

        # Normalize hard-coded params to generic pragmas.
        pragmas = dict(pragmas or {})
        pragmas.setdefault('journal_mode', 'wal')
        if 'journal_mode' in kwargs:
            pragmas['journal_mode'] = kwargs.pop('journal_mode') or 'wal'
        if 'cache_mb' in kwargs:
            pragmas['cache_size'] = kwargs.pop('cache_mb') * -1000
        if 'fsync' in kwargs:
            pragmas['synchronous'] = 2 if kwargs.pop('fsync') else 0

        super(CySqliteStorage, self).__init__(
            name,
            filename,
            timeout=timeout,
            strict_fifo=strict_fifo,
            create_tables=create_tables,
            result_ttl=result_ttl,
            time_function=time_function,
            pragmas=pragmas,
            **kwargs)

    def _create_connection(self):
        return cysqlite.connect(self.filename, timeout=self._timeout,
                                **self._conn_kwargs)


class PostgresStorage(BaseSqlStorage):
    def __init__(self, name='huey', dsn=None, connection=None, blocking=True,
                read_timeout=1, table_prefix='huey', create_tables=True,
                result_ttl=None, **connection_params):
        if psycopg is None:
            raise ConfigurationError('"psycopg" (version 3.2 or newer) not '
                                     'found, cannot use Postgres storage '
                                     'backend. Run "pip install psycopg" to '
                                     'install.')
        self.dsn = dsn
        self.connection = connection  # Zero-arg callable returning conn.
        self.blocking = blocking
        self.read_timeout = read_timeout
        self.connection_params = connection_params

        prefix = re.sub('[^A-Za-z0-9_]', '', table_prefix)
        self.table_kv = prefix + '_kv'
        self.table_schedule = prefix + '_schedule'
        self.table_task = prefix + '_task'
        self.table_counter = prefix + '_counter'

        # Postgres channel names longer than 63 bytes raise "channel name
        # too long" from pg_notify(), which would break every enqueue.
        channel = '%s.q.%s' % (prefix, name)
        if len(channel.encode('utf-8')) > 63:
            digest = hashlib.md5(channel.encode('utf-8')).hexdigest()
            channel = 'huey.q.%s' % digest
        self.channel = channel

        self.ddl = [q.format(p=prefix) for q in (
            'create table if not exists {p}_kv ('
            'queue text not null, key text not null, value bytea not null, '
            'primary key(queue, key))',

            'create table if not exists {p}_schedule ('
            'id bigserial primary key, queue text not null, '
            'data bytea not null, timestamp double precision not null)',

            'create index if not exists {p}_schedule_queue_timestamp '
            'on {p}_schedule (queue, timestamp)',

            'create table if not exists {p}_task ('
            'id bigserial primary key, queue text not null, '
            'data bytea not null, '
            'priority double precision not null default 0.0)',

            'create index if not exists {p}_task_queue_priority_id '
            'on {p}_task (queue, priority desc, id)',

            'create table if not exists {p}_counter ('
            'queue text not null, key text not null, '
            'value bigint not null default 0, primary key(queue, key))')]

        # Do not reuse conns across fork!
        self._inherited = []
        self._conn_pid = None

        # Each worker thread gets its own LISTEN connection on first
        # dequeue. A dead thread's connection is released by GC: psycopg
        # only sends the protocol Terminate from the creating process, so
        # this is safe on both sides of a fork.
        self._listen_local = threading.local()

        super(PostgresStorage, self).__init__(
            name, result_ttl=result_ttl, create_tables=create_tables)

    def _connect(self):
        if self.connection is not None:
            conn = self.connection()
        else:
            conn = psycopg.connect(self.dsn or '', **self.connection_params)
        conn.autocommit = True
        return conn
    _create_connection = _connect

    @property
    def conn(self):
        if self._conn is not None:
            if self._conn_pid != os.getpid():
                self._inherited.append(self._conn)
                self._conn = None
            elif self._conn.closed or self._conn.broken:
                self._close_quiet(self._conn)
                self._conn = None
        if self._conn is None:
            self._conn = self._connect()
            self._conn_pid = os.getpid()
        return self._conn

    def _close_quiet(self, conn):
        try:
            conn.close()
        except Exception:
            pass

    def close(self):
        local = self._listen_local
        conn = getattr(local, 'conn', None)
        if conn is not None:
            if local.pid == os.getpid():
                self._close_quiet(conn)
            else:
                self._inherited.append(conn)
            local.conn = None
        return super(PostgresStorage, self).close()

    def _listen_conn(self):
        local = self._listen_local
        conn = getattr(local, 'conn', None)
        if conn is not None and (local.pid != os.getpid() or conn.closed or
                                 conn.broken):
            if local.pid != os.getpid():
                self._inherited.append(conn)
            else:
                self._close_quiet(conn)
            conn = local.conn = None
        if conn is None:
            conn = self._connect()
            conn.execute('listen "%s"' % self.channel.replace('"', '""'))
            local.conn, local.pid = conn, os.getpid()
        return conn

    def enqueue(self, data, priority=None):
        with self.db(commit=True) as curs:
            curs.execute('insert into {} (queue, data, priority) '
                         'values (%s, %s, %s)'.format(self.table_task),
                         (self.name, data, priority or 0))
            curs.execute('select pg_notify(%s, %s)', (self.channel, ''))

    def _dequeue(self):
        with self.db() as curs:
            curs.execute('delete from {t} where id = ('
                         'select id from {t} where queue = %s '
                         'order by priority desc, id limit 1 '
                         'for update skip locked) '
                         'returning data'.format(t=self.table_task),
                         (self.name,))
            row = curs.fetchone()
        if row is not None:
            return bytes(row[0])

    def dequeue(self):
        data = self._dequeue()
        if data is not None or not self.blocking:
            return data

        conn = self._listen_conn()
        deadline = time.monotonic() + self.read_timeout
        while True:
            timeout = deadline - time.monotonic()
            if timeout <= 0:
                return None
            if not list(conn.notifies(timeout=timeout, stop_after=1)):
                return None
            data = self._dequeue()
            if data is not None:
                return data  # Otherwise another worker won, keep waiting.

    def queue_size(self):
        return self.sql('select count(*) from {} where queue = %s'.format(
            self.table_task), (self.name,), results=True)[0][0]

    def enqueued_items(self, limit=None):
        sql = ('select data from {} where queue = %s '
               'order by priority desc, id'.format(self.table_task))
        params = (self.name,)
        if limit is not None:
            sql += ' limit %s'
            params = (self.name, limit)

        return [bytes(i) for i, in self.sql(sql, params, results=True)]

    def flush_queue(self):
        self.sql('delete from {} where queue = %s'.format(self.table_task),
                 (self.name,))

    def add_to_schedule(self, data, ts):
        self.sql('insert into {} (queue, data, timestamp) '
                 'values (%s, %s, %s)'.format(self.table_schedule),
                 (self.name, data, ts.timestamp()))

    def read_schedule(self, ts):
        with self.db() as curs:
            curs.execute('delete from {t} where id in ('
                         'select id from {t} where queue = %s and '
                         'timestamp <= %s for update skip locked) '
                         'returning timestamp, id, data'.format(
                             t=self.table_schedule),
                         (self.name, ts.timestamp()))
            rows = curs.fetchall()
        return [bytes(data) for _, _, data in
                sorted(rows, key=lambda row: row[:2])]

    def schedule_size(self):
        return self.sql('select count(*) from {} where queue = %s'.format(
            self.table_schedule), (self.name,), results=True)[0][0]

    def scheduled_items(self, limit=None):
        sql = ('select data from {} where queue = %s '
               'order by timestamp'.format(self.table_schedule))
        params = (self.name,)
        if limit is not None:
            sql += ' limit %s'
            params = (self.name, limit)

        return [bytes(i) for i, in self.sql(sql, params, results=True)]

    def flush_schedule(self):
        self.sql('delete from {} where queue = %s'.format(
            self.table_schedule), (self.name,))

    def _key(self, key):
        return key.decode('utf-8') if isinstance(key, bytes) else key

    def put_data(self, key, value, is_result=False, ttl=None):
        if ttl is not None:
            raise NotImplementedError(
                'per-result TTL is not supported by this storage.')
        self.sql('insert into {} (queue, key, value) values (%s, %s, %s) '
                 'on conflict (queue, key) do update set '
                 'value = excluded.value'.format(self.table_kv),
                 (self.name, self._key(key), value))

    def peek_data(self, key):
        res = self.sql('select value from {} where queue = %s and '
                       'key = %s'.format(self.table_kv),
                       (self.name, self._key(key)), results=True)
        return bytes(res[0][0]) if res else EmptyData

    def peek_many(self, keys):
        res = self.sql('select key, value from {} where queue = %s and '
                       'key = any(%s)'.format(self.table_kv),
                       (self.name, [self._key(k) for k in keys]), results=True)
        return dict((k, bytes(v)) for k, v in res)

    def pop_data(self, key):
        with self.db() as curs:
            curs.execute('delete from {} where queue = %s and key = %s '
                         'returning value'.format(self.table_kv),
                         (self.name, self._key(key)))
            row = curs.fetchone()
        return bytes(row[0]) if row is not None else EmptyData

    def has_data_for_key(self, key):
        return bool(self.sql('select 1 from {} where queue = %s and '
                             'key = %s'.format(self.table_kv),
                             (self.name, self._key(key)), results=True))

    def put_if_empty(self, key, value, ttl=None):
        if ttl is not None:
            raise NotImplementedError('ttl is not supported by this storage.')
        with self.db() as curs:
            curs.execute('insert into {} (queue, key, value) '
                         'values (%s, %s, %s) '
                         'on conflict do nothing'.format(self.table_kv),
                         (self.name, self._key(key), value))
            return curs.rowcount == 1

    def incr(self, key, amount=1):
        with self.db() as curs:
            curs.execute('insert into {t} as c (queue, key, value) '
                         'values (%s, %s, %s) '
                         'on conflict (queue, key) do update set '
                         'value = c.value + excluded.value '
                         'returning value'.format(t=self.table_counter),
                         (self.name, self._key(key), amount))
            return curs.fetchone()[0]

    def delete_counter(self, key):
        self.sql('delete from {} where queue = %s and key = %s'.format(
            self.table_counter), (self.name, self._key(key)))

    def result_store_size(self):
        return self.sql('select count(*) from {} where queue = %s'.format(
            self.table_kv), (self.name,), results=True)[0][0]

    def result_items(self):
        res = self.sql('select key, value from {} where queue = %s'.format(
            self.table_kv), (self.name,), results=True)
        return dict((k, bytes(v)) for k, v in res)

    def flush_results(self):
        self.sql('delete from {} where queue = %s'.format(self.table_kv),
                 (self.name,))

    def flush_counters(self):
        self.sql('delete from {} where queue = %s'.format(
            self.table_counter), (self.name,))


class FileStorage(BaseStorage):
    """
    Simple file-system storage implementation.

    This storage implementation should NOT be used in production as it utilizes
    exclusive locks around all file-system operations. This is done to prevent
    race-conditions when reading from the file-system.
    """
    MAX_PRIORITY = 0xffff

    def __init__(self, name, path, levels=2, use_thread_lock=False,
                 result_ttl=None, **storage_kwargs):
        super(FileStorage, self).__init__(name, result_ttl=result_ttl,
                                          **storage_kwargs)

        self.path = path
        if os.path.exists(self.path) and not os.path.isdir(self.path):
            raise ValueError('path "%s" is not a directory' % path)
        if levels < 0 or levels > 4:
            raise ValueError('%s levels must be between 0 and 4' % self)

        self.queue_path = os.path.join(self.path, 'queue')
        self.schedule_path = os.path.join(self.path, 'schedule')
        self.result_path = os.path.join(self.path, 'results')
        self.counter_path = os.path.join(self.path, 'counters')
        self.levels = levels

        if use_thread_lock:
            self.lock = threading.Lock()
        else:
            self.lock_file = os.path.join(self.path, '.lock')
            self.lock = FileLock(self.lock_file)

    def _flush_dir(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
            os.makedirs(path)

    def enqueue(self, data, priority=None):
        priority = int(priority or 0)
        if priority < 0: raise ValueError('priority must be a positive number')
        if priority > self.MAX_PRIORITY:
            raise ValueError('priority must be <= %s' % self.MAX_PRIORITY)

        with self.lock:
            if not os.path.exists(self.queue_path):
                os.makedirs(self.queue_path)

            # Encode the filename so that tasks are sorted by priority (desc) and
            # timestamp (asc).
            prefix = '%04x-%012x' % (
                self.MAX_PRIORITY - priority,
                int(time.time() * 1000))

            base = filename = os.path.join(self.queue_path, prefix)
            conflict = 0
            while os.path.exists(filename):
                conflict += 1
                filename = '%s.%03d' % (base, conflict)

            with open(filename, 'wb') as fh:
                fh.write(data)

    def _get_sorted_filenames(self, path):
        if not os.path.exists(path):
            return ()
        return [f for f in sorted(os.listdir(path)) if not f.endswith('.tmp')]

    def dequeue(self):
        with self.lock:
            filenames = self._get_sorted_filenames(self.queue_path)
            if not filenames:
                return

            filename = os.path.join(self.queue_path, filenames[0])
            tmp_dest = filename + '.tmp'
            os.rename(filename, tmp_dest)

            with open(tmp_dest, 'rb') as fh:
                data = fh.read()
            os.unlink(tmp_dest)
        return data

    def queue_size(self):
        return len(self._get_sorted_filenames(self.queue_path))

    def enqueued_items(self, limit=None):
        filenames = self._get_sorted_filenames(self.queue_path)[:limit]
        accum = []
        for filename in filenames:
            with open(os.path.join(self.queue_path, filename), 'rb') as fh:
                accum.append(fh.read())
        return accum

    def flush_queue(self):
        self._flush_dir(self.queue_path)

    def _timestamp_to_prefix(self, ts):
        ts = time.mktime(ts.timetuple()) + (ts.microsecond * 1e-6)
        return '%012x' % int(ts * 1000)

    def add_to_schedule(self, data, ts):
        with self.lock:
            if not os.path.exists(self.schedule_path):
                os.makedirs(self.schedule_path)

            ts_prefix = self._timestamp_to_prefix(ts)
            base = filename = os.path.join(self.schedule_path, ts_prefix)
            conflict = 0
            while os.path.exists(filename):
                conflict += 1
                filename = '%s.%03d' % (base, conflict)

            with open(filename, 'wb') as fh:
                fh.write(data)

    def read_schedule(self, ts):
        with self.lock:
            prefix = self._timestamp_to_prefix(ts)
            accum = []
            for basename in self._get_sorted_filenames(self.schedule_path):
                if basename[:12] > prefix:
                    break
                filename = os.path.join(self.schedule_path, basename)
                new_filename = filename + '.tmp'
                os.rename(filename, new_filename)
                accum.append(new_filename)

            tasks = []
            for filename in accum:
                with open(filename, 'rb') as fh:
                    tasks.append(fh.read())
                    os.unlink(filename)

        return tasks

    def schedule_size(self):
        return len(self._get_sorted_filenames(self.schedule_path))

    def scheduled_items(self, limit=None):
        filenames = self._get_sorted_filenames(self.schedule_path)[:limit]
        accum = []
        for filename in filenames:
            with open(os.path.join(self.schedule_path, filename), 'rb') as fh:
                accum.append(fh.read())
        return accum

    def flush_schedule(self):
        self._flush_dir(self.schedule_path)

    def path_for_key(self, key):
        if isinstance(key, str):
            key = key.encode('utf8')
        checksum = hashlib.md5(key).hexdigest()
        prefix = checksum[:self.levels]
        prefix_filename = itertools.chain(prefix, (checksum,))
        return os.path.join(self.result_path, *prefix_filename)

    def put_data(self, key, value, is_result=False, ttl=None):
        if ttl is not None:
            raise NotImplementedError(
                'per-result TTL is not supported by this storage.')
        with self.lock:
            self._put_data(key, value)

    def _put_data(self, key, value):
        # Write a key/value pair. The lock must already be held.
        if isinstance(key, str):
            key = key.encode('utf8')

        filename = self.path_for_key(key)
        dirname = os.path.dirname(filename)

        if not os.path.exists(dirname):
            os.makedirs(dirname)

        with open(filename, 'wb') as fh:
            key_len = len(key)
            fh.write(struct.pack('>I', key_len))
            fh.write(key)
            fh.write(value)

    def put_if_empty(self, key, value, ttl=None):
        if ttl is not None:
            raise NotImplementedError('ttl is not supported by this storage.')
        with self.lock:
            if os.path.exists(self.path_for_key(key)):
                return False
            self._put_data(key, value)
            return True

    def _unpack_result(self, data):
        key_len, = struct.unpack('>I', data[:4])
        key = data[4:4 + key_len]
        if len(key) != key_len:
            return None, None
        return key, data[4 + key_len:]

    def peek_data(self, key):
        filename = self.path_for_key(key)
        if not os.path.exists(filename):
            return EmptyData

        with open(filename, 'rb') as fh:
            _, value = self._unpack_result(fh.read())

        # If file is corrupt or has been tampered with, return EmptyData.
        return value if value is not None else EmptyData

    def pop_data(self, key):
        filename = self.path_for_key(key)

        with self.lock:
            if not os.path.exists(filename):
                return EmptyData

            with open(filename, 'rb') as fh:
                _, value = self._unpack_result(fh.read())

            os.unlink(filename)

        # If file is corrupt or has been tampered with, return EmptyData.
        return value if value is not None else EmptyData

    def has_data_for_key(self, key):
        return os.path.exists(self.path_for_key(key))

    def _counter_filename(self, key):
        if isinstance(key, str):
            key = key.encode('utf8')
        return os.path.join(self.counter_path, hashlib.md5(key).hexdigest())

    def incr(self, key, amount=1):
        filename = self._counter_filename(key)
        with self.lock:
            if not os.path.exists(self.counter_path):
                os.makedirs(self.counter_path)
            try:
                with open(filename, 'rt') as fh:
                    value = int(fh.read()) + amount
            except Exception:
                value = amount
            with open(filename, 'wt') as fh:
                fh.write(str(value))

        return value

    def delete_counter(self, key):
        filename = self._counter_filename(key)
        with self.lock:
            if os.path.exists(filename):
                os.unlink(filename)

    def result_store_size(self):
        return sum(len(filenames) for _, _, filenames
                   in os.walk(self.result_path))

    def result_items(self):
        accum = {}
        for root, _, filenames in os.walk(self.result_path):
            for filename in filenames:
                path = os.path.join(root, filename)
                with open(path, 'rb') as fh:
                    key, value = self._unpack_result(fh.read())
                if key is not None:
                    accum[key.decode('utf8')] = value
        return accum

    def flush_results(self):
        self._flush_dir(self.result_path)

    def flush_counters(self):
        self._flush_dir(self.counter_path)
