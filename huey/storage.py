from collections import deque
import base64
import contextlib
import hashlib
import heapq
import itertools
import json
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
import warnings

try:
    from redis import ConnectionPool
    try:
        from redis import StrictRedis as Redis
    except ImportError:
        from redis import Redis
    from redis.exceptions import ConnectionError
    from redis.exceptions import TimeoutError
except ImportError:
    ConnectionPool = Redis = ConnectionError = TimeoutError = None

from huey.constants import EmptyData
from huey.expiration import decode_result_meta
from huey.expiration import encode_result_meta
from huey.exceptions import ConfigurationError
from huey.utils import FileLock
from huey.utils import text_type
from huey.utils import to_timestamp


def _paginate_meta(sorted_items, cursor, limit):
    """
    Given (key, raw-meta) pairs sorted by key, return the segment strictly
    greater than "cursor", along with the cursor for the next segment.
    """
    if cursor is not None:
        sorted_items = [item for item in sorted_items if item[0] > cursor]
    next_cursor = None
    if limit is not None and len(sorted_items) > limit:
        next_cursor = sorted_items[limit - 1][0]
        sorted_items = sorted_items[:limit]
    return sorted_items, next_cursor


class BaseStorage(object):
    """
    Base storage-layer interface. Subclasses should implement all methods.
    """
    blocking = False  # Does dequeue() block until ready, or should we poll?
    priority = True

    def __init__(self, name='huey', **storage_kwargs):
        self.name = name

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

    def put_data(self, key, value, is_result=False):
        """
        Store an arbitrary key/value pair.

        :param bytes key: lookup key
        :param bytes value: value
        :param bool is_result: indicate if we are storing a (volatile) task
            result versus metadata like a task revocation key or lock.
        :return: No return value.
        """
        raise NotImplementedError

    def peek_data(self, key):
        """
        Non-destructively read the value at the given key, if it exists.

        :param bytes key: Key to read.
        :return: Associated value, if key exists, or ``EmptyData``.
        """
        raise NotImplementedError

    def pop_data(self, key):
        """
        Destructively read the value at the given key, if it exists.

        :param bytes key: Key to read.
        :return: Associated value, if key exists, or ``EmptyData``.
        """
        raise NotImplementedError

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

    def put_if_empty(self, key, value):
        """
        Atomically write data only if the key is not already set.

        :param bytes key: Key to check/set.
        :param bytes value: Arbitrary data.
        :return: Boolean whether key/value was set.
        """
        if self.has_data_for_key(key):
            return False
        self.put_data(key, value)
        return True

    def result_store_size(self):
        """
        :return: Number of key/value pairs in the result store.
        """
        raise NotImplementedError

    def result_items(self):
        """
        Non-destructively read all the key/value pairs from the data-store.

        :return: Dictionary mapping all key/value pairs in the data-store.
        """
        raise NotImplementedError

    def flush_results(self):
        """
        Delete all key/value pairs from the data-store.

        :return: No return value.
        """
        raise NotImplementedError

    # Whether the storage backend keeps result metadata (write timestamps,
    # result category, group references) alongside results. Backends that do
    # not support result metadata cannot participate in expiration cleanup;
    # for them the cleanup APIs are safe no-ops.
    supports_result_meta = False

    def put_result(self, key, value, meta=None, ttl=None):
        """
        Store a task result along with optional metadata.

        :param bytes key: lookup key (typically the task id).
        :param bytes value: serialized result value.
        :param dict meta: result metadata (see huey.expiration), which is
            used by the expiration cleanup to decide when the result may be
            deleted. Should be written atomically with the value whenever
            the backend allows it.
        :param float ttl: optional TTL in seconds, used only by backends
            with native expiration support. None means "no native expiry"
            (backend-specific default may still apply).
        :return: No return value.
        """
        self.put_data(key, value, is_result=True)
        if meta is not None:
            self.put_result_meta(key, meta)

    def put_result_meta(self, key, meta):
        """
        Store metadata for the given result-store key. Metadata-only entries
        (e.g. group bookkeeping) may not have an associated value.
        """
        pass

    def get_result_meta(self, key):
        """
        Return the decoded metadata dict for the given key, or None.
        """
        return None

    def iter_result_meta(self, cursor=None, limit=None):
        """
        Iterate over (key, raw-metadata) pairs in sorted-key order.

        :param cursor: opaque cursor (the last key of the previous segment);
            only keys greater than the cursor are returned.
        :param limit: maximum number of items to return, or None for all.
        :return: (items, next_cursor) where items is a list of (key, raw)
            tuples and next_cursor is None when iteration is complete.
        """
        return ([], None)

    def delete_result_if_matches(self, key, meta):
        """
        Atomically delete the result value and metadata for the given key,
        but only if the currently-stored metadata is identical to "meta"
        (a dict previously returned by get_result_meta/iter_result_meta).
        This prevents lost updates when a worker writes a fresh result
        concurrently with cleanup.

        :return: boolean indicating whether anything was deleted.
        """
        return False

    def flush_all(self):
        """
        Remove all persistent or semi-persistent data.

        :return: No return value.
        """
        self.flush_queue()
        self.flush_schedule()
        self.flush_results()


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
    def put_data(self, key, value, is_result=False): pass
    def peek_data(self, key): return EmptyData
    def pop_data(self, key): return EmptyData
    def has_data_for_key(self, key): return False
    def result_store_size(self): return 0
    def result_items(self): return {}
    def flush_results(self): pass


class MemoryStorage(BaseStorage):
    supports_result_meta = True

    def __init__(self, *args, **kwargs):
        super(MemoryStorage, self).__init__(*args, **kwargs)
        self._c = 0  # Counter to ensure FIFO behavior for queue.
        self._queue = []
        self._results = {}
        self._results_meta = {}
        self._schedule = []
        self._lock = threading.RLock()

    def enqueue(self, data, priority=None):
        with self._lock:
            self._c += 1
            priority = 0 if priority is None else -priority
            heapq.heappush(self._queue, (priority, self._c, data))

    def dequeue(self):
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
        items = sorted(data for _, data in self._schedule)
        if limit:
            items = items[:limit]
        return items

    def flush_schedule(self):
        self._schedule = []

    def put_data(self, key, value, is_result=False):
        self._results[key] = value

    def peek_data(self, key):
        return self._results.get(key, EmptyData)

    def pop_data(self, key):
        return self._results.pop(key, EmptyData)

    def has_data_for_key(self, key):
        return key in self._results

    def result_store_size(self):
        return len(self._results)

    def result_items(self):
        return dict(self._results)

    def put_result(self, key, value, meta=None, ttl=None):
        # Write the result and its metadata atomically under the lock so
        # that cleanup never observes a partially-written result.
        with self._lock:
            self._results[key] = value
            if meta is not None:
                self._results_meta[key] = encode_result_meta(meta)

    def put_result_meta(self, key, meta):
        with self._lock:
            self._results_meta[key] = encode_result_meta(meta)

    def get_result_meta(self, key):
        return decode_result_meta(self._results_meta.get(key))

    def iter_result_meta(self, cursor=None, limit=None):
        with self._lock:
            items = sorted(self._results_meta.items())
        return _paginate_meta(items, cursor, limit)

    def delete_result_if_matches(self, key, meta):
        with self._lock:
            if self._results_meta.get(key) != encode_result_meta(meta):
                return False
            self._results.pop(key, None)
            del self._results_meta[key]
            return True

    def flush_results(self):
        self._results = {}
        self._results_meta = {}


# A custom lua script to pass to redis that will read tasks from the schedule
# and atomically pop them from the sorted set and return them. It won't return
# anything if it isn't able to remove the items it reads.
SCHEDULE_POP_LUA = """\
local unix_ts = ARGV[1]
local res = redis.call('zrangebyscore', KEYS[1], '-inf', unix_ts)
if #res and redis.call('zremrangebyscore', KEYS[1], '-inf', unix_ts) == #res then
    return res
end"""

# Atomically delete a result and its metadata (both stored as fields in a
# hash), but only when the stored metadata matches the expected value. This
# ensures cleanup cannot delete a result that was concurrently re-written
# by a worker (lost update).
RESULT_DELETE_LUA = """\
if redis.call('hget', KEYS[2], ARGV[1]) == ARGV[2] then
    redis.call('hdel', KEYS[1], ARGV[1])
    redis.call('hdel', KEYS[2], ARGV[1])
    return 1
end
return 0"""

# Same as above, but for storage layouts that keep results and metadata in
# standalone keys (e.g. RedisExpireStorage).
RESULT_DELETE_KEY_LUA = """\
if redis.call('get', KEYS[2]) == ARGV[1] then
    redis.call('del', KEYS[1], KEYS[2])
    return 1
end
return 0"""


class RedisStorage(BaseStorage):
    priority = False  # Use PriorityRedisStorage instead. Requires Redis>=5.0.
    redis_client = Redis
    supports_result_meta = True

    def __init__(self, name='huey', blocking=True, read_timeout=1,
                 connection_pool=None, url=None, client_name=None,
                 **connection_params):

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
        self._del_result = self.conn.register_script(RESULT_DELETE_LUA)

        self.name = self.clean_name(name)
        self.queue_key = 'huey.redis.%s' % self.name
        self.schedule_key = 'huey.schedule.%s' % self.name
        self.result_key = 'huey.results.%s' % self.name
        self.error_key = 'huey.errors.%s' % self.name
        self.result_meta_key = 'huey.results.meta.%s' % self.name

        if client_name is not None:
            self.conn.client_setname(client_name)

        self.blocking = blocking
        self.read_timeout = read_timeout

    def clean_name(self, name):
        return re.sub('[^a-z0-9]', '', name)

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
            except (ConnectionError, TimeoutError, TypeError, IndexError):
                # Unfortunately, there is no way to differentiate a socket
                # timing out and a host being unreachable.
                return None
        else:
            return self.conn.rpop(self.queue_key)

    def queue_size(self):
        return self.conn.llen(self.queue_key)

    def enqueued_items(self, limit=None):
        limit = limit or -1
        return self.conn.lrange(self.queue_key, 0, limit)[::-1]

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
        limit = limit or -1
        return self.conn.zrange(self.schedule_key, 0, limit, withscores=False)

    def flush_schedule(self):
        self.conn.delete(self.schedule_key)

    def put_data(self, key, value, is_result=False):
        self.conn.hset(self.result_key, key, value)

    def peek_data(self, key):
        pipe = self.conn.pipeline()
        pipe.hexists(self.result_key, key)
        pipe.hget(self.result_key, key)
        exists, val = pipe.execute()
        return EmptyData if not exists else val

    def pop_data(self, key):
        pipe = self.conn.pipeline()
        pipe.hexists(self.result_key, key)
        pipe.hget(self.result_key, key)
        pipe.hdel(self.result_key, key)
        exists, val, n = pipe.execute()
        return EmptyData if not exists else val

    def has_data_for_key(self, key):
        return self.conn.hexists(self.result_key, key)

    def put_if_empty(self, key, value):
        return self.conn.hsetnx(self.result_key, key, value)

    def result_store_size(self):
        return self.conn.hlen(self.result_key)

    def result_items(self):
        return self.conn.hgetall(self.result_key)

    def put_result(self, key, value, meta=None, ttl=None):
        # NOTE: Redis hashes do not support per-field expiration, so the
        # TTL is not applied natively here. Expiration is handled by the
        # cleanup sweeper (Huey.cleanup_expired_results).
        if meta is None:
            self.conn.hset(self.result_key, key, value)
        else:
            pipe = self.conn.pipeline()
            pipe.hset(self.result_key, key, value)
            pipe.hset(self.result_meta_key, key, encode_result_meta(meta))
            pipe.execute()

    def put_result_meta(self, key, meta):
        self.conn.hset(self.result_meta_key, key, encode_result_meta(meta))

    def get_result_meta(self, key):
        return decode_result_meta(self.conn.hget(self.result_meta_key, key))

    def iter_result_meta(self, cursor=None, limit=None):
        items = sorted(self.conn.hgetall(self.result_meta_key).items())
        if isinstance(cursor, str):
            cursor = cursor.encode('utf8')
        return _paginate_meta(items, cursor, limit)

    def delete_result_if_matches(self, key, meta):
        return bool(self._del_result(
            keys=[self.result_key, self.result_meta_key],
            args=[key, encode_result_meta(meta)]))

    def flush_results(self):
        self.conn.delete(self.result_key, self.result_meta_key)


class RedisExpireStorage(RedisStorage):
    # Redis storage subclass that adds expiration to task result values. Since
    # the Redis server handles deleting our results after the expiration time,
    # this storage layer will not delete the results when they are read.
    def __init__(self, name='huey', expire_time=86400, *args, **kwargs):
        super(RedisExpireStorage, self).__init__(name, *args, **kwargs)

        self._expire_time = expire_time

        encode = lambda s: s if isinstance(s, bytes) else s.encode('utf8')
        self.result_prefix = rp = b'huey.r.%s.' % self.name.encode('utf8')
        self.result_key = lambda k: rp + encode(k)
        mp = b'huey.rm.%s.' % self.name.encode('utf8')
        self.result_meta_prefix = mp
        self.result_meta_key = lambda k: mp + encode(k)
        self._del_result = self.conn.register_script(RESULT_DELETE_KEY_LUA)

    def put_data(self, key, value, is_result=False):
        if is_result and self._expire_time:
            # We only want to expire task result data. If we are storing an
            # important metadata like a revocation key, we need to preserve it.
            self.conn.setex(self.result_key(key), self._expire_time, value)
        else:
            self.conn.set(self.result_key(key), value)

    def put_result(self, key, value, meta=None, ttl=None):
        # When the expiration policy provides a TTL it takes precedence over
        # the storage-level default expire_time. A TTL of None means the
        # policy does not restrict the lifetime, so the storage default is
        # used. A TTL of 0 cannot be applied natively (Redis requires a
        # positive expiry), so the result is stored without native expiry
        # and is removed by the next cleanup pass instead.
        expire = self._expire_time if ttl is None else ttl
        pipe = self.conn.pipeline()
        if expire:
            pipe.setex(self.result_key(key), int(expire), value)
        else:
            pipe.set(self.result_key(key), value)
        if meta is not None:
            encoded = encode_result_meta(meta)
            if expire:
                pipe.setex(self.result_meta_key(key), int(expire), encoded)
            else:
                pipe.set(self.result_meta_key(key), encoded)
        pipe.execute()

    def put_result_meta(self, key, meta):
        self.conn.set(self.result_meta_key(key), encode_result_meta(meta))

    def get_result_meta(self, key):
        return decode_result_meta(self.conn.get(self.result_meta_key(key)))

    def _result_meta_keys(self):
        return self.conn.scan_iter(match=self.result_meta_prefix + b'*')

    def iter_result_meta(self, cursor=None, limit=None):
        keys = sorted(self._result_meta_keys())
        if isinstance(cursor, str):
            cursor = cursor.encode('utf8')
        items = []
        if keys:
            pfx_len = len(self.result_meta_prefix)
            for key, raw in zip(keys, self.conn.mget(keys)):
                items.append((key[pfx_len:], raw))
        return _paginate_meta(items, cursor, limit)

    def delete_result_if_matches(self, key, meta):
        return bool(self._del_result(
            keys=[self.result_key(key), self.result_meta_key(key)],
            args=[encode_result_meta(meta)]))

    def peek_data(self, key):
        pipe = self.conn.pipeline()
        pipe.exists(self.result_key(key))
        pipe.get(self.result_key(key))
        exists, val = pipe.execute()
        return EmptyData if not exists else val

    # Here we explicitly prevent result items from being removed by using the
    # same implementation for "pop" (get and delete) as we do for "peek"
    # (non-destructive read).
    pop_data = peek_data

    def delete_data(self, key):
        return self.conn.delete(self.result_key(key))

    def has_data_for_key(self, key):
        return self.conn.exists(self.result_key(key)) != 0

    def put_if_empty(self, key, value):
        return self.conn.setnx(self.result_key(key), value)

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
                accum[key[pfx_len:]] = value
        return accum

    def flush_results(self):
        keys = list(self._result_keys())
        keys += list(self._result_meta_keys())
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
            except (ConnectionError, TimeoutError, TypeError, IndexError):
                # Unfortunately, there is no way to differentiate a socket
                # timing out and a host being unreachable.
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
        items = self.conn.zrange(self.queue_key, 0, limit or -1)
        return [item[8:] for item in items]  # Unprefix the data.


class PriorityRedisStorage(RedisPriorityQueue, RedisStorage): pass


class PriorityRedisExpireStorage(RedisPriorityQueue, RedisExpireStorage): pass


class _ConnectionState(object):
    def __init__(self, **kwargs):
        super(_ConnectionState, self).__init__(**kwargs)
        self.reset()
    def reset(self):
        self.conn = None
        self.closed = True
    def set_connection(self, conn):
        self.conn = conn
        self.closed = False
class _ConnectionLocal(_ConnectionState, threading.local): pass

# Python 2.x may return <buffer> object for BLOB columns.
to_bytes = lambda b: bytes(b) if not isinstance(b, bytes) else b
to_blob = lambda b: sqlite3.Binary(b)


class BaseSqlStorage(BaseStorage):
    begin_sql = 'begin'
    ddl = []

    def __init__(self, *args, **kwargs):
        super(BaseSqlStorage, self).__init__(*args, **kwargs)
        self._state = _ConnectionLocal()
        self.initialize_schema()

    def close(self):
        if self._state.closed: return False
        self._state.conn.close()
        self._state.reset()
        return True

    @property
    def conn(self):
        if self._state.closed:
            self._state.set_connection(self._create_connection())
        return self._state.conn

    def _create_connection(self):
        raise NotImplementedError

    @contextlib.contextmanager
    def db(self, commit=False, close=False):
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
                self._state.reset()

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
    supports_result_meta = True
    table_kv = ('create table if not exists kv ('
                'queue text not null, key text not null, value blob not null, '
                'primary key(queue, key))')
    table_sched = ('create table if not exists schedule ('
                   'id integer not null primary key, queue text not null, '
                   'data blob not null, timestamp real not null)')
    index_sched = ('create index if not exists schedule_queue_timestamp '
                   'on schedule (queue, timestamp)')
    table_task = ('create table if not exists task ('
                  'id integer not null primary key, queue text not null, '
                  'data blob not null, priority real not null default 0.0)')
    index_task = ('create index if not exists task_priority_id on task '
                  '(priority desc, id asc)')

    table_kv_meta = ('create table if not exists kv_meta ('
                     'queue text not null, key text not null, '
                     'meta text not null, primary key(queue, key))')
    ddl = [table_kv, table_sched, index_sched, table_task, index_task,
           table_kv_meta]

    def __init__(self, name='huey', filename='huey.db', cache_mb=8,
                 fsync=False, journal_mode='wal', timeout=5, strict_fifo=False,
                 **kwargs):
        self.filename = filename
        self._cache_mb = cache_mb
        self._fsync = fsync
        self._journal_mode = journal_mode
        self._timeout = timeout  # Busy timeout in seconds, default is 5.
        self._conn_kwargs = kwargs

        # By default Sqlite may reuse rowids when rows are removed. This means
        # that SqliteHuey may not strictly be a FIFO. If strict FIFO ordering
        # is needed, then we will utilize Sqlite's AUTOINCREMENT functionality,
        # which prevents deleted rowids from being reused.
        # NOTE: changing an existing database is not supported, so you will
        # need to delete and re-create it to change this value.
        if strict_fifo:
            self.ddl[3] = self.table_task.replace(
                'primary key',
                'primary key autoincrement')

        super(SqliteStorage, self).__init__(name)

    def _create_connection(self):
        conn = sqlite3.connect(self.filename, timeout=self._timeout,
                               **self._conn_kwargs)
        conn.isolation_level = None  # Autocommit mode.
        conn.execute('pragma journal_mode="%s"' % self._journal_mode)
        if self._cache_mb:
            conn.execute('pragma cache_size=%s' % (-1000 * self._cache_mb))
        conn.execute('pragma synchronous=%s' % (2 if self._fsync else 0))
        return conn

    def enqueue(self, data, priority=None):
        self.sql('insert into task (queue, data, priority) values (?, ?, ?)',
                 (self.name, to_blob(data), priority or 0), commit=True)

    def dequeue(self):
        with self.db(commit=True) as curs:
            curs.execute('select id, data from task where queue = ? '
                         'order by priority desc, id limit 1', (self.name,))
            result = curs.fetchone()
            if result is not None:
                tid, data = result
                curs.execute('delete from task where id = ?', (tid,))
                if curs.rowcount == 1:
                    return to_bytes(data)

    def queue_size(self):
        return self.sql('select count(id) from task where queue=?',
                        (self.name,), results=True)[0][0]

    def enqueued_items(self, limit=None):
        sql = 'select data from task where queue=? order by priority desc, id'
        params = (self.name,)
        if limit is not None:
            sql += ' limit ?'
            params = (self.name, limit)

        return [to_bytes(i) for i, in self.sql(sql, params, results=True)]

    def flush_queue(self):
        self.sql('delete from task where queue=?', (self.name,), commit=True)

    def add_to_schedule(self, data, ts):
        params = (self.name, to_blob(data), to_timestamp(ts))
        self.sql('insert into schedule (queue, data, timestamp) '
                 'values (?, ?, ?)', params, commit=True)

    def read_schedule(self, ts):
        with self.db(commit=True) as curs:
            params = (self.name, to_timestamp(ts))
            curs.execute('select id, data from schedule where '
                         'queue = ? and timestamp <= ?', params)
            id_list, data = [], []
            for task_id, task_data in curs.fetchall():
                id_list.append(task_id)
                data.append(to_bytes(task_data))
            if id_list:
                plist = ','.join('?' * len(id_list))
                curs.execute('delete from schedule where id IN (%s)' % plist,
                             id_list)
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

        return [to_bytes(i) for i, in self.sql(sql, params, results=True)]

    def flush_schedule(self):
        self.sql('delete from schedule where queue = ?', (self.name,), True)

    def put_data(self, key, value, is_result=False):
        self.sql('insert or replace into kv (queue, key, value) '
                 'values (?, ?, ?)', (self.name, key, to_blob(value)), True)

    def peek_data(self, key):
        res = self.sql('select value from kv where queue = ? and key = ?',
                       (self.name, key), results=True)
        return to_bytes(res[0][0]) if res else EmptyData

    def pop_data(self, key):
        with self.db(commit=True) as curs:
            curs.execute('select value from kv where queue = ? and key = ?',
                         (self.name, key))
            result = curs.fetchone()
            if result is not None:
                curs.execute('delete from kv where queue=? and key=?',
                             (self.name, key))
                if curs.rowcount == 1:
                    return to_bytes(result[0])
            return EmptyData

    def has_data_for_key(self, key):
        return bool(self.sql('select 1 from kv where queue=? and key=?',
                             (self.name, key), results=True))

    def put_if_empty(self, key, value):
        try:
            with self.db(commit=True) as curs:
                curs.execute('insert or abort into kv '
                             '(queue, key, value) values (?, ?, ?)',
                             (self.name, key, to_blob(value)))
        except sqlite3.IntegrityError:
            return False
        else:
            return True

    def result_store_size(self):
        return self.sql('select count(*) from kv where queue=?', (self.name,),
                        results=True)[0][0]

    def result_items(self):
        res = self.sql('select key, value from kv where queue=?', (self.name,),
                       results=True)
        return dict((k, to_bytes(v)) for k, v in res)

    def put_result(self, key, value, meta=None, ttl=None):
        # The result and its metadata are written in a single transaction
        # so cleanup never observes a partially-written result.
        with self.db(commit=True) as curs:
            curs.execute('insert or replace into kv (queue, key, value) '
                         'values (?, ?, ?)', (self.name, key, to_blob(value)))
            if meta is not None:
                curs.execute('insert or replace into kv_meta '
                             '(queue, key, meta) values (?, ?, ?)',
                             (self.name, key, encode_result_meta(meta)))

    def put_result_meta(self, key, meta):
        self.sql('insert or replace into kv_meta (queue, key, meta) '
                 'values (?, ?, ?)',
                 (self.name, key, encode_result_meta(meta)), commit=True)

    def get_result_meta(self, key):
        res = self.sql('select meta from kv_meta where queue=? and key=?',
                       (self.name, key), results=True)
        return decode_result_meta(res[0][0]) if res else None

    def iter_result_meta(self, cursor=None, limit=None):
        sql = 'select key, meta from kv_meta where queue=?'
        params = [self.name]
        if cursor is not None:
            sql += ' and key > ?'
            params.append(cursor)
        sql += ' order by key'
        if limit is not None:
            sql += ' limit ?'
            params.append(limit + 1)
        rows = self.sql(sql, params, results=True)
        next_cursor = None
        if limit is not None and len(rows) > limit:
            next_cursor = rows[limit - 1][0]
            rows = rows[:limit]
        return ([(key, meta) for key, meta in rows], next_cursor)

    def delete_result_if_matches(self, key, meta):
        # The check and the delete run in a single exclusive transaction,
        # so a concurrent worker write cannot be lost.
        with self.db(commit=True) as curs:
            curs.execute('select meta from kv_meta where queue=? and key=?',
                         (self.name, key))
            row = curs.fetchone()
            if row is None or row[0] != encode_result_meta(meta):
                return False
            curs.execute('delete from kv where queue=? and key=?',
                         (self.name, key))
            curs.execute('delete from kv_meta where queue=? and key=?',
                         (self.name, key))
            return True

    def flush_results(self):
        with self.db(commit=True) as curs:
            curs.execute('delete from kv where queue=?', (self.name,))
            curs.execute('delete from kv_meta where queue=?', (self.name,))


class FileStorage(BaseStorage):
    """
    Simple file-system storage implementation.

    This storage implementation should NOT be used in production as it utilizes
    exclusive locks around all file-system operations. This is done to prevent
    race-conditions when reading from the file-system.
    """
    MAX_PRIORITY = 0xffff

    def __init__(self, name, path, levels=2, use_thread_lock=False,
                 **storage_kwargs):
        super(FileStorage, self).__init__(name, **storage_kwargs)

        self.path = path
        if os.path.exists(self.path) and not os.path.isdir(self.path):
            raise ValueError('path "%s" is not a directory' % path)
        if levels < 0 or levels > 4:
            raise ValueError('%s levels must be between 0 and 4' % self)

        self.queue_path = os.path.join(self.path, 'queue')
        self.schedule_path = os.path.join(self.path, 'schedule')
        self.result_path = os.path.join(self.path, 'results')
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
        priority = priority or 0
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
        if isinstance(key, text_type):
            key = key.encode('utf8')
        checksum = hashlib.md5(key).hexdigest()
        prefix = checksum[:self.levels]
        prefix_filename = itertools.chain(prefix, (checksum,))
        return os.path.join(self.result_path, *prefix_filename)

    def put_data(self, key, value, is_result=False):
        if isinstance(key, text_type):
            key = key.encode('utf8')

        filename = self.path_for_key(key)
        dirname = os.path.dirname(filename)

        with self.lock:
            if not os.path.exists(dirname):
                os.makedirs(dirname)

            with open(self.path_for_key(key), 'wb') as fh:
                key_len = len(key)
                fh.write(struct.pack('>I', key_len))
                fh.write(key)
                fh.write(value)

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
                accum[key] = value
        return accum

    def flush_results(self):
        self._flush_dir(self.result_path)
