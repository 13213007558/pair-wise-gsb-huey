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
from huey.exceptions import ConfigurationError
from huey.utils import FileLock
from huey.utils import text_type
from huey.utils import to_timestamp


class BaseStorage(object):
    """
    Base storage-layer interface. Subclasses should implement all methods.
    """
    blocking = False  # Does dequeue() block until ready, or should we poll?
    priority = True

    # Default upper bound on how many effective-priority levels a waiting
    # task can gain through aging. Guarantees that effective priority stays
    # bounded even for tasks that have waited a very long time.
    aging_max_priority_boost = 1000

    def __init__(self, name='huey', aging_step=None, aging_threshold=0,
                 aging_max_boost=None, **storage_kwargs):
        self.name = name

        # Optional priority aging. When disabled (the default), ordering is
        # purely by the static task priority. When enabled, tasks gain
        # effective priority the longer they wait:
        #
        #   boost = min(aging_max_boost,
        #               floor(max(0, wait - aging_threshold) / aging_step))
        #   effective_priority = static_priority + boost
        #
        # Effective priority is always derived from the fixed enqueue time
        # and the current time -- it is never written back to the queue, so
        # a task that is not selected does not cause the ordering of other
        # tasks to change as it grows older (other than the intended aging).
        self.aging_step = None if aging_step is None else float(aging_step)
        self.aging_threshold = float(aging_threshold)
        if aging_max_boost is None:
            aging_max_boost = self.aging_max_priority_boost
        self.aging_max_boost = int(aging_max_boost)
        self._validate_aging_config()

    @property
    def aging(self):
        return self.aging_step is not None

    def _validate_aging_config(self):
        if self.aging_step is not None and self.aging_step <= 0:
            raise ValueError('aging_step must be a positive number of '
                             'seconds, got %r' % self.aging_step)
        if self.aging_threshold < 0:
            raise ValueError('aging_threshold must not be negative, got %r'
                             % self.aging_threshold)
        if self.aging_max_boost < 0:
            raise ValueError('aging_max_boost must not be negative, got %r'
                             % self.aging_max_boost)

    def now(self):
        """Current wall-clock time as epoch seconds.

        Backends use this to derive effective priority at dequeue time. It
        can be overridden (e.g. in tests) to inject a controlled clock.
        """
        return time.time()

    def _validate_priority(self, priority):
        """Validate a user-provided priority value.

        Negative priorities are allowed (they sort below the default of 0),
        but booleans, NaN and infinity are rejected when priority aging is
        enabled -- otherwise they could corrupt the ordering arithmetic.
        """
        if priority is None:
            return
        if isinstance(priority, bool) or not isinstance(priority,
                                                        (int, float)):
            raise TypeError('priority must be a number, got %r' %
                            type(priority).__name__)
        if self.aging and (priority != priority or priority in
                           (float('inf'), float('-inf'))):
            raise ValueError('priority must be finite, got %r' % priority)

    def aged_priority(self, priority, enqueue_ts, now=None):
        """Derive the effective priority from wait time.

        Returns the effective priority as a float. Callers are expected to
        break ties with a stable FIFO value (enqueue sequence or the
        persisted enqueue timestamp), never with derived effective priority
        that gets written back.
        """
        if now is None:
            now = self.now()
        if self.aging_step is None:
            return float(priority or 0)
        # Clamp the wait at zero so a backwards clock jump can never demote a
        # task below its static priority, and cap the boost so very long
        # waits cannot produce unbounded effective priorities.
        wait = now - enqueue_ts - self.aging_threshold
        if wait < 0:
            boost = 0
        else:
            boost = min(self.aging_max_boost, int(wait / self.aging_step))
        return float(priority or 0) + boost

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
    def __init__(self, *args, **kwargs):
        super(MemoryStorage, self).__init__(*args, **kwargs)
        self._c = 0  # Counter to ensure FIFO behavior for queue.
        # Without aging the queue is a heap keyed by static priority. With
        # aging enabled, effective priority is a function of time so each
        # dequeue derives ordering by scanning items stored as
        # (priority, enqueue_ts, seq, data). No effective priority is ever
        # written back.
        self._queue = []
        self._results = {}
        self._schedule = []
        self._lock = threading.RLock()

    def enqueue(self, data, priority=None):
        self._validate_priority(priority)
        with self._lock:
            self._c += 1
            if self.aging:
                self._queue.append((float(priority or 0), self.now(),
                                    self._c, data))
            else:
                priority = 0 if priority is None else -priority
                heapq.heappush(self._queue, (priority, self._c, data))

    def dequeue(self):
        with self._lock:
            if not self._queue:
                return
            if self.aging:
                now = self.now()
                # Derive ordering from wait time -- never from list position.
                # Equal effective priority falls back to FIFO by sequence.
                idx = min(range(len(self._queue)),
                          key=lambda i: (-self.aged_priority(
                                             self._queue[i][0],
                                             self._queue[i][1], now),
                                         self._queue[i][2]))
                _, _, _, data = self._queue.pop(idx)
                return data
            try:
                _, _, data = heapq.heappop(self._queue)
            except IndexError:
                pass
            else:
                return data

    def queue_size(self):
        return len(self._queue)

    def enqueued_items(self, limit=None):
        if self.aging:
            now = self.now()
            ordered = sorted(
                self._queue,
                key=lambda item: (-self.aged_priority(item[0], item[1], now),
                                  item[2]))
            items = [item[3] for item in ordered]
        else:
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

    def flush_results(self):
        self._results = {}


# A custom lua script to pass to redis that will read tasks from the schedule
# and atomically pop them from the sorted set and return them. It won't return
# anything if it isn't able to remove the items it reads.
SCHEDULE_POP_LUA = """\
local unix_ts = ARGV[1]
local res = redis.call('zrangebyscore', KEYS[1], '-inf', unix_ts)
if #res and redis.call('zremrangebyscore', KEYS[1], '-inf', unix_ts) == #res then
    return res
end"""

# Priority-aging queue, backed by a sorted-set. The member carries the
# enqueue time (epoch seconds from the Redis server clock), a monotonic FIFO
# sequence number and the static priority; the score only acts as a hint for
# BZPOPMIN readiness notifications. All aging arithmetic happens server-side
# at dequeue time and is never written back to the queue.
#
# KEYS[1] = queue sorted-set, KEYS[2] = monotonic FIFO sequence counter.
# ARGV[1] = task data, ARGV[2] = static priority, ARGV[3] = score hint.
Z_ENQUEUE_LUA = """\
local now = redis.call('TIME')
local enq = tonumber(now[1])
local seq = redis.call('INCR', KEYS[2])
local member = string.format('%012d:%020d:%s:%s', enq, seq, ARGV[2], ARGV[1])
redis.call('ZADD', KEYS[1], tonumber(ARGV[3]), member)
return 1"""

# Derive the effective-priority winner from wait time. Effective priority is
# computed on every call; nothing is persisted, so repeated dequeue attempts
# cannot cause ordering drift for the remaining items. Equal effective
# priority falls back to the monotonic sequence (FIFO).
# ARGV: step, threshold, max-boost, mode (remove|pop), popped member, popped
# member score. In "pop" mode a BZPOPMIN hint member was already removed by
# the caller: the script first restores it atomically, then scans and removes
# the actual winner, so concurrent consumers can each receive a different
# task exactly once even when the hint was the only remaining member.
Z_DEQUEUE_LUA = """\
local queue = KEYS[1]
local nowt = redis.call('TIME')
local now = tonumber(nowt[1]) + tonumber(nowt[2]) / 1000000.0
local step = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
local maxboost = tonumber(ARGV[3])
local mode = ARGV[4]
if mode == 'pop' then
    local popped = ARGV[5]
    if #popped > 0 then
        redis.call('ZADD', queue, tonumber(ARGV[6]), popped)
    end
end
local members = redis.call('ZRANGE', queue, 0, -1)
local candidate = false
local candeff = nil
local candseq = nil
for i = 1, #members do
    local member = members[i]
    local first = string.find(member, ':', 1, true)
    local second = string.find(member, ':', first + 1, true)
    local third = string.find(member, ':', second + 1, true)
    local ts = tonumber(string.sub(member, 1, first - 1))
    local seq = tonumber(string.sub(member, first + 1, second - 1))
    local pri = tonumber(string.sub(member, second + 1, third - 1))
    local wait = now - ts - threshold
    local boost = 0
    if wait > 0 then
        boost = math.floor(wait / step)
        if boost > maxboost then boost = maxboost end
    end
    local eff = pri + boost
    if (not candidate) or eff > candeff or (eff == candeff and seq < candseq) then
        candidate = member
        candeff = eff
        candseq = seq
    end
end
if not candidate then return false end
redis.call('ZREM', queue, candidate)
return candidate"""

# Non-destructively read tasks in effective-priority order.
# ARGV: step, threshold, max-boost, limit (-1 for all).
Z_PEEK_LUA = """\
local queue = KEYS[1]
local nowt = redis.call('TIME')
local now = tonumber(nowt[1]) + tonumber(nowt[2]) / 1000000.0
local step = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
local maxboost = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
local members = redis.call('ZRANGE', queue, 0, -1)
local ranked = {}
for i = 1, #members do
    local member = members[i]
    local first = string.find(member, ':', 1, true)
    local second = string.find(member, ':', first + 1, true)
    local third = string.find(member, ':', second + 1, true)
    local ts = tonumber(string.sub(member, 1, first - 1))
    local seq = tonumber(string.sub(member, first + 1, second - 1))
    local pri = tonumber(string.sub(member, second + 1, third - 1))
    local wait = now - ts - threshold
    local boost = 0
    if wait > 0 then
        boost = math.floor(wait / step)
        if boost > maxboost then boost = maxboost end
    end
    ranked[#ranked + 1] = {member, pri + boost, seq}
end
table.sort(ranked, function(a, b)
    if a[2] ~= b[2] then return a[2] > b[2] end
    return a[3] < b[3]
end)
local res = {}
for i = 1, #ranked do
    if limit >= 0 and i > limit then break end
    res[#res + 1] = ranked[i][1]
end
return res"""


class RedisStorage(BaseStorage):
    priority = False  # Use PriorityRedisStorage instead. Requires Redis>=5.0.
    redis_client = Redis

    def __init__(self, name='huey', blocking=True, read_timeout=1,
                 connection_pool=None, url=None, client_name=None,
                 aging_step=None, aging_threshold=0, aging_max_boost=None,
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

        self.name = self.clean_name(name)
        self.queue_key = 'huey.redis.%s' % self.name
        self.queue_seq_key = 'huey.redis.%s.seq' % self.name
        self.schedule_key = 'huey.schedule.%s' % self.name
        self.result_key = 'huey.results.%s' % self.name
        self.error_key = 'huey.errors.%s' % self.name

        if client_name is not None:
            self.conn.client_setname(client_name)

        self.blocking = blocking
        self.read_timeout = read_timeout

        # Configure optional priority aging. When enabled the queue is stored
        # in a sorted-set instead of a list and all ordering is derived
        # server-side from the Redis clock (see the Lua scripts above).
        super(RedisStorage, self).__init__(
            self.name, aging_step=aging_step,
            aging_threshold=aging_threshold,
            aging_max_boost=aging_max_boost)
        if self.aging:
            self._z_enqueue = self.conn.register_script(Z_ENQUEUE_LUA)
            self._z_dequeue = self.conn.register_script(Z_DEQUEUE_LUA)
            self._z_peek = self.conn.register_script(Z_PEEK_LUA)

    def clean_name(self, name):
        return re.sub('[^a-z0-9]', '', name)

    def _aging_args(self):
        return (self.aging_step, self.aging_threshold, self.aging_max_boost)

    @staticmethod
    def _unprefix_aging_member(member):
        # Member layout: enqueued_ts:seq:priority:data. Find the third colon
        # so that data bytes following the prefix are returned untouched.
        first = member.index(b':')
        second = member.index(b':', first + 1)
        third = member.index(b':', second + 1)
        return member[third + 1:]

    def convert_ts(self, ts):
        return time.mktime(ts.timetuple()) + (ts.microsecond * 1e-6)

    def enqueue(self, data, priority=None):
        if self.aging:
            self._validate_priority(priority)
            # Score is only a readiness hint (smallest score pops first):
            # real ordering is derived from wait time inside the script.
            score = 0 if priority is None else -float(priority)
            self._z_enqueue(
                keys=[self.queue_key, self.queue_seq_key],
                args=[data, priority or 0, score])
            return
        if priority:
            raise NotImplementedError('Task priorities are not supported by '
                                      'this storage.')
        self.conn.lpush(self.queue_key, data)

    def dequeue(self):
        if self.aging:
            if self.blocking:
                while True:
                    args = list(self._aging_args())
                    try:
                        # Use BZPOPMIN purely to sleep until data may be
                        # available; the script re-checks state atomically and
                        # restores the hint member when aging selects another
                        # task, so concurrent consumers never duplicate or
                        # lose tasks.
                        hint = self.conn.bzpopmin(
                            self.queue_key, timeout=self.read_timeout)
                    except (ConnectionError, TimeoutError, TypeError,
                            IndexError):
                        return
                    if not hint:
                        return
                    _, member, score = hint
                    args.extend(['pop', member, score])
                    try:
                        winner = self._z_dequeue(keys=[self.queue_key],
                                                args=args)
                    except (ConnectionError, TimeoutError):
                        return
                    if winner is not False and winner is not None:
                        return self._unprefix_aging_member(winner)
                    # Race: another consumer drained the queue between
                    # BZPOPMIN and the script. Wait for new data.
            else:
                args = list(self._aging_args())
                args.extend(['remove', b'', 0.0])
                winner = self._z_dequeue(keys=[self.queue_key], args=args)
                if winner is False or winner is None:
                    return
                return self._unprefix_aging_member(winner)
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
        if self.aging:
            return self.conn.zcard(self.queue_key)
        return self.conn.llen(self.queue_key)

    def enqueued_items(self, limit=None):
        if self.aging:
            args = list(self._aging_args()) + [(limit if limit is not None
                                               else -1)]
            items = self._z_peek(keys=[self.queue_key], args=args)
            return [self._unprefix_aging_member(item) for item in items]
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

    def flush_results(self):
        self.conn.delete(self.result_key)


class RedisExpireStorage(RedisStorage):
    # Redis storage subclass that adds expiration to task result values. Since
    # the Redis server handles deleting our results after the expiration time,
    # this storage layer will not delete the results when they are read.
    def __init__(self, name='huey', expire_time=86400, *args, **kwargs):
        super(RedisExpireStorage, self).__init__(name, *args, **kwargs)

        self._expire_time = expire_time

        self.result_prefix = rp = b'huey.r.%s.' % self.name.encode('utf8')
        encode = lambda s: s if isinstance(s, bytes) else s.encode('utf8')
        self.result_key = lambda k: rp + encode(k)

    def put_data(self, key, value, is_result=False):
        if is_result:
            # We only want to expire task result data. If we are storing an
            # important metadata like a revocation key, we need to preserve it.
            self.conn.setex(self.result_key(key), self._expire_time, value)
        else:
            self.conn.set(self.result_key(key), value)

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
        if keys:
            self.conn.delete(*keys)


class RedisPriorityQueue(object):
    priority = True

    def enqueue(self, data, priority=None):
        if self.aging:
            return RedisStorage.enqueue(self, data, priority)
        priority = 0 if priority is None else -priority
        # Prefix the message with an encoded timestamp to ensure that messages
        # created with the same priority are stored in the correct order. Since
        # the underlying data-type is a sorted-set, this also prevents multiple
        # identical messages, except they are enqueued on the same microsecond,
        # from being treated as a single item.
        prefix = struct.pack('>Q', int(time.time() * 1e6))
        self.conn.zadd(self.queue_key, {prefix + data: priority})

    def dequeue(self):
        if self.aging:
            return RedisStorage.dequeue(self)
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
        if self.aging:
            return RedisStorage.queue_size(self)
        return self.conn.zcard(self.queue_key)

    def enqueued_items(self, limit=None):
        if self.aging:
            return RedisStorage.enqueued_items(self, limit)
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
                  'data blob not null, priority real not null default 0.0, '
                  'enqueued_ts real not null default 0.0)')
    index_task = ('create index if not exists task_priority_id on task '
                  '(priority desc, id asc)')
    ddl = [table_kv, table_sched, index_sched, table_task, index_task]

    def __init__(self, name='huey', filename='huey.db', cache_mb=8,
                 fsync=False, journal_mode='wal', timeout=5, strict_fifo=False,
                 aging_step=None, aging_threshold=0, aging_max_boost=None,
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

        super(SqliteStorage, self).__init__(
            name, aging_step=aging_step,
            aging_threshold=aging_threshold,
            aging_max_boost=aging_max_boost)

        # Migrate databases created before priority aging was introduced:
        # add the persisted enqueue timestamp column. Existing rows receive a
        # timestamp of zero, so they are treated as having waited forever and
        # are eligible for aging immediately; a clock jump can never demote
        # them below their static priority.
        columns = self.sql('pragma table_info(task)', results=True)
        if columns and not any(col[1] == 'enqueued_ts' for col in columns):
            self.sql('alter table task add column enqueued_ts real not null '
                     'default 0.0', commit=True)

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
        self._validate_priority(priority)
        if self.aging:
            self.sql('insert into task (queue, data, priority, enqueued_ts) '
                     'values (?, ?, ?, ?)',
                     (self.name, to_blob(data), priority or 0, self.now()),
                     commit=True)
        else:
            self.sql('insert into task (queue, data, priority) values '
                     '(?, ?, ?)', (self.name, to_blob(data), priority or 0),
                     commit=True)

    def _aged_ordering(self, now):
        # Effective priority is derived purely from the persisted enqueue
        # time and the current time -- nothing is rewritten on dequeue. Tasks
        # at equal effective priority retain FIFO by id.
        # The 1.0 factor forces floating-point division (SQLite truncates
        # integer/integer).
        boost = ('min(%s, cast(max(0, cast((%s - enqueued_ts - %s) * 1.0 / %s '
                 'as integer)) as real))' % (
                     self.aging_max_boost, now, self.aging_threshold,
                     self.aging_step))
        return 'priority + ' + boost

    def dequeue(self):
        if self.aging:
            with self.db(commit=True) as curs:
                ordering = self._aged_ordering('?')
                curs.execute('select id, data from task where queue = ? '
                             'order by (%s) desc, id limit 1' % ordering,
                             (self.name, self.now()))
                result = curs.fetchone()
                if result is not None:
                    tid, data = result
                    curs.execute('delete from task where id = ?', (tid,))
                    if curs.rowcount == 1:
                        return to_bytes(data)
            return
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
        if self.aging:
            sql = ('select data from task where queue=? order by (%s) desc, '
                   'id' % self._aged_ordering('?'))
            params = (self.name, self.now())
        else:
            sql = ('select data from task where queue=? order by priority '
                   'desc, id')
            params = (self.name,)
        if limit is not None:
            sql += ' limit ?'
            params = params + (limit,)

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

    def flush_results(self):
        self.sql('delete from kv where queue=?', (self.name,), True)


class FileStorage(BaseStorage):
    """
    Simple file-system storage implementation.

    This storage implementation should NOT be used in production as it utilizes
    exclusive locks around all file-system operations. This is done to prevent
    race-conditions when reading from the file-system.
    """
    aging = False  # File-based queue does not support priority aging.

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
