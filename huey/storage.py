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
    # Whether the storage provides the atomic primitives required to run a
    # chord safely under at-least-once message delivery (see the chord_*
    # methods below). Backends that cannot perform the member-claim and
    # finalize steps atomically must leave this False so that huey refuses to
    # run a chord rather than silently counting a redelivered member twice.
    supports_chord_dedup = False

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

    def put_data(self, key, value, is_result=False):
        """
        Store an arbitrary key/value pair, overwrites any existing value.

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
        :param int ttl: Seconds until the key expires. Supported by the redis
            storages and ignored by memory storage. Others raise
            ``NotImplementedError``. With ``RedisStorage`` the server must
            support hash-field TTL (redis 7.4+ or valkey 9+).
            ``RedisExpireStorage`` works on any version.
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

    # Chord completion bookkeeping. These methods provide the member-identity
    # deduplication needed to make chord completion safe when the queue may
    # redeliver a member task. Three key namespaces are used per chord:
    #
    # * the member result keys hold the one accepted terminal value per member
    # * the member claim keys mark which members have already contributed
    # * the finalize key is a tombstone that marks the chord as finalized.
    #
    # The claim and finalize checks are each performed atomically, so a
    # redelivered completion (success, permanent failure or revocation) can
    # contribute at most once, and a completion arriving after finalization
    # cannot re-create the chord or enqueue the callback a second time.

    def chord_member_complete(self, claim_key, result_key, value,
                              finalize_key, ttl=None):
        """
        Record the terminal value of one chord member, but only if this member
        has not already contributed and the chord has not been finalized.

        The membership check, value write and completion count increment must
        happen atomically. Returns a boolean indicating whether this report was
        accepted. A False return means either a duplicate report for the same
        member, or a late report for an already-finalized chord; in both cases
        the stored value and counter are left untouched.
        """
        raise NotImplementedError

    def chord_count(self, finalize_key):
        """
        Return the number of distinct members that have contributed, or None if
        the chord is already finalized (and its bookkeeping has been removed).
        """
        raise NotImplementedError

    def chord_finalize(self, finalize_key, ttl=None):
        """
        Mark the chord as finalized, atomically. Returns True only for the
        caller that wins the finalization race; every other caller gets False.
        Once set, the tombstone causes all further member reports to be
        rejected until it is cleared.
        """
        raise NotImplementedError

    def chord_read_results(self, result_keys):
        """
        Non-destructively read the accepted member values in order.
        """
        raise NotImplementedError

    def chord_clear(self, claim_keys, result_keys, finalize_key):
        """
        Remove member values and the completion counter for a finalized chord.
        The claim markers and finalize tombstone are intentionally left in
        place (for their TTL where supported) so late duplicate completions
        continue to be rejected.
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
    # The black-hole storage is stateless and discards everything, so every
    # report is trivially accepted exactly once and nothing is ever finalized.
    supports_chord_dedup = True

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
    def put_if_empty(self, key, value, ttl=None): return True
    def incr(self, key, amount=1): return amount
    def delete_counter(self, key): pass
    def result_store_size(self): return 0
    def result_items(self): return {}
    def flush_results(self): pass
    def flush_counters(self): pass

    def chord_member_complete(self, claim_key, result_key, value,
                              finalize_key, ttl=None):
        return True
    def chord_count(self, finalize_key):
        return None
    def chord_finalize(self, finalize_key, ttl=None):
        return True
    def chord_read_results(self, result_keys):
        return {}
    def chord_clear(self, claim_keys, result_keys, finalize_key):
        pass


class MemoryStorage(BaseStorage):
    supports_chord_dedup = True
    # How long a finalized chord's tombstone is retained so that late,
    # redelivered completions keep being rejected.
    chord_tombstone_ttl = 7 * 24 * 60 * 60

    def __init__(self, *args, **kwargs):
        super(MemoryStorage, self).__init__(*args, **kwargs)
        self._c = 0  # Counter to ensure FIFO behavior for queue.
        self._queue = []
        self._results = {}
        self._schedule = []
        self._counters = {}
        # chord id -> {'claims': set of member indexes, 'count': int,
        #              'tombstone': monotonic expiry timestamp or None}.
        # Member values themselves live in the regular result store.
        self._chords = {}
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

    def put_data(self, key, value, is_result=False):
        self._results[key] = value

    def peek_data(self, key):
        return self._results.get(key, EmptyData)

    def pop_data(self, key):
        return self._results.pop(key, EmptyData)

    def has_data_for_key(self, key):
        return key in self._results

    def put_if_empty(self, key, value, ttl=None):
        with self._lock:
            if key in self._results:
                return False
            self._results[key] = value
            return True

    def incr(self, key, amount=1):
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + amount
        return self._counters[key]

    def delete_counter(self, key):
        with self._lock:
            self._counters.pop(key, None)

    def result_store_size(self):
        return len(self._results)

    def result_items(self):
        return dict(self._results)

    def flush_results(self):
        self._results = {}

    def flush_counters(self):
        self._counters = {}
        # Chord bookkeeping lives alongside the counters and must be reset as
        # well when an application asks for a clean counter store.
        self._chords = {}

    # The chord keys passed by the api layer are namespaced as
    # "chord:<cid>:..." so the chord id can always be recovered.
    def _chord_id(self, key):
        return key.split(':', 2)[1]

    def _chord_expired(self, state):
        expiry = state.get('tombstone')
        return expiry is not None and expiry <= time.monotonic()

    def _get_chord(self, cid):
        state = self._chords.get(cid)
        if state is not None and self._chord_expired(state):
            # An expired tombstone means the storage has forgotten this
            # chord, so any stray report must start from a clean slate.
            state = None
        if state is None:
            state = {'claims': set(), 'count': 0, 'tombstone': None}
            self._chords[cid] = state
        return state

    def chord_member_complete(self, claim_key, result_key, value,
                              finalize_key, ttl=None):
        cid = self._chord_id(claim_key)
        idx = claim_key.rsplit(':', 1)[1]
        with self._lock:
            state = self._get_chord(cid)
            if state['tombstone'] is not None or idx in state['claims']:
                # Finalized chord or duplicate report for the same member.
                return False

            state['claims'].add(idx)
            state['count'] += 1
            self._results[result_key] = value
            return True

    def chord_count(self, finalize_key):
        cid = self._chord_id(finalize_key)
        with self._lock:
            state = self._chords.get(cid)
            if state is None or self._chord_expired(state):
                return None
            # A finalized chord no longer keeps a live completion count; its
            # tombstone is what late reports consult.
            if state['tombstone'] is not None:
                return None
            return state['count']

    def chord_finalize(self, finalize_key, ttl=None):
        cid = self._chord_id(finalize_key)
        ttl = self.chord_tombstone_ttl if ttl is None else ttl
        with self._lock:
            state = self._get_chord(cid)
            if state['tombstone'] is not None:
                return False
            state['tombstone'] = time.monotonic() + ttl
            return True

    def chord_read_results(self, result_keys):
        with self._lock:
            return {key: self._results[key] for key in result_keys
                    if key in self._results}

    def chord_clear(self, claim_keys, result_keys, finalize_key):
        cid = self._chord_id(finalize_key)
        with self._lock:
            for key in result_keys:
                self._results.pop(key, None)
            state = self._chords.get(cid)
            if state is not None:
                # Reset the count and drop member values, but keep the claims
                # and tombstone so late duplicates stay rejected.
                state['count'] = 0


# A custom lua script to pass to redis that will read tasks from the schedule
# and atomically pop them from the sorted set and return them. It won't return
# anything if it isn't able to remove the items it reads.
SCHEDULE_POP_LUA = """\
local unix_ts = tonumber(ARGV[1])
local res = redis.call('zrangebyscore', KEYS[1], '-inf', unix_ts)
if #res and redis.call('zremrangebyscore', KEYS[1], '-inf', unix_ts) == #res then
    return res
end"""

# Atomically accept one chord member completion only if the member has not
# contributed yet and the chord has not been finalized. Keys are the result
# hash, counter hash, claim key, result key and finalize key. Returns the new
# distinct-member count, or 0 when the report was rejected.
CHORD_MEMBER_LUA = """\
local finalize = ARGV[1]
local claim = ARGV[2]
local rkey = ARGV[3]
local rval = ARGV[4]
if redis.call('hexists', KEYS[1], finalize) == 1 then
    return 0
end
if redis.call('hexists', KEYS[1], claim) == 1 then
    return 0
end
redis.call('hset', KEYS[1], claim, '1')
redis.call('hset', KEYS[1], rkey, rval)
return redis.call('hincrby', KEYS[2], finalize, 1)"""

# Atomically write the finalize tombstone unless it already exists. Returns 1
# when the caller won finalization, 0 otherwise.
CHORD_FINALIZE_LUA = """\
if redis.call('hsetnx', KEYS[1], ARGV[1], '1') == 0 then
    return 0
end
return 1"""

# Variants used by RedisExpireStorage, where every key/value pair and counter
# lives in its own top-level key.
CHORD_MEMBER_EXPIRE_LUA = """\
local finalize = ARGV[1]
local claim = ARGV[2]
local rkey = ARGV[3]
local rval = ARGV[4]
local ttl = tonumber(ARGV[5])
if redis.call('exists', finalize) == 1 then
    return 0
end
if redis.call('exists', claim) == 1 then
    return 0
end
redis.call('set', claim, '1')
redis.call('set', rkey, rval, 'ex', ttl)
return redis.call('incr', KEYS[1])"""

CHORD_FINALIZE_EXPIRE_LUA = """\
local ttl = tonumber(ARGV[2])
if redis.call('set', KEYS[1], ARGV[1], 'nx', 'ex', ttl) == false then
    return 0
end
return 1"""


class RedisStorage(BaseStorage):
    priority = False  # Use PriorityRedisStorage instead. Requires Redis>=5.0.
    # All redis backends can run the atomic lua-based chord bookkeeping.
    supports_chord_dedup = True
    redis_client = Redis

    def __init__(self, name='huey', blocking=True, read_timeout=1,
                 connection_pool=None, url=None, client_name=None,
                 notify_result=False, notify_result_ttl=60,
                 clean_name=True, **connection_params):

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
        self._chord_member = self.conn.register_script(CHORD_MEMBER_LUA)
        self._chord_finalize = self.conn.register_script(CHORD_FINALIZE_LUA)

        self.name = self.clean_name(name) if clean_name else name
        self.queue_key = 'huey.redis.%s' % self.name
        self.schedule_key = 'huey.schedule.%s' % self.name
        self.result_key = 'huey.results.%s' % self.name
        self.counter_key = 'huey.counters.%s' % self.name
        self.notify_prefix = 'huey.notify.%s.' % self.name
        self.notify_result = notify_result
        self.notify_result_ttl = notify_result_ttl

        if client_name is not None:
            self.conn.client_setname(client_name)

        self.blocking = blocking
        self.read_timeout = read_timeout

    @cached_property
    def supports_hash_ttl(self):
        # HEXPIRE needs redis 7.4+ or valkey 9+ (which reports redis 7.2).
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
                # Empty or timed out. ConnectionError propagates for backoff.
                return None
        else:
            return self.conn.rpop(self.queue_key)

    def queue_size(self):
        return self.conn.llen(self.queue_key)

    def enqueued_items(self, limit=None):
        if limit:
            # The next `limit` tasks to be dequeued.
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

    def put_data(self, key, value, is_result=False):
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
        try:
            result = self.conn.blpop(nkey, timeout=timeout or 0)
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

    def chord_member_complete(self, claim_key, result_key, value,
                              finalize_key, ttl=None):
        count = self._chord_member(
            keys=[self.result_key, self.counter_key],
            args=[finalize_key, claim_key, result_key, value])
        return count != 0

    def chord_count(self, finalize_key):
        value = self.conn.hget(self.counter_key, finalize_key)
        return None if value is None else int(value)

    def chord_finalize(self, finalize_key, ttl=None):
        return bool(self._chord_finalize(keys=[self.result_key],
                                         args=[finalize_key]))

    def chord_read_results(self, result_keys):
        values = self.conn.hmget(self.result_key, result_keys)
        return {k: v for k, v in zip(result_keys, values) if v is not None}

    def chord_clear(self, claim_keys, result_keys, finalize_key):
        # Keep the claim markers and finalize tombstone; only member values
        # and the completion counter are removed.
        pipe = self.conn.pipeline()
        if result_keys:
            pipe.hdel(self.result_key, *result_keys)
        pipe.hdel(self.counter_key, finalize_key)
        pipe.execute()

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
    def __init__(self, name='huey', expire_time=86400, *args, **kwargs):
        super(RedisExpireStorage, self).__init__(name, *args, **kwargs)

        self._expire_time = expire_time

        self.result_prefix = rp = b'huey.r.%s.' % self.name.encode('utf8')
        self.counter_prefix = cp = b'huey.c.%s.' % self.name.encode('utf8')

        encode = lambda s: s if isinstance(s, bytes) else s.encode('utf8')
        self.result_key = lambda k: rp + encode(k)
        self.counter_key = lambda k: cp + encode(k)

        self._chord_member = self.conn.register_script(
            CHORD_MEMBER_EXPIRE_LUA)
        self._chord_finalize = self.conn.register_script(
            CHORD_FINALIZE_EXPIRE_LUA)

    def put_data(self, key, value, is_result=False):
        if is_result:
            # We only want to expire task result data. If we are storing an
            # important metadata like a revocation key, we need to preserve it.
            self.conn.set(self.result_key(key), value, ex=self._expire_time)
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
        return bool(self.conn.set(self.result_key(key), value, nx=True,
                                  ex=ttl))

    def incr(self, key, amount=1):
        pipe = self.conn.pipeline()
        pipe.incr(self.counter_key(key), amount)
        pipe.expire(self.counter_key(key), self._expire_time)
        return pipe.execute()[0]

    def delete_counter(self, key):
        self.conn.delete(self.counter_key(key))

    def chord_member_complete(self, claim_key, result_key, value,
                              finalize_key, ttl=None):
        ttl = self._expire_time if ttl is None else ttl
        count = self._chord_member(
            keys=[self.counter_key(finalize_key)],
            args=[self.result_key(finalize_key), self.result_key(claim_key),
                  self.result_key(result_key), value, ttl])
        return count != 0

    def chord_count(self, finalize_key):
        value = self.conn.get(self.counter_key(finalize_key))
        return None if value is None else int(value)

    def chord_finalize(self, finalize_key, ttl=None):
        ttl = self._expire_time if ttl is None else ttl
        return bool(self._chord_finalize(
            keys=[self.result_key(finalize_key)],
            args=['1', ttl]))

    def chord_read_results(self, result_keys):
        values = self.conn.mget([self.result_key(k) for k in result_keys])
        return {k: v for k, v in zip(result_keys, values) if v is not None}

    def chord_clear(self, claim_keys, result_keys, finalize_key):
        # Keep the claim markers and finalize tombstone; only member values
        # and the completion counter are removed.
        keys = [self.result_key(k) for k in result_keys]
        keys.append(self.counter_key(finalize_key))
        if keys:
            self.conn.delete(*keys)

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
                # Empty or timed out. ConnectionError propagates for backoff.
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
    # The exclusive write transaction makes the claim/count/finalize steps
    # atomic even with multiple threads or processes sharing the database.
    supports_chord_dedup = True
    integrity_error = getattr(sqlite3, 'IntegrityError', None)
    operational_error = getattr(sqlite3, 'OperationalError', None)
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
                 create_tables=True, **kwargs):
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
            ddl = list(self.ddl)
            ddl[3] = self.table_task.replace(
                'primary key',
                'primary key autoincrement')
            self.ddl = ddl

        self.to_blob = memoryview

        super(SqliteStorage, self).__init__(name, create_tables=create_tables)

    def _create_connection(self):
        conn = sqlite3.connect(self.filename, timeout=self._timeout,
                               check_same_thread=False,
                               **self._conn_kwargs)
        conn.isolation_level = None  # Autocommit mode.
        self._set_journal_mode(conn)
        if self._cache_mb:
            conn.execute('pragma cache_size=%s' % (-1000 * self._cache_mb))
        if self._fsync is not None:
            conn.execute('pragma synchronous=%s' % (2 if self._fsync else 0))
        return conn

    def _set_journal_mode(self, conn):
        if self._journal_mode is None:
            return
        # Switching a new database to WAL upgrades a read lock to a write
        # lock, which can fail under concurrency.
        deadline = time.monotonic() + self._timeout
        while True:
            try:
                conn.execute('pragma journal_mode="%s"' % self._journal_mode)
                return
            except self.operational_error as exc:
                if 'database is locked' not in str(exc):
                    raise
                elif time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)

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

    def put_data(self, key, value, is_result=False):
        self.sql('insert or replace into kv (queue, key, value) '
                 'values (?, ?, ?)',
                 (self.name, key, self.to_blob(value)), True)

    def peek_data(self, key):
        res = self.sql('select value from kv where queue = ? and key = ?',
                       (self.name, key), results=True)
        return res[0][0] if res else EmptyData

    def peek_many(self, keys):
        return dict(self.sql(
            'select key, value from kv where queue = ? and key in (%s)' %
            ','.join('?' * len(keys)), (self.name,) + tuple(keys),
            results=True))

    def pop_data(self, key):
        with self.db(commit=True) as curs:
            curs.execute('select value from kv where queue = ? and key = ?',
                         (self.name, key))
            result = curs.fetchone()
            if result is not None:
                curs.execute('delete from kv where queue=? and key=?',
                             (self.name, key))
                if curs.rowcount == 1:
                    return result[0]
            return EmptyData

    def has_data_for_key(self, key):
        return bool(self.sql('select 1 from kv where queue=? and key=?',
                             (self.name, key), results=True))

    def put_if_empty(self, key, value, ttl=None):
        if ttl is not None:
            raise NotImplementedError('ttl is not supported by this storage.')
        try:
            with self.db(commit=True) as curs:
                curs.execute('insert or abort into kv '
                             '(queue, key, value) values (?, ?, ?)',
                             (self.name, key, self.to_blob(value)))
        except self.integrity_error:
            return False
        else:
            return True

    def incr(self, key, amount=1):
        with self.db(commit=True) as curs:
            curs.execute('insert into counter (queue, key, value) '
                         'values (?, ?, ?) on conflict (queue, key) '
                         'do update set value = value + ?',
                         (self.name, key, amount, amount))
            curs.execute('select value from counter '
                         'where queue = ? and key = ?', (self.name, key))
            return curs.fetchone()[0]

    def delete_counter(self, key):
        self.sql('delete from counter where queue = ? and key = ?',
                 (self.name, key), commit=True)

    def chord_member_complete(self, claim_key, result_key, value,
                              finalize_key, ttl=None):
        with self.db(commit=True) as curs:
            curs.execute('select 1 from kv where queue = ? and key = ?',
                         (self.name, finalize_key))
            if curs.fetchone() is not None:
                return False  # Chord already finalized.
            curs.execute('select 1 from kv where queue = ? and key = ?',
                         (self.name, claim_key))
            if curs.fetchone() is not None:
                return False  # Duplicate report for the same member.
            curs.execute('insert into kv (queue, key, value) values (?, ?, ?)',
                         (self.name, claim_key, self.to_blob(b'1')))
            curs.execute('insert or replace into kv (queue, key, value) '
                         'values (?, ?, ?)',
                         (self.name, result_key, self.to_blob(value)))
            curs.execute('insert into counter (queue, key, value) '
                         'values (?, ?, 1) on conflict (queue, key) '
                         'do update set value = value + 1',
                         (self.name, finalize_key))
            return True

    def chord_count(self, finalize_key):
        res = self.sql('select value from counter where queue = ? and key = ?',
                       (self.name, finalize_key), results=True)
        if not res:
            return None
        return res[0][0]

    def chord_finalize(self, finalize_key, ttl=None):
        try:
            with self.db(commit=True) as curs:
                curs.execute('insert into kv (queue, key, value) '
                             'values (?, ?, ?)',
                             (self.name, finalize_key, self.to_blob(b'1')))
        except self.integrity_error:
            return False  # Another worker finalized first.
        return True

    def chord_read_results(self, result_keys):
        if not result_keys:
            return {}
        return dict(self.sql(
            'select key, value from kv where queue = ? and key in (%s)' %
            ','.join('?' * len(result_keys)),
            (self.name,) + tuple(result_keys), results=True))

    def chord_clear(self, claim_keys, result_keys, finalize_key):
        # Member values are removed, but the claim markers and the finalize
        # tombstone are deliberately retained so a redelivered completion is
        # still rejected even after the completion counter has been reset.
        with self.db(commit=True) as curs:
            for i in range(0, len(result_keys), 500):
                chunk = result_keys[i:i + 500]
                curs.execute('delete from kv where queue = ? and key in (%s)' %
                             ','.join('?' * len(chunk)),
                             (self.name,) + tuple(chunk))
            curs.execute('delete from counter where queue = ? and key = ?',
                         (self.name, finalize_key))

    def result_store_size(self):
        return self.sql('select count(*) from kv where queue=?', (self.name,),
                        results=True)[0][0]

    def result_items(self):
        res = self.sql('select key, value from kv where queue=?', (self.name,),
                       results=True)
        return dict((k, v) for k, v in res)

    def flush_results(self):
        self.sql('delete from kv where queue=?', (self.name,), True)

    def flush_counters(self):
        self.sql('delete from counter where queue=?', (self.name,), True)


class CySqliteStorage(SqliteStorage):
    def __init__(self, name='huey', filename='huey.db', pragmas=None,
                 timeout=5, strict_fifo=False, create_tables=True, **kwargs):
        if cysqlite is None:
            raise ConfigurationError('"cysqlite" not found. Run "pip install '
                                     'cysqlite" to install.')
        self.integrity_error = cysqlite.IntegrityError
        self.operational_error = cysqlite.OperationalError

        pragmas = dict(pragmas or {})
        if 'cache_mb' in kwargs:
            pragmas['cache_size'] = kwargs.pop('cache_mb') * -1000
        if 'fsync' in kwargs:
            pragmas['synchronous'] = 2 if kwargs.pop('fsync') else 0
        kwargs.setdefault('journal_mode', pragmas.pop('journal_mode', 'wal'))

        super(CySqliteStorage, self).__init__(
            name,
            filename,
            timeout=timeout,
            strict_fifo=strict_fifo,
            create_tables=create_tables,
            pragmas=pragmas,
            **kwargs)

    def _create_connection(self):
        conn = cysqlite.connect(
            self.filename,
            timeout=self._timeout,
            **self._conn_kwargs)
        self._set_journal_mode(conn)
        return conn


class PostgresStorage(BaseSqlStorage):
    supports_chord_dedup = True

    def __init__(self, name='huey', dsn=None, connection=None, blocking=True,
                 read_timeout=1, table_prefix='huey', create_tables=True,
                 **connection_params):
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

        # Per-thread LISTEN conns, left to GC. Fork-safe, as psycopg only
        # sends Terminate from the process that created the conn.
        self._listen_local = threading.local()

        super(PostgresStorage, self).__init__(name, create_tables=create_tables)

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
        if self._conn is not None and (self._conn.closed or self._conn.broken):
            self._close_quiet(self._conn)
            self._conn = None
        return super(PostgresStorage, self).conn

    def _close_quiet(self, conn):
        try:
            conn.close()
        except Exception:
            pass

    def close(self):
        local = self._listen_local
        conn = getattr(local, 'conn', None)
        if conn is not None:
            self._close_quiet(conn)
            local.conn = None
        return super(PostgresStorage, self).close()

    def _listen_conn(self):
        local = self._listen_local
        conn = getattr(local, 'conn', None)
        if conn is not None and (conn.closed or conn.broken):
            self._close_quiet(conn)
            conn = None
        if conn is None:
            conn = self._connect()
            conn.execute('listen "%s"' % self.channel.replace('"', '""'))
            local.conn = conn
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

    def put_data(self, key, value, is_result=False):
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

    def chord_member_complete(self, claim_key, result_key, value,
                              finalize_key, ttl=None):
        # A single statement (one implicit transaction in autocommit mode)
        # performs the finalize/duplicate checks, claim, value write and
        # counter increment. The CTEs short-circuit when the tombstone or an
        # existing claim is found, so duplicate and late reports have no
        # effect. Row locks taken by the claim insert serialize concurrent
        # reports for the same member.
        query = """\
        with finalized as (
            select 1 as present from {kv}
            where queue = %s and key = %s
        ), claimed as (
            insert into {kv} (queue, key, value)
            select %s, %s, %s
            where not exists (select 1 from finalized)
            on conflict do nothing
            returning 1 as present
        ), value_written as (
            insert into {kv} (queue, key, value)
            select %s, %s, %s
            where exists (select 1 from claimed)
            on conflict (queue, key) do update set value = excluded.value
            returning 1 as present
        ), counted as (
            insert into {ct} (queue, key, value)
            select %s, %s, 1
            where exists (select 1 from claimed)
            on conflict (queue, key) do update set value = {ct}.value + 1
            returning 1 as present
        )
        select exists(select 1 from claimed) as accepted""".format(
            kv=self.table_kv, ct=self.table_counter)
        with self.db() as curs:
            curs.execute(query, (
                self.name, self._key(finalize_key),
                self.name, self._key(claim_key), b'1',
                self.name, self._key(result_key), value,
                self.name, self._key(finalize_key)))
            return curs.fetchone()[0]

    def chord_count(self, finalize_key):
        res = self.sql('select value from {} where queue = %s and key = %s'.
                       format(self.table_counter),
                       (self.name, self._key(finalize_key)), results=True)
        if not res:
            return None
        return res[0][0]

    def chord_finalize(self, finalize_key, ttl=None):
        with self.db() as curs:
            curs.execute('insert into {} (queue, key, value) '
                         'values (%s, %s, %s) on conflict do nothing'.
                         format(self.table_kv),
                         (self.name, self._key(finalize_key), b'1'))
            return curs.rowcount == 1

    def chord_read_results(self, result_keys):
        if not result_keys:
            return {}
        res = self.sql('select key, value from {} where queue = %s and '
                       'key = any(%s)'.format(self.table_kv),
                       (self.name, [self._key(k) for k in result_keys]),
                       results=True)
        return dict((k, bytes(v)) for k, v in res)

    def chord_clear(self, claim_keys, result_keys, finalize_key):
        # Keep the claim markers and finalize tombstone; only member values
        # and the completion counter are removed.
        keys = [self._key(k) for k in result_keys]
        with self.db() as curs:
            if keys:
                curs.execute('delete from {} where queue = %s and key = any(%s)'
                             .format(self.table_kv), (self.name, keys))
            curs.execute('delete from {} where queue = %s and key = %s'.
                         format(self.table_counter),
                         (self.name, self._key(finalize_key)))

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

    def put_data(self, key, value, is_result=False):
        with self.lock:
            self._put_data(key, value)

    def _put_data(self, key, value):
        # Caller holds the lock.
        if isinstance(key, str):
            key = key.encode('utf8')

        filename = self.path_for_key(key)
        dirname = os.path.dirname(filename)

        if not os.path.exists(dirname):
            os.makedirs(dirname)

        tmp = filename + '.tmp'
        with open(tmp, 'wb') as fh:
            key_len = len(key)
            fh.write(struct.pack('>I', key_len))
            fh.write(key)
            fh.write(value)
        os.replace(tmp, filename)

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

    def _result_files(self):
        for root, _, filenames in os.walk(self.result_path):
            for filename in filenames:
                if not filename.endswith('.tmp'):
                    yield os.path.join(root, filename)

    def result_store_size(self):
        return sum(1 for _ in self._result_files())

    def result_items(self):
        accum = {}
        for path in self._result_files():
            with open(path, 'rb') as fh:
                key, value = self._unpack_result(fh.read())
            if key is not None:
                accum[key.decode('utf8')] = value
        return accum

    def flush_results(self):
        self._flush_dir(self.result_path)

    def flush_counters(self):
        self._flush_dir(self.counter_path)
