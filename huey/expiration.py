"""
Tools for expiring task results and cleaning up the result store.

Huey distinguishes several kinds of data that share the result store:

* short-lived debugging information (exceptions, tracebacks, retry state),
* longer-lived business results (task return values),
* group/chord metadata which references individual task results,
* revocation markers and placeholders for unfinished (pending) results.

Each kind can be assigned its own TTL via the ExpirationPolicy class, and
individual tasks may override the TTL for their own results. Expired data is
removed by calling Huey.cleanup_results(), which is safe to run concurrently
with workers and may be called repeatedly (it is idempotent).
"""
import datetime
import time

from collections import namedtuple


class ResultKind(object):
    """
    Classification for data stored in the result store. Each kind may be
    assigned an independent TTL.
    """
    COMPLETE = 'complete'  # Task finished successfully (business result).
    ERROR = 'error'        # Task failed, no retries remaining (debug info).
    RETRY = 'retry'        # Task failed but will be retried (debug info).
    GROUP = 'group'        # Group/chord summary referencing member results.
    REVOKED = 'revoked'    # Revocation marker for a task or task-class.
    PENDING = 'pending'    # Placeholder for an unfinished (in-flight) result.

    @classmethod
    def all(cls):
        return (cls.COMPLETE, cls.ERROR, cls.RETRY, cls.GROUP, cls.REVOKED,
                cls.PENDING)


class ResultMetadata(namedtuple('_ResultMetadata', (
        'key', 'kind', 'task_name', 'timestamp', 'references'))):
    """
    Bookkeeping record stored alongside a result-store key.

    :param str key: the result-store key this metadata describes.
    :param str kind: one of the ResultKind values.
    :param str task_name: originating task name, used for per-task TTLs.
    :param float timestamp: unix timestamp of the last write.
    :param tuple references: keys referenced by this entry (group members).
    """
    __slots__ = ()

    def __new__(cls, key, kind, task_name=None, timestamp=None,
                references=()):
        if timestamp is None:
            timestamp = time.time()
        return super(ResultMetadata, cls).__new__(
            cls, key, kind, task_name, float(timestamp), tuple(references))

    def is_expired(self, ttl, now=None):
        # A TTL of None means "keep forever". A TTL of zero expires
        # immediately. Otherwise the entry expires ttl seconds after it was
        # last written.
        if ttl is None:
            return False
        if now is None:
            now = time.time()
        return self.timestamp + ttl <= now

    def serialize(self):
        return {
            'kind': self.kind,
            'task_name': self.task_name,
            'timestamp': self.timestamp,
            'references': list(self.references)}

    @classmethod
    def deserialize(cls, key, data):
        return cls(key,
                   data['kind'],
                   data.get('task_name'),
                   data['timestamp'],
                   data.get('references') or ())


class CleanupReport(namedtuple('_CleanupReport', (
        'scanned', 'deleted', 'skipped_referenced', 'skipped_pending',
        'cursor'))):
    """
    Result of a Huey.cleanup_results() call.

    :param int scanned: entries examined in this call.
    :param int deleted: entries actually removed.
    :param int skipped_referenced: expired entries kept because a live group
        still references them.
    :param int skipped_pending: unfinished (pending) entries kept because
        their TTL has not elapsed.
    :param cursor: opaque continuation token; pass back as the cursor
        argument to resume a segmented cleanup. None means the scan is
        complete.

    The report is a plain value object: repeated cleanup calls against an
    unchanged store always produce equal reports, so operations tooling can
    safely retry a cleanup and compare results.
    """
    __slots__ = ()


def normalize_ttl(ttl, param='ttl'):
    """
    Normalize a TTL specification to a number of seconds (float), or None.

    * None -- never expires.
    * 0 -- expires immediately.
    * positive int/float or datetime.timedelta -- seconds until expiry.
    * negative values are rejected with a ValueError.
    """
    if ttl is None:
        return None
    if isinstance(ttl, datetime.timedelta):
        ttl = ttl.total_seconds()
    if not isinstance(ttl, (int, float)):
        raise ValueError('%s must be None, a number of seconds, or a '
                         'timedelta, got %r' % (param, ttl))
    if ttl < 0:
        raise ValueError('%s must not be negative, got %r' % (param, ttl))
    return float(ttl)


class ExpirationPolicy(object):
    """
    Maps result kinds (and optionally individual tasks) to TTLs.

    :param default: fallback TTL for kinds without an explicit setting.
    :param task_ttls: optional mapping of task name -> mapping of
        kind -> TTL, overriding the per-kind defaults.
    :param kind_ttls: per-kind TTLs, e.g. complete=86400, error=300.

    TTL semantics:

    * None (the default) -- entries of this kind never expire.
    * 0 -- entries expire immediately; results with a zero TTL are not
      retained at all.
    * positive value -- seconds (or timedelta) after the last write at
      which the entry becomes eligible for cleanup.
    * negative values raise ValueError.
    """
    def __init__(self, default=None, task_ttls=None, **kind_ttls):
        self.default = normalize_ttl(default, 'default')
        self._kind_ttls = {}
        self._task_ttls = {}
        for kind, ttl in kind_ttls.items():
            self.set_ttl(kind, ttl)
        for task_name, mapping in (task_ttls or {}).items():
            for kind, ttl in mapping.items():
                self.set_task_ttl(task_name, kind, ttl)

    def _check_kind(self, kind):
        if kind not in ResultKind.all():
            raise ValueError('unknown result kind: %r (expected one of %s)'
                             % (kind, ', '.join(ResultKind.all())))

    def set_ttl(self, kind, ttl):
        self._check_kind(kind)
        self._kind_ttls[kind] = normalize_ttl(ttl)

    def set_task_ttl(self, task_name, kind, ttl):
        self._check_kind(kind)
        self._task_ttls.setdefault(task_name, {})[kind] = \
            normalize_ttl(ttl)

    def ttl_for(self, kind, task_name=None):
        """
        Resolve the effective TTL for the given kind, preferring a per-task
        override, then the per-kind setting, then the policy default.
        """
        self._check_kind(kind)
        if task_name is not None:
            task_mapping = self._task_ttls.get(task_name)
            if task_mapping is not None and kind in task_mapping:
                return task_mapping[kind]
        if kind in self._kind_ttls:
            return self._kind_ttls[kind]
        return self.default

    def is_expired(self, metadata, now=None):
        ttl = self.ttl_for(metadata.kind, metadata.task_name)
        return metadata.is_expired(ttl, now)

    def __repr__(self):
        return '<ExpirationPolicy: default=%s kinds=%s tasks=%s>' % (
            self.default, self._kind_ttls, self._task_ttls)
