.. _expiration:

Result expiration and cleanup
=============================

The result store accumulates several kinds of data with different
lifetimes:

* **business results** -- task return values, typically longer-lived,
* **debugging information** -- exception and retry data, typically
  short-lived,
* **group/chord metadata** -- summaries that reference individual task
  results,
* **revocation markers** and **pending placeholders** for unfinished
  results.

Huey lets you assign an independent TTL to each kind of result (and to
individual tasks), and provides an idempotent cleanup operation to remove
expired data.

Configuring expiration
----------------------

Pass an :py:class:`ExpirationPolicy` (or a plain dict) as the
``result_expiration`` argument when instantiating :py:class:`Huey`:

.. code-block:: python

    from huey import RedisHuey
    from huey.expiration import ExpirationPolicy

    policy = ExpirationPolicy(
        default=None,      # Anything not listed never expires.
        complete=86400,    # Business results are kept for a day.
        error=3600,        # Debugging info is kept for an hour.
        retry=600,         # Retry state is short-lived.
        group=43200,       # Group/chord metadata.
        revoked=86400,     # Revocation markers.
        pending=3600,      # Unfinished (in-flight) result placeholders.
        task_ttls={
            'generate_report': {'complete': 604800},  # Per-task override.
        })

    huey = RedisHuey('my-app', result_expiration=policy)

The recognized kinds are defined by :py:class:`ResultKind`:

* ``complete`` -- the task finished successfully.
* ``error`` -- the task raised an exception and will not be retried.
* ``retry`` -- the task raised an exception but will be retried.
* ``group`` -- group/chord summary metadata (see :py:meth:`Huey.put_group`).
* ``revoked`` -- revocation markers written by :py:meth:`Huey.revoke` and
  :py:meth:`Huey.revoke_all`.
* ``pending`` -- placeholder recorded when a task is enqueued; replaced by
  the task's result (or error) metadata when the task finishes.

TTL semantics:

* ``None`` (the default) -- entries never expire.
* ``0`` -- entries expire immediately; results with a zero TTL are not
  retained at all.
* a positive number of seconds (or a ``datetime.timedelta``) -- the entry
  becomes eligible for cleanup that many seconds after it was last written.
* negative values raise ``ValueError``.

TTLs are resolved per entry: a per-task override wins over the per-kind
TTL, which wins over the policy default.

Running the cleanup
-------------------

Expired entries are removed by calling :py:meth:`Huey.cleanup_results`,
for example from a cron job or a periodic task:

.. code-block:: python

    report = huey.cleanup_results()
    print('deleted', report.deleted, 'of', report.scanned, 'entries')

The method returns a :py:class:`CleanupReport` describing what was
examined and removed. The cleanup is designed to be safe to run at any
time, including while workers are executing tasks:

* **No lost updates.** Entries are removed with a conditional delete that
  verifies the entry was not rewritten since the scan began. If a worker
  stores a new result concurrently, the cleanup leaves it alone.
* **Groups stay consistent.** Results referenced by an unexpired group
  (see :py:meth:`Huey.put_group`) are never deleted, even if their own
  TTL has elapsed. When a group itself expires, the group summary is
  deleted *before* its members, so readers never observe a group whose
  members have already been removed.
* **Pending results are protected.** Placeholders for tasks that have
  been enqueued but not yet finished are kept until the ``pending``
  TTL elapses, so a crashed worker's unfinished results eventually expire
  without disturbing in-flight work.

Idempotency and segmented cleanups
----------------------------------

The cleanup is idempotent: running it repeatedly, or resuming it after a
restart, is safe and does not require any locking. Deleting an
already-deleted entry is a no-op, and repeated calls against an unchanged
store return equal :py:class:`CleanupReport` values, so operations
tooling can blindly retry a failed cleanup run.

For large result stores, the cleanup can be performed in segments using
``limit`` and the cursor returned in the report:

.. code-block:: python

    cursor = None
    while True:
        report = huey.cleanup_results(limit=1000, cursor=cursor)
        cursor = report.cursor
        if cursor is None:
            break

The cursor is simply the last examined key; it can be persisted and reused
after a restart (with a storage backend that persists metadata, see
below). Retrying a segment with the same cursor examines the same entries
and is harmless if they were already deleted.

Storage backend differences
---------------------------

Expiration metadata (kind, task name, write timestamp, group references)
is tracked alongside each result. How visible and durable this metadata
is depends on the storage backend:

* **Memory** (``MemoryHuey`): metadata lives in the process and is lost
  on restart. Cleanup is fully atomic with respect to other threads.
* **SQLite** (``SqliteHuey`): metadata is stored in a ``kv_meta``
  table and survives restarts, so segmented cleanups can be resumed after
  a restart. Writes and conditional deletes are transactional.
* **Redis** (``RedisHuey`, ``PriorityRedisHuey`): metadata is stored
  in a ``huey.results.meta.<queue>` hash and survives restarts. Redis
  hashes do not support per-field TTLs, so expiration is always performed
  by :py:meth:`~Huey.cleanup_results`; conditional deletes are executed
  atomically via a Lua script.
* **Redis with native expiration** (``RedisExpireHuey`,
  ``PriorityRedisExpireHuey`): each result is a standalone Redis key
  with a server-side TTL (``expire_time``, or the per-kind/per-task
  TTL when a policy is configured). The server removes expired results
  lazily, so memory may be reclaimed later than the TTL; cleanup metadata
  is still tracked and ``cleanup_results()`` removes anything the
  server has not. A per-result TTL of zero means the result is never
  written.
* **File-system** (``FileHuey`): metadata is tracked in memory only and
  does not survive a restart.

When a storage backend does not support native expiration
(``Storage.supports_native_expiration` is ``False``), the
``expire`` hint passed to the storage layer is ignored and
``cleanup_results()`` is solely responsible for removing expired
entries.
