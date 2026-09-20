"""
Durable, transaction-safe Huey tasks for Django (the "transactional outbox"
pattern).

Unlike `on_commit_task` (whose behavior is unchanged), a function decorated
with `outbox_task` is first written to a database table *inside the
caller's transaction* and only handed to Huey after the commit succeeds. If
the process exits between the commit and the enqueue, the pending row is
recovered and enqueued later by re-running the dispatcher (see the
`dispatch_outbox` management command or `dispatch_outbox()` below).

Delivery is **at-least-once**: a row is only marked sent after Huey accepted
the message, so a crash between the send and the acknowledgement re-enqueues
the *same* task id. This does not provide exactly-once execution of the task
body -- task functions must be idempotent.
"""
from functools import wraps
import logging

from django.db import DEFAULT_DB_ALIAS
from django.db import transaction


__all__ = ['outbox_task', 'dispatch_outbox', 'OutboxTask']


logger = logging.getLogger('huey.outbox')


def __getattr__(name):
    # Imported lazily: Django imports this package while populating the app
    # registry, before models may be loaded.
    if name == 'OutboxTask':
        from huey.contrib.djhuey.outbox.models import OutboxTask
        return OutboxTask
    raise AttributeError(name)


def _nudge(using):
    # Best-effort, in-process fast path: try to dispatch right after the
    # commit. If this process dies first (or this raises), the row stays
    # pending and a later dispatcher run recovers it.
    from huey.contrib.djhuey.outbox import dispatcher
    try:
        dispatcher.dispatch_outbox(using=using)
    except Exception:
        logger.exception('outbox: post-commit dispatch failed (using=%s); '
                         'the row remains pending for recovery.', using)


def outbox_task(*args, **kwargs):
    """
    Decorator combining the calling conventions of `on_commit_task` with
    durable, at-least-once delivery.

    Calling the decorated function:

    1. Builds the Huey task (with a stable, randomly-generated task id).
    2. Inserts an `OutboxTask` row within the *current* transaction on the
       configured database alias, so the row commits or rolls back together
       with the surrounding business writes (savepoints included).
    3. Registers an `on_commit` callback that attempts an immediate
       dispatch; if the process exits before it runs, the row is recovered
       by a later `dispatch_outbox` run.

    Returns a result handle immediately, like `on_commit_task`.

    :param using: database alias the outbox row (and the surrounding
        transaction) belongs to. Defaults to the "default" alias; may be
        overridden per-call with the `_outbox_using` keyword argument.
    Remaining args/kwargs are passed to `Huey.task`; see
    `on_commit_task` for the limitations that also apply here.
    """
    using = kwargs.pop('using', None)

    def decorator(fn):
        from huey.contrib.djhuey import close_db
        from huey.contrib.djhuey import HUEY
        from huey.contrib.djhuey import task
        task_wrapper = task(*args, **kwargs)(close_db(fn))

        @wraps(fn)
        def inner(*a, **k):
            from huey.contrib.djhuey.outbox.models import OutboxTask
            db_alias = k.pop('_outbox_using', None) or using or DEFAULT_DB_ALIAS
            huey_task = task_wrapper.s(*a, **k)
            OutboxTask.objects.using(db_alias).create(
                task_id=huey_task.id,
                task_name=huey_task.name,
                payload=HUEY.serialize_task(huey_task))
            transaction.on_commit(lambda: _nudge(db_alias), using=db_alias)
            return HUEY._result_handle(huey_task)
        inner.call_local = fn
        inner.task_wrapper = task_wrapper
        return inner
    return decorator


def dispatch_outbox(using=DEFAULT_DB_ALIAS, **kwargs):
    """
    Re-runnable recovery entry-point: dispatch one bounded batch of pending
    outbox rows stored on the given database alias. Safe to call at any
    time, from any number of processes.
    """
    from huey.contrib.djhuey.outbox.dispatcher import dispatch_outbox
    return dispatch_outbox(using=using, **kwargs)
