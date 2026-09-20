"""
Decorator that stores Huey tasks in the transactional outbox.

Usage::

    from huey.contrib.djhuey_outbox.tasks import outbox_task

    @outbox_task()
    def send_welcome_email(user_id):
        ...

    def signup(request):
        with transaction.atomic():
            user = User.objects.create(...)
            # The outbox row is inserted in the SAME transaction/connection,
            # so it only becomes visible to dispatchers after commit (and it
            # disappears on rollback).
            send_welcome_email(user.pk)
"""
from functools import wraps

from huey.contrib.djhuey_outbox.models import OutboxTask


def save_outbox_task(huey, task, using='default'):
    """Serialize ``task`` and insert an outbox row on the ``using`` database.

    The insert participates in whatever transaction (including a nested
    ``atomic()`` savepoint) is active on that connection: it is committed
    only when the outermost atomic block commits and undone on rollback.
    """
    message = huey.serialize_task(task)
    outbox_task = OutboxTask.from_task(task, message=message)
    outbox_task.save(using=using)
    return outbox_task


def outbox_task(huey=None, using='default', name=None, **task_kwargs):
    """Decorator turning a function into an outbox-backed Huey task.

    Calling the wrapped object does NOT enqueue anything.  Instead it builds
    the Huey task with a stable id and persists it to the outbox table inside
    the caller's current transaction.  A separate dispatcher later forwards
    committed rows to Huey (at-least-once delivery).

    :param huey: the :class:`~huey.api.Huey` instance (defaults to the
        configured ``huey.contrib.djhuey.HUEY``).
    :param using: Django database alias the outbox row is written to.  It may
        also be overridden per call via the ``using`` keyword.
    :param name: optional Huey task name.
    :param task_kwargs: extra options forwarded to ``Huey.task()``.
    """
    def decorator(fn):
        if huey is not None:
            # Register eagerly so the task exists in the consumer/registry
            # before the first call (dispatchers in other processes rely on
            # this).
            wrapper = huey.task(name=name, **task_kwargs)(fn)

            @wraps(fn)
            def inner(*args, **kwargs):
                call_using = kwargs.pop('using', using)
                return _persist(huey, wrapper, call_using, args, kwargs)
        else:
            # Resolve the project HUEY lazily so importing this module does
            # not require a configured Django settings module.
            lazy = {'wrapper': None}

            @wraps(fn)
            def inner(*args, **kwargs):
                call_using = kwargs.pop('using', using)
                from huey.contrib.djhuey import HUEY as resolved_huey
                if lazy['wrapper'] is None:
                    lazy['wrapper'] = resolved_huey.task(
                        name=name, **task_kwargs)(fn)
                return _persist(resolved_huey, lazy['wrapper'],
                                call_using, args, kwargs)

            def bind(resolved_huey):
                lazy['wrapper'] = resolved_huey.task(
                    name=name, **task_kwargs)(fn)
                return lazy['wrapper']

            inner.bind = bind

        inner.call_local = fn
        return inner

    return decorator


def _persist(resolved_huey, wrapper, using, args, kwargs):
    # ``wrapper.s`` honors an explicit ``id=`` keyword; otherwise the Task
    # mints its stable UUID4 id.  Either way the SAME id is persisted now and
    # enqueued by the dispatcher later.
    task = wrapper.s(*args, **kwargs)
    save_outbox_task(resolved_huey, task, using=using)
    return resolved_huey._result_handle(task)
