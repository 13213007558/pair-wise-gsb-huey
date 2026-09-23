import datetime
import inspect
import itertools
import logging
import re
import time
import traceback
import uuid
import warnings

from collections import OrderedDict
from collections import deque
from functools import partial
from functools import wraps

from huey import signals as S
from huey.constants import EmptyData
from huey.consumer import Consumer
from huey.exceptions import CancelExecution
from huey.exceptions import ConfigurationError
from huey.exceptions import HueyException
from huey.exceptions import ResultTimeout
from huey.exceptions import RetryTask
from huey.exceptions import TaskException
from huey.exceptions import TaskLockedException
from huey.expiration import ResultExpirationPolicy
from huey.expiration import decode_result_meta
from huey.registry import Registry
from huey.serializer import Serializer
from huey.storage import BlackHoleStorage
from huey.storage import FileStorage
from huey.storage import MemoryStorage
from huey.storage import PriorityRedisExpireStorage
from huey.storage import PriorityRedisStorage
from huey.storage import RedisExpireStorage
from huey.storage import RedisStorage
from huey.storage import SqliteStorage
from huey.utils import Error
from huey.utils import normalize_expire_time
from huey.utils import normalize_time
from huey.utils import reraise_as
from huey.utils import string_type
from huey.utils import time_clock
from huey.utils import to_timestamp
from huey.utils import utcnow


logger = logging.getLogger('huey')
_sentinel = object()


class Huey(object):
    """
    Huey executes tasks by exposing function decorators that cause the function
    call to be enqueued for execution by a separate consumer process.

    :param name: a name for the task queue, e.g. your application's name.
    :param bool results: whether to store task results.
    :param bool store_none: whether to store ``None`` in the result store.
    :param bool utc: use UTC internally by converting from local time.
    :param bool immediate: useful for debugging; causes tasks to be executed
        synchronously in the application.
    :param Serializer serializer: serializer implementation for tasks and
        result data. The default implementation uses pickle.
    :param bool compression: compress tasks and result data (gzip by default).
    :param bool use_zlib: use zlib for compression instead of gzip.
    :param bool immediate_use_memory: automatically switch to a local in-memory
        storage backend when immediate-mode is enabled.
    :param storage_kwargs: arbitrary keyword arguments that will be passed to
        the storage backend for additional configuration.
    :param result_store_expiration: configure how long entries in the result
        store are retained. Accepts None (never expire, the default), a
        number of seconds or timedelta (single TTL applied to every
        category), or a dict configuring per-category TTLs ("complete",
        "error", "retry", "group", "revoked", "pending"), an optional
        "default", and optional per-task rules under "tasks". Expiration is
        enforced by calling :py:meth:`Huey.cleanup_expired_results` (and,
        for backends with native support, by the storage itself).

    Example usage::

        from huey import RedisHuey

        # Create a huey instance.
        huey = RedisHuey('my-app')

        @huey.task()
        def add_numbers(a, b):
            return a + b

        @huey.periodic_task(crontab(minute='0', hour='2'))
        def nightly_report():
            generate_nightly_report()
    """
    storage_class = None
    _deprecated_params = ('result_store', 'events', 'store_errors',
                          'global_registry')

    def __init__(self, name='huey', results=True, store_none=False, utc=True,
                 immediate=False, serializer=None, compression=False,
                 use_zlib=False, immediate_use_memory=True, always_eager=None,
                 storage_class=None, result_store_expiration=None,
                 **storage_kwargs):
        if always_eager is not None:
            warnings.warn('"always_eager" parameter is deprecated, use '
                          '"immediate" instead', DeprecationWarning)
            immediate = always_eager

        invalid = [p for p in self._deprecated_params
                   if storage_kwargs.pop(p, None) is not None]
        if invalid:
            warnings.warn('the following Huey initialization arguments are no '
                          'longer supported: %s' % ', '.join(invalid),
                          DeprecationWarning)

        self.name = name
        self.results = results
        self.store_none = store_none
        self.utc = utc
        self.result_store_expiration = ResultExpirationPolicy.from_config(
            result_store_expiration)
        self._immediate = immediate
        self.immediate_use_memory = immediate_use_memory
        if serializer is None:
            serializer = Serializer(compression, use_zlib=use_zlib)
        self.serializer = serializer

        # Initialize storage.
        self.storage_kwargs = storage_kwargs
        if storage_class is not None:
            self.storage_class = storage_class
        self.storage = self.create_storage()

        # Allow overriding the default TaskWrapper implementation.
        self.task_wrapper_class = self.get_task_wrapper_class()

        self._locks = set()
        self._pre_execute = OrderedDict()
        self._post_execute = OrderedDict()
        self._startup = OrderedDict()
        self._shutdown = OrderedDict()
        self._registry = Registry()
        self._signal = S.Signal()
        self._tasks_in_flight = set()

    def get_task_wrapper_class(self):
        return TaskWrapper

    def create_storage(self):
        # When using immediate mode, the default behavior is to use an
        # in-memory broker rather than a live one like Redis or Sqlite, however
        # this can be overridden by specifying "immediate_use_memory=False"
        # when initializing Huey.
        if self._immediate and self.immediate_use_memory:
            return self.get_immediate_storage()

        return self.get_storage(**self.storage_kwargs)

    def get_immediate_storage(self):
        return MemoryStorage(self.name)

    def get_storage(self, **kwargs):
        if self.storage_class is None:
            warnings.warn('storage_class not specified when initializing '
                          'huey, will default to RedisStorage.')
            Storage = RedisStorage
        else:
            Storage = self.storage_class
        return Storage(self.name, **kwargs)

    @property
    def immediate(self):
        return self._immediate

    @immediate.setter
    def immediate(self, value):
        if self._immediate != value:
            self._immediate = value
            # If we are using different storage engines for immediate-mode
            # versus normal mode, we need to recreate the storage engine.
            if self.immediate_use_memory:
                self.storage = self.create_storage()

    def create_consumer(self, **options):
        return Consumer(self, **options)

    def task(self, retries=0, retry_delay=0, priority=None, context=False,
             name=None, expires=None, **kwargs):
        TaskWrapper = self.task_wrapper_class
        def decorator(func):
            return TaskWrapper(
                self,
                func.func if isinstance(func, TaskWrapper) else func,
                context=context,
                name=name,
                default_retries=retries,
                default_retry_delay=retry_delay,
                default_priority=priority,
                default_expires=expires,
                **kwargs)
        return decorator

    def periodic_task(self, validate_datetime, retries=0, retry_delay=0,
                      priority=None, context=False, name=None, expires=None,
                      **kwargs):
        TaskWrapper = self.task_wrapper_class
        def decorator(func):
            def method_validate(self, timestamp):
                return validate_datetime(timestamp)

            return TaskWrapper(
                self,
                func.func if isinstance(func, TaskWrapper) else func,
                context=context,
                name=name,
                default_retries=retries,
                default_retry_delay=retry_delay,
                default_priority=priority,
                default_expires=expires,
                validate_datetime=method_validate,
                task_base=PeriodicTask,
                **kwargs)

        return decorator

    def context_task(self, obj, as_argument=False, **kwargs):
        def context_decorator(fn):
            @wraps(fn)
            def inner(*a, **k):
                with obj as ctx:
                    if as_argument:
                        return fn(ctx, *a, **k)
                    else:
                        return fn(*a, **k)
            return inner
        def task_decorator(func):
            return self.task(**kwargs)(context_decorator(func))
        return task_decorator

    def pre_execute(self, name=None):
        def decorator(fn):
            self._pre_execute[name or fn.__name__] = fn
            return fn
        return decorator

    def unregister_pre_execute(self, name):
        if not isinstance(name, string_type):
            # Assume we were given the function itself.
            name = name.__name__
        return self._pre_execute.pop(name, None) is not None

    def post_execute(self, name=None):
        def decorator(fn):
            self._post_execute[name or fn.__name__] = fn
            return fn
        return decorator

    def unregister_post_execute(self, name):
        if not isinstance(name, string_type):
            # Assume we were given the function itself.
            name = name.__name__
        return self._post_execute.pop(name, None) is not None

    def on_startup(self, name=None):
        def decorator(fn):
            self._startup[name or fn.__name__] = fn
            return fn
        return decorator

    def unregister_on_startup(self, name):
        if not isinstance(name, string_type):
            # Assume we were given the function itself.
            name = name.__name__
        return self._startup.pop(name, None) is not None

    def on_shutdown(self, name=None):
        def decorator(fn):
            self._shutdown[name or fn.__name__] = fn
            return fn
        return decorator

    def unregister_on_shutdown(self, name=None):
        if not isinstance(name, string_type):
            # Assume we were given the function itself.
            name = name.__name__
        return self._shutdown.pop(name, None) is not None

    def notify_interrupted_tasks(self):
        while self._tasks_in_flight:
            task = self._tasks_in_flight.pop()
            self._emit(S.SIGNAL_INTERRUPTED, task)

    def signal(self, *signals):
        def decorator(fn):
            self._signal.connect(fn, *signals)
            return fn
        return decorator

    def disconnect_signal(self, receiver, *signals):
        self._signal.disconnect(receiver, *signals)

    def _emit(self, signal, task, *args, **kwargs):
        try:
            self._signal.send(signal, task, *args, **kwargs)
        except Exception as exc:
            logger.exception('Error occurred sending signal "%s"', signal)

    def serialize_task(self, task):
        message = self._registry.create_message(task)
        return self.serializer.serialize(message)

    def deserialize_task(self, data):
        message = self.serializer.deserialize(data)
        return self._registry.create_task(message)

    def enqueue(self, task):
        # Resolve the expiration time when the task is enqueued.
        if task.expires:
            task.resolve_expires(self.utc)

        self._emit(S.SIGNAL_ENQUEUED, task)

        if self._immediate:
            self.execute(task)
        else:
            self.storage.enqueue(self.serialize_task(task), task.priority)

        if not self.results:
            return

        if task.on_complete:
            current = task
            results = []
            while current is not None:
                results.append(Result(self, current))
                current = current.on_complete
            return ResultGroup(results)
        else:
            return Result(self, task)

    def enqueue_group(self, tasks, group_id=None):
        """
        Enqueue a group of tasks as a unit, recording group metadata in the
        result store. The group metadata serves two purposes:

        * it makes the group itself queryable (the group id and its member
          task ids are stored under the "group" expiration category),
        * it protects the member results from expiration cleanup for as
          long as the group metadata is alive, so a queryable group never
          observes its members missing (which could be mistaken for an
          empty, falsely-successful group).

        :param tasks: list of Task instances to enqueue.
        :param group_id: optional explicit group id.
        :return: ResultGroup wrapping the individual task Result handles.
        """
        tasks = list(tasks)
        if group_id is None:
            group_id = 'g:%s' % uuid.uuid4()
        meta = {'ts': time.time(), 'cat': 'group', 'task': None,
                'members': [task.id for task in tasks]}
        # Write the group metadata before enqueueing the members, so that
        # any cleanup running concurrently always sees the reference from
        # the group to its (future) results.
        self.storage.put_result_meta(group_id, meta)
        results = [self.enqueue(task) for task in tasks]
        return ResultGroup(results, group_id=group_id)

    def dequeue(self):
        data = self.storage.dequeue()
        if data is not None:
            return self.deserialize_task(data)

    def put(self, key, data):
        return self.storage.put_data(key, self.serializer.serialize(data))

    def put_result(self, key, data):
        return self.storage.put_data(key, self.serializer.serialize(data),
                                     is_result=True)

    def _put_task_result(self, task, data, category):
        # Write a task result along with the metadata used by the
        # expiration cleanup. The TTL is resolved from the expiration
        # policy, with an optional per-task override (the task class may
        # define "result_ttl", e.g. via @huey.task(result_ttl=...)).
        task_name = self._registry.task_to_string(type(task))
        ttl = self.result_store_expiration.ttl_for(
            category, task_name, getattr(task, 'result_ttl', None))
        meta = {'ts': time.time(), 'cat': category, 'task': task_name}
        return self.storage.put_result(
            task.id, self.serializer.serialize(data), meta, ttl=ttl)

    def put_if_empty(self, key, data):
        return self.storage.put_if_empty(key, self.serializer.serialize(data))

    def get_raw(self, key, peek=False):
        if peek:
            return self.storage.peek_data(key)
        else:
            return self.storage.pop_data(key)

    def get(self, key, peek=False):
        data = self.get_raw(key, peek)
        if data is not EmptyData:
            return self.serializer.deserialize(data)

    def delete(self, key):
        return self.storage.delete_data(key)

    def _get_timestamp(self):
        return (utcnow() if self.utc else
                datetime.datetime.now())

    def execute(self, task, timestamp=None):
        if timestamp is None:
            timestamp = self._get_timestamp()

        if not self.ready_to_run(task, timestamp):
            self.add_schedule(task)
        elif self.is_revoked(task, timestamp, False):
            logger.warning('Task %s was revoked, not executing', task)
            self._emit(S.SIGNAL_REVOKED, task)
        elif task.expires_resolved and task.expires_resolved < timestamp:
            logger.info('Task %s expired, not executing.', task)
            self._emit(S.SIGNAL_EXPIRED, task)
        else:
            logger.info('Executing %s', task)
            self._emit(S.SIGNAL_EXECUTING, task)
            return self._execute(task, timestamp)

    def _execute(self, task, timestamp):
        if self._pre_execute:
            try:
                self._run_pre_execute(task)
            except CancelExecution:
                self._emit(S.SIGNAL_CANCELED, task)
                return

        start = time_clock()
        exception = None
        retry_eta = None
        task_value = None

        try:
            self._tasks_in_flight.add(task)
            try:
                task_value = task.execute()
            finally:
                self._tasks_in_flight.remove(task)
                duration = time_clock() - start
        except TaskLockedException as exc:
            logger.warning('Task %s not run, %s.', task.id, exc)
            exception = exc
            self._emit(S.SIGNAL_LOCKED, task)
        except RetryTask as exc:
            logger.info('Task %s raised RetryTask, retrying.', task.id)
            task.retries += 1
            if exc.eta or exc.delay is not None:
                retry_eta = normalize_time(exc.eta, exc.delay, self.utc)
            exception = exc
        except CancelExecution as exc:
            if exc.retry or (exc.retry is None and task.retries):
                task.retries = max(task.retries, 1)
                msg = '(task will be retried)'
            else:
                task.retries = 0
                msg = '(aborted, will not be retried)'
            logger.warning('Task %s raised CancelExecution %s.', task.id, msg)
            self._emit(S.SIGNAL_CANCELED, task)
            exception = exc
        except KeyboardInterrupt:
            logger.warning('Received exit signal, %s did not finish.', task.id)
            self._emit(S.SIGNAL_INTERRUPTED, task)
            return
        except Exception as exc:
            logger.exception('Unhandled exception in task %s.', task.id)
            exception = exc
            self._emit(S.SIGNAL_ERROR, task, exc)
        else:
            logger.info('%s executed in %0.3fs', task, duration)

        # Clear the flag if this instance of the task was revoked after it
        # began executing by destructively reading it's revoke key.
        if not isinstance(task, PeriodicTask):
            self.get(task.revoke_id)

        if self.results and not isinstance(task, PeriodicTask):
            if exception is not None:
                error_data = self.build_error_result(task, exception)
                # Error results for tasks that will be retried are treated
                # as short-term debugging information and get their own
                # expiration category.
                category = 'retry' if task.retries else 'error'
                self._put_task_result(task, Error(error_data), category)
            elif task_value is not None or self.store_none:
                self._put_task_result(task, task_value, 'complete')

        if self._post_execute:
            self._run_post_execute(task, task_value, exception)

        if exception is None:
            # Task executed successfully, send the COMPLETE signal.
            self._emit(S.SIGNAL_COMPLETE, task)

        if task.on_complete and exception is None:
            next_task = task.on_complete
            next_task.extend_data(task_value)
            self.enqueue(next_task)
        elif task.on_error and exception is not None:
            next_task = task.on_error
            next_task.extend_data(exception)
            self.enqueue(next_task)

        if exception is not None and task.retries:
            self._emit(S.SIGNAL_RETRYING, task)
            self._requeue_task(task, self._get_timestamp(), retry_eta)

        return task_value

    def _requeue_task(self, task, timestamp, retry_eta=None):
        task.retries -= 1
        logger.info('Requeueing %s, %s retries', task.id, task.retries)
        if retry_eta is not None:
            task.eta = retry_eta
            self.add_schedule(task)
        elif task.retry_delay:
            delay = datetime.timedelta(seconds=task.retry_delay)
            task.eta = timestamp + delay
            self.add_schedule(task)
        else:
            self.enqueue(task)

    def _run_pre_execute(self, task):
        for name, callback in self._pre_execute.items():
            logger.debug('Pre-execute hook %s for %s.', name, task)
            try:
                callback(task)
            except CancelExecution:
                logger.warning('Task %s cancelled by %s (pre-execute).',
                               task, name)
                raise
            except Exception:
                logger.exception('Unhandled exception calling pre-execute '
                                 'hook %s for %s.', name, task)

    def _run_post_execute(self, task, task_value, exception):
        for name, callback in self._post_execute.items():
            logger.debug('Post-execute hook %s for %s.', name, task)
            try:
                callback(task, task_value, exception)
            except Exception as exc:
                logger.exception('Unhandled exception calling post-execute '
                                 'hook %s for %s.', name, task)

    def build_error_result(self, task, exception):
        try:
            tb = traceback.format_exc()
        except AttributeError:  # Seems to only happen on 3.4.
            tb = '- unable to resolve traceback on Python 3.4 -'

        if isinstance(exception, TaskException):
            error = exception.metadata.get('error') or repr(exception)
        else:
            error = repr(exception)

        return {
            'error': error,
            'retries': task.retries,
            'traceback': tb,
            'task_id': task.id,
        }

    def _task_key(self, task_class, key):
        return ':'.join((key, self._registry.task_to_string(task_class)))

    def revoke_all(self, task_class, revoke_until=None, revoke_once=False):
        if isinstance(task_class, TaskWrapper):
            task_class = task_class.task_class
        if revoke_until is not None:
            revoke_until = normalize_time(revoke_until, utc=self.utc)
        key = self._task_key(task_class, 'rt')
        self.put(key, (revoke_until, revoke_once))
        self._tag_revoke_key(key)

    def restore_all(self, task_class):
        if isinstance(task_class, TaskWrapper):
            task_class = task_class.task_class
        return self.delete(self._task_key(task_class, 'rt'))

    def revoke(self, task, revoke_until=None, revoke_once=False):
        if revoke_until is not None:
            revoke_until = normalize_time(revoke_until, utc=self.utc)
        self.put(task.revoke_id, (revoke_until, revoke_once))
        self._tag_revoke_key(task.revoke_id)

    def _tag_revoke_key(self, key):
        # Tag revocation markers so the expiration cleanup can retire them
        # using the "revoked" category TTL.
        self.storage.put_result_meta(key, {'ts': time.time(),
                                           'cat': 'revoked', 'task': None})

    def restore(self, task):
        # Return value indicates whether the task was in fact revoked.
        return self.delete(task.revoke_id)

    def revoke_by_id(self, id, revoke_until=None, revoke_once=False):
        return self.revoke(Task(id=id), revoke_until, revoke_once)

    def restore_by_id(self, id):
        return self.restore(Task(id=id))

    def _check_revoked(self, revoke_id, timestamp=None, peek=True):
        """
        Checks if a task is revoked, returns a 2-tuple indicating:

        1. Is task revoked?
        2. Should task be restored?
        """
        res = self.get(revoke_id, peek=True)
        if res is None:
            return False, False

        revoke_until, revoke_once = res
        if revoke_until is not None and timestamp is None:
            timestamp = self._get_timestamp()

        if revoke_once:
            # This task *was* revoked for one run, but now it should be
            # restored to normal execution (unless we are just peeking).
            return True, not peek
        elif revoke_until is not None and revoke_until <= timestamp:
            # Task is no longer revoked and can be restored.
            return False, not peek
        else:
            # Task is still revoked. Do not restore.
            return True, False

    def is_revoked(self, task, timestamp=None, peek=True):
        if isinstance(task, TaskWrapper):
            task = task.task_class
        if inspect.isclass(task) and issubclass(task, Task):
            key = self._task_key(task, 'rt')
            is_revoked, can_restore = self._check_revoked(key, timestamp, peek)
            if can_restore:
                self.restore_all(task)
            return is_revoked

        if isinstance(task, Result):
            task = task.task
        elif not isinstance(task, Task):
            # Assume we've been given a task ID.
            task = Task(id=task)

        key = task.revoke_id
        is_revoked, can_restore = self._check_revoked(key, timestamp, peek)
        if can_restore:
            self.restore(task)
        if not is_revoked:
            is_revoked = self.is_revoked(type(task), timestamp, peek)

        return is_revoked

    def add_schedule(self, task):
        data = self.serialize_task(task)
        eta = task.eta or datetime.datetime.fromtimestamp(0)
        self.storage.add_to_schedule(data, eta)
        logger.info('Added task %s to schedule, eta %s', task.id, eta)
        self._emit(S.SIGNAL_SCHEDULED, task)

    def read_schedule(self, timestamp=None):
        if timestamp is None:
            timestamp = self._get_timestamp()
        accum = []
        for msg in self.storage.read_schedule(timestamp):
            try:
                task = self.deserialize_task(msg)
            except Exception:
                logger.exception('Unable to deserialize scheduled task.')
            else:
                accum.append(task)
        return accum

    def read_periodic(self, timestamp):
        if timestamp is None:
            timestamp = self._get_timestamp()
        return [task for task in self._registry.periodic_tasks
                if task.validate_datetime(timestamp)]

    def ready_to_run(self, task, timestamp=None):
        if timestamp is None:
            timestamp = self._get_timestamp()
        return task.eta is None or task.eta <= timestamp

    def pending(self, limit=None):
        return [self.deserialize_task(task)
                for task in self.storage.enqueued_items(limit)]

    def pending_count(self):
        return self.storage.queue_size()

    def scheduled(self, limit=None):
        return [self.deserialize_task(task)
                for task in self.storage.scheduled_items(limit)]

    def scheduled_count(self):
        return self.storage.schedule_size()

    def all_results(self):
        return self.storage.result_items()

    def result_count(self):
        return self.storage.result_store_size()

    @staticmethod
    def _cleanup_key(key):
        return key.decode('utf8') if isinstance(key, bytes) else key

    def _cleanup_timestamp(self, now):
        if now is None:
            return time.time()
        if isinstance(now, datetime.datetime):
            return to_timestamp(now)
        return float(now)

    def _task_result_ttl(self, task_name):
        # Best-effort lookup of a per-task TTL override declared on the
        # task class ("result_ttl"). The task may not be registered in
        # this process, in which case only the policy applies.
        if not task_name:
            return None
        try:
            task_class = self._registry.string_to_task(task_name)
        except HueyException:
            return None
        return getattr(task_class, 'result_ttl', None)

    def cleanup_expired_results(self, now=None, limit=None, cursor=None):
        """
        Delete expired entries from the result store, honoring the
        per-category / per-task TTLs configured via the
        "result_store_expiration" parameter.

        The cleanup is:

        * idempotent - running it repeatedly (or after a restart) deletes
          the same entries; subsequent runs report an empty "deleted"
          list, making the return value stable for operational retries.
        * reference-aware - results that are still referenced by a live
          (unexpired) group are never deleted, and expired group metadata
          is removed *before* any member sweep, so a queryable group can
          never observe its members as missing.
        * safe against lost updates - an entry is only deleted if its
          metadata is unchanged since it was read, so a result being
          (re-)written by a worker concurrently is never removed.
        * segmentable - pass "limit" to process at most that many entries;
          the returned "next_cursor" can be supplied to a subsequent call
          to resume where the previous call stopped. Cursors are
          deterministic (entries are visited in sorted-key order), so a
          given cursor always identifies the same segment.

        Entries whose metadata is missing or unrecognized are treated as
        "pending" (in-progress writes) and are not deleted unless an
        explicit TTL is configured for the "pending" category.

        :param now: reference timestamp (defaults to the current time);
            may be a unix timestamp or a datetime.
        :param limit: maximum number of entries to examine in this call.
        :param cursor: opaque cursor returned by a previous call.
        :return: dict with the keys: "deleted" (sorted list of deleted
            result keys), "deleted_count", "groups_expired" (sorted list
            of deleted group-metadata keys), "scanned",
            "skipped_referenced", "skipped_stale" (concurrently-modified
            entries left in place), "next_cursor" and "done".
        """
        policy = self.result_store_expiration
        now_ts = self._cleanup_timestamp(now)

        # Phase 1: scan the group metadata to determine which member
        # results are still referenced by a live (unexpired) group, and
        # which groups have themselves expired. This scan always covers
        # all group entries, regardless of "limit", since protection is a
        # global property.
        protected = set()
        expired_groups = []
        meta_items, _ = self.storage.iter_result_meta()
        for key, raw in meta_items:
            meta = decode_result_meta(raw)
            if meta is None or meta.get('cat') != 'group':
                continue
            if policy.is_expired('group', meta.get('ts'), now_ts,
                                 meta.get('task')):
                expired_groups.append((key, meta))
            else:
                protected.update(meta.get('members') or ())

        # Expired groups are deleted before any member result so that a
        # group that is still queryable always has its members present.
        groups_expired = []
        for key, meta in expired_groups:
            if self.storage.delete_result_if_matches(key, meta):
                groups_expired.append(self._cleanup_key(key))

        # Phase 2: sweep non-group entries, optionally bounded to a
        # segment of "limit" entries starting after "cursor".
        deleted = []
        skipped_referenced = 0
        skipped_stale = 0
        items, next_cursor = self.storage.iter_result_meta(cursor=cursor,
                                                           limit=limit)
        for key, raw in items:
            meta = decode_result_meta(raw)
            if meta is None:
                category, ts, task_name = 'pending', None, None
            else:
                category = meta.get('cat') or 'pending'
                ts = meta.get('ts')
                task_name = meta.get('task')
            if category == 'group':
                continue  # Handled in phase 1.
            if self._cleanup_key(key) in protected:
                skipped_referenced += 1
                continue
            task_config = self._task_result_ttl(task_name)
            if not policy.is_expired(category, ts, now_ts, task_name,
                                     task_config):
                continue
            if self.storage.delete_result_if_matches(key, meta):
                deleted.append(self._cleanup_key(key))
            else:
                # The entry was modified concurrently (e.g. a worker
                # re-wrote the result); leave it in place.
                skipped_stale += 1

        deleted.sort()
        groups_expired.sort()
        return {
            'deleted': deleted,
            'deleted_count': len(deleted),
            'groups_expired': groups_expired,
            'scanned': len(items),
            'skipped_referenced': skipped_referenced,
            'skipped_stale': skipped_stale,
            'next_cursor': (self._cleanup_key(next_cursor)
                            if next_cursor is not None else None),
            'done': next_cursor is None,
        }

    def __len__(self):
        return self.pending_count()

    def flush(self):
        self.storage.flush_all()

    def lock_task(self, lock_name):
        return TaskLock(self, lock_name)

    def is_locked(self, lock_name):
        return TaskLock(self, lock_name).is_locked()

    def flush_locks(self, *names):
        flushed = set()
        locks = self._locks
        if names:
            lock_template = '%s.lock.%%s' % self.name
            named_locks = (lock_template % name.strip() for name in names)
            locks = itertools.chain(locks, named_locks)

        for lock_key in locks:
            if self.delete(lock_key):
                flushed.add(lock_key.split('.lock.', 1)[-1])

        return flushed

    def _result_handle(self, task):
        return Result(self, task)

    def result(self, id, blocking=False, timeout=None, backoff=1.15,
               max_delay=1.0, revoke_on_timeout=False, preserve=False):
        task_result = Result(self, Task(id=id))
        return task_result.get(
            blocking=blocking,
            timeout=timeout,
            backoff=backoff,
            max_delay=max_delay,
            revoke_on_timeout=revoke_on_timeout,
            preserve=preserve)


class Task(object):
    default_expires = None
    default_priority = None
    default_retries = 0
    default_retry_delay = 0

    def __init__(self, args=None, kwargs=None, id=None, eta=None, retries=None,
                 retry_delay=None, priority=None, expires=None,
                 on_complete=None, on_error=None, expires_resolved=None):
        self.name = type(self).__name__
        self.args = () if args is None else args
        self.kwargs = {} if kwargs is None else kwargs
        self.id = id or self.create_id()
        self.revoke_id = 'r:%s' % self.id
        self.eta = eta
        self.retries = retries if retries is not None else self.default_retries
        self.retry_delay = retry_delay if retry_delay is not None else \
                self.default_retry_delay
        self.priority = priority if priority is not None else \
                self.default_priority
        self.expires = expires if expires is not None else self.default_expires
        self.expires_resolved = expires_resolved

        self.on_complete = on_complete
        self.on_error = on_error

    @property
    def data(self):
        return (self.args, self.kwargs)

    def __repr__(self):
        rep = '%s.%s: %s' % (self.__module__, self.name, self.id)
        if self.eta:
            rep += ' @%s' % self.eta
        if self.expires:
            if self.expires_resolved and self.expires != self.expires_resolved:
                rep += ' exp=%s (%s)' % (self.expires, self.expires_resolved)
            else:
                rep += ' exp=%s' % self.expires
        if self.priority:
            rep += ' p=%s' % self.priority
        if self.retries:
            rep += ' %s retries' % self.retries
        if self.on_complete:
            rep += ' -> %s' % self.on_complete
        if self.on_error:
            rep += ', on error %s' % self.on_error
        return rep

    def __hash__(self):
        return hash(self.id)

    def create_id(self):
        return str(uuid.uuid4())

    def resolve_expires(self, utc=True):
        if self.expires:
            self.expires_resolved = normalize_expire_time(self.expires, utc)
        return self.expires_resolved

    def extend_data(self, data):
        if data is None or data == ():
            return

        if isinstance(data, tuple):
            self.args += data
        elif isinstance(data, dict):
            # XXX: alternate would be self.kwargs.update(data), but this will
            # stomp on user-provided parameters.
            for key, value in data.items():
                self.kwargs.setdefault(key, value)
        else:
            self.args = self.args + (data,)

    def then(self, task, *args, **kwargs):
        if self.on_complete:
            self.on_complete.then(task, *args, **kwargs)
        else:
            if isinstance(task, Task):
                if args: task.extend_data(args)
                if kwargs: task.extend_data(kwargs)
            else:
                task = task.s(*args, **kwargs)
            self.on_complete = task
        return self

    def error(self, task, *args, **kwargs):
        if self.on_error:
            self.on_error.error(task, *args, **kwargs)
        else:
            if isinstance(task, Task):
                if args: task.extend_data(args)
                if kwargs: task.extend_data(kwargs)
            else:
                task = task.s(*args, **kwargs)
            self.on_error = task
        return self

    def execute(self):
        # Implementation provided by subclass, see: TaskWrapper.create_task().
        raise NotImplementedError

    def __eq__(self, rhs):
        if not isinstance(rhs, Task):
            return False

        return (
            self.id == rhs.id and
            self.eta == rhs.eta and
            type(self) == type(rhs))


class PeriodicTask(Task):
    def validate_datetime(self, timestamp):
        return False


class TaskWrapper(object):
    task_base = Task

    def __init__(self, huey, func, retries=None, retry_delay=None,
                 context=False, name=None, task_base=None, **settings):
        self.__doc__ = getattr(func, '__doc__', None)
        self.huey = huey
        self.func = func
        self.retries = retries
        self.retry_delay = retry_delay
        self.context = context
        self.name = name
        self.settings = settings
        if task_base is not None:
            self.task_base = task_base

        # Dynamically create task class and register with Huey instance.
        self.task_class = self.create_task(func, context, name, **settings)
        self.huey._registry.register(self.task_class)

    def unregister(self):
        return self.huey._registry.unregister(self.task_class)

    def create_task(self, func, context=False, name=None, **settings):
        def execute(self):
            args, kwargs = self.data
            if self.context:
                kwargs['task'] = self
            return func(*args, **kwargs)

        attrs = {
            'context': context,
            'execute': execute,
            '__module__': func.__module__,
            '__doc__': func.__doc__}
        attrs.update(settings)

        if not name:
            name = func.__name__

        return type(name, (self.task_base,), attrs)

    def is_revoked(self, timestamp=None, peek=True):
        return self.huey.is_revoked(self.task_class, timestamp, peek)

    def revoke(self, revoke_until=None, revoke_once=False):
        self.huey.revoke_all(self.task_class, revoke_until, revoke_once)

    def restore(self):
        return self.huey.restore_all(self.task_class)

    def schedule(self, args=None, kwargs=None, eta=None, delay=None,
                 priority=None, retries=None, retry_delay=None, expires=None,
                 id=None):
        if eta is None and delay is None:
            if isinstance(args, (int, float)):
                delay = args
            elif isinstance(args, datetime.timedelta):
                delay = args.total_seconds()
            elif isinstance(args, datetime.datetime):
                eta = args
            else:
                raise ValueError('schedule() missing required eta= or delay=')
            args = None

        if kwargs is not None and not isinstance(kwargs, dict):
            raise ValueError('schedule() kwargs argument must be a dict.')

        eta = normalize_time(eta, delay, self.huey.utc)
        task = self.task_class(
            args or (),
            kwargs or {},
            id=id,
            eta=eta,
            retries=retries,
            retry_delay=retry_delay,
            priority=priority,
            expires=expires)
        return self.huey.enqueue(task)

    def _apply(self, it):
        return [self.s(*(i if isinstance(i, tuple) else (i,))) for i in it]

    def map(self, it):
        return ResultGroup([self.huey.enqueue(t) for t in self._apply(it)])

    def __call__(self, *args, **kwargs):
        return self.huey.enqueue(self.s(*args, **kwargs))

    def call_local(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def s(self, *args, **kwargs):
        eta = kwargs.pop('eta', None)
        delay = kwargs.pop('delay', None)
        if delay is not None and isinstance(delay, datetime.timedelta):
            delay = delay.total_seconds()
        if eta is not None or delay is not None:
            eta = normalize_time(eta, delay, self.huey.utc)

        return self.task_class(args, kwargs,
                               eta=eta,
                               retries=kwargs.pop('retries', None),
                               retry_delay=kwargs.pop('retry_delay', None),
                               priority=kwargs.pop('priority', None),
                               expires=kwargs.pop('expires', None))


class TaskLock(object):
    """
    Utilize the Storage key/value APIs to implement simple locking. For more
    details see :py:meth:`Huey.lock_task`.
    """
    def __init__(self, huey, name):
        self._huey = huey
        self._name = name
        self._key = '%s.lock.%s' % (self._huey.name, self._name)
        self._huey._locks.add(self._key)

    def is_locked(self):
        return self._huey.storage.has_data_for_key(self._key)

    def __call__(self, fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)
        return inner

    def __enter__(self):
        if not self._huey.put_if_empty(self._key, '1'):
            raise TaskLockedException('unable to acquire lock %s' % self._name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._huey.delete(self._key)

    def clear(self):
        return self._huey.delete(self._key)


class Result(object):
    """
    Wrapper around task result data. When a task is executed, an instance of
    ``Result`` is returned to provide access to the return value.

    To retrieve the task's result value, you can simply call the wrapper::

        @huey.task()
        def my_task(a, b):
            return a + b

        result = my_task(1, 2)

        # After a moment, when the consumer has executed the task and put
        # the result in the result storage, we can retrieve the value.
        print result()  # Prints 3

        # If you want to block until the result is ready, you can pass
        # blocking=True. We'll also specify a 4 second timeout so we don't
        # block forever if the consumer goes down:
        result2 = my_task(2, 3)
        print result(blocking=True, timeout=4)
    """
    def __init__(self, huey, task):
        self.huey = huey
        self.task = task
        self.revoke_id = task.revoke_id
        self._result = EmptyData

    def __repr__(self):
        return '<Result: task %s>' % self.id

    @property
    def id(self):
        return self.task.id

    def __call__(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    def _get(self, preserve=False):
        task_id = self.id
        if self._result is EmptyData:
            res = self.huey.get_raw(task_id, peek=preserve)

            if res is not EmptyData:
                self._result = self.huey.serializer.deserialize(res)
                return self._result
            else:
                return res
        else:
            return self._result

    def get_raw_result(self, blocking=False, timeout=None, backoff=1.15,
                       max_delay=1.0, revoke_on_timeout=False, preserve=False):
        if not blocking:
            res = self._get(preserve)
            if res is not EmptyData:
                return res
        else:
            start = time_clock()
            delay = .1
            while self._result is EmptyData:
                if timeout and time_clock() - start >= timeout:
                    if revoke_on_timeout:
                        self.revoke()
                    raise ResultTimeout('timed out waiting for result')
                if delay > max_delay:
                    delay = max_delay
                if self._get(preserve) is EmptyData:
                    time.sleep(delay)
                    delay *= backoff

            return self._result

    def get(self, blocking=False, timeout=None, backoff=1.15, max_delay=1.0,
            revoke_on_timeout=False, preserve=False):
        result = self.get_raw_result(blocking, timeout, backoff, max_delay,
                                     revoke_on_timeout, preserve)
        if result is not None and isinstance(result, Error):
            raise TaskException(result.metadata)
        return result

    def is_revoked(self):
        return self.huey.is_revoked(self.task, peek=True)

    def revoke(self, revoke_once=True):
        self.huey.revoke(self.task, revoke_once=revoke_once)

    def restore(self):
        return self.huey.restore(self.task)

    def reschedule(self, eta=None, delay=None, expires=None, priority=None,
                   preserve_pipeline=True):
        # Rescheduling works by revoking the currently-scheduled task (nothing
        # is done to check if the task has already run, however). Then the
        # original task's data is used to enqueue a new task with a new task ID
        # and execution_time.
        self.revoke()
        if eta is not None or delay is not None:
            eta = normalize_time(eta, delay, self.huey.utc)
        if preserve_pipeline:
            on_complete = self.task.on_complete
            on_error = self.task.on_error
        else:
            on_complete = on_error = None

        task = type(self.task)(
            self.task.args,
            self.task.kwargs,
            eta=eta,
            retries=self.task.retries,
            retry_delay=self.task.retry_delay,
            priority=priority if priority is not None else self.task.priority,
            expires=expires if expires is not None else self.task.expires,
            on_complete=on_complete,
            on_error=on_error)
        return self.huey.enqueue(task)

    def reset(self):
        self._result = EmptyData


class ResultGroup(object):
    def __init__(self, results, group_id=None):
        self._results = results
        self.group_id = group_id

    def get(self, *args, **kwargs):
        return [result.get(*args, **kwargs) for result in self._results]
    __call__ = get

    def __getitem__(self, idx):
        return self._results[idx].get(True)
    def __iter__(self):
        return iter(self._results)
    def __len__(self):
        return len(self._results)
    def as_completed(self, backoff=1.15, max_delay=1.0):
        res = deque(self._results)
        delay = {r.id: 0. for r in res}
        while res:
            r = res.popleft()
            if delay[r.id]:
                time.sleep(delay[r.id])
            if r._get() is EmptyData:
                res.append(r)
                delay[r.id] = min((delay[r.id] or 0.1) * backoff, max_delay)
            else:
                yield r.get()


dash_re = re.compile(r'(\d+)-(\d+)')
every_re = re.compile(r'\*\/(\d+)')


def crontab(minute='*', hour='*', day='*', month='*', day_of_week='*', strict=False):
    """
    Convert a "crontab"-style set of parameters into a test function that will
    return True when the given datetime matches the parameters set forth in
    the crontab.

    For day-of-week, 0=Sunday and 6=Saturday.

    Acceptable inputs:
    * = every distinct value
    */n = run every "n" times, i.e. hours='*/4' == 0, 4, 8, 12, 16, 20
    m-n = run every time m..n
    m,n = run on m and n

    The strict parameter will cause crontab to raise a ValueError if an input
    does not match a supported crontab input format. This provides backwards
    compatibility.
    """
    validation = (
        ('m', month, range(1, 13)),
        ('d', day, range(1, 32)),
        ('w', day_of_week, range(8)), # 0-6, but also 7 for Sunday.
        ('H', hour, range(24)),
        ('M', minute, range(60))
    )
    cron_settings = []

    for (date_str, value, acceptable) in validation:
        settings = set([])

        if isinstance(value, int):
            value = str(value)

        for piece in value.split(','):
            if piece == '*':
                settings.update(acceptable)
                continue

            if piece.isdigit():
                piece = int(piece)
                if piece not in acceptable:
                    raise ValueError('%d is not a valid input' % piece)
                elif date_str == 'w':
                    piece %= 7
                settings.add(piece)
                continue

            dash_match = dash_re.match(piece)
            if dash_match:
                lhs, rhs = map(int, dash_match.groups())
                if lhs not in acceptable or rhs not in acceptable:
                    raise ValueError('%s is not a valid input' % piece)
                elif date_str == 'w':
                    lhs %= 7
                    rhs %= 7
                settings.update(range(lhs, rhs + 1))
                continue

            # Handle stuff like */3, */6.
            every_match = every_re.match(piece)
            if every_match:
                if date_str == 'w':
                    raise ValueError('Cannot perform this kind of matching'
                                     ' on day-of-week.')
                interval = int(every_match.groups()[0])
                settings.update(acceptable[::interval])
                continue

            # Older versions of Huey would, at this point, ignore the unmatched piece.
            if strict:
                raise ValueError('%s is not a valid input' % piece)

        cron_settings.append(sorted(list(settings)))

    def validate_date(timestamp):
        _, m, d, H, M, _, w, _, _ = timestamp.timetuple()

        # fix the weekday to be sunday=0
        w = (w + 1) % 7

        for (date_piece, selection) in zip((m, d, w, H, M), cron_settings):
            if date_piece not in selection:
                return False

        return True

    return validate_date


def _unsupported(name, library):
    class UnsupportedHuey(Huey):
        def __init__(self, *args, **kwargs):
            raise ConfigurationError('Cannot initialize "%s", %s module not '
                                     'installed.' % (name, library))
    return UnsupportedHuey


# Convenience wrappers for the various storage implementations.
class BlackHoleHuey(Huey):
    storage_class = BlackHoleStorage

class MemoryHuey(Huey):
    storage_class = MemoryStorage

class SqliteHuey(Huey):
    storage_class = SqliteStorage

class RedisHuey(Huey):
    storage_class = RedisStorage

class RedisExpireHuey(RedisHuey):
    storage_class = RedisExpireStorage

class PriorityRedisHuey(RedisHuey):
    storage_class = PriorityRedisStorage

class PriorityRedisExpireHuey(RedisHuey):
    storage_class = PriorityRedisExpireStorage

class FileHuey(Huey):
    storage_class = FileStorage
