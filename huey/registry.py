from collections import namedtuple

from huey.exceptions import HueyException
from huey.exceptions import TaskMigrationError
from huey.utils import ChordConfig


Message = namedtuple('Message', ('id', 'name', 'eta', 'retries', 'retry_delay',
                                 'priority', 'args', 'kwargs', 'on_complete',
                                 'on_error', 'expires', 'expires_resolved',
                                 'timeout', 'chord_config', 'retry_backoff',
                                 'version'))

# Automatically set missing parameters to None. This is kind-of a hack, but it
# allows us to add new parameters while continuing to be able to handle
# messages enqueued with a smaller-set of arguments.
Message.__new__.__defaults__ = (None,) * len(Message._fields)


class Registry(object):
    def __init__(self):
        self._registry = {}
        self._aliases = {}
        self._migrations = {}
        self._periodic_tasks = []

    def task_to_string(self, task_class):
        return '%s.%s' % (task_class.__module__, task_class.__name__)

    def register(self, task_class):
        task_str = self.task_to_string(task_class)
        if task_str in self._registry or task_str in self._aliases:
            raise ValueError('Attempting to register a task with the same '
                             'identifier as existing task. Specify a different'
                             ' name= to register this task. "%s"' % task_str)

        version = getattr(task_class, 'version', 0) or 0
        if not isinstance(version, int) or isinstance(version, bool) or \
                version < 0:
            raise ValueError('Task version must be a non-negative integer, '
                             'got %r for "%s".' % (version, task_str))

        aliases = tuple(getattr(task_class, 'aliases', ()) or ())
        seen = set()
        for alias in aliases:
            if alias in seen or alias == task_str or \
                    alias in self._registry or alias in self._aliases:
                raise ValueError('Attempting to register task "%s" with an '
                                 'alias that conflicts with an existing task '
                                 'name or alias: "%s"' % (task_str, alias))
            seen.add(alias)

        self._registry[task_str] = task_class
        for alias in aliases:
            self._aliases[alias] = task_str
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks.append(task_class)
        return True

    def unregister(self, task_class):
        task_str = self.task_to_string(task_class)
        if task_str not in self._registry:
            return False

        del self._registry[task_str]
        self._aliases = {alias: name for alias, name in self._aliases.items()
                         if name != task_str}
        self._migrations = {key: fn for key, fn in self._migrations.items()
                            if key[0] != task_str}
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks = [t for t in self._periodic_tasks
                                    if t is not task_class]
        return True

    def string_to_task(self, task_str):
        task_str = self._aliases.get(task_str, task_str)
        if task_str not in self._registry:
            raise HueyException('%s not found in TaskRegistry' % task_str)
        return self._registry[task_str]

    def register_migration(self, task, from_version, fn):
        # "task" may be a task class or a task name (canonical or alias).
        if isinstance(task, str):
            task_str = self._aliases.get(task, task)
        else:
            task_str = self.task_to_string(task)

        if task_str not in self._registry:
            raise HueyException('Cannot register migration for "%s": task is '
                                'not registered.' % task_str)
        if not isinstance(from_version, int) or isinstance(from_version, \
                bool) or from_version < 0:
            raise ValueError('from_version must be a non-negative integer, '
                             'got %r.' % (from_version,))

        key = (task_str, from_version)
        if key in self._migrations:
            raise ValueError('A migration from version %s is already '
                             'registered for task "%s".'
                             % (from_version, task_str))
        self._migrations[key] = fn
        return fn

    def migrate_message(self, message, task_class):
        # Upgrade a message's argument layout to the registered task's
        # current version. Only the name, args, kwargs and version fields
        # are touched -- id, eta, retries, priority and expiration metadata
        # are carried through unchanged.
        task_str = self.task_to_string(task_class)
        target = getattr(task_class, 'version', 0) or 0
        version = message.version if message.version is not None else 0

        if version > target:
            raise TaskMigrationError(
                task_str, version, target,
                'message version is newer than the registered task version')

        args, kwargs = message.args, message.kwargs
        while version < target:
            key = (task_str, version)
            if key not in self._migrations:
                raise TaskMigrationError(
                    task_str, version, target,
                    'no migration registered from version %s to version %s'
                    % (version, version + 1))

            migrate = self._migrations[key]
            try:
                result = migrate(args, kwargs)
            except Exception as exc:
                raise TaskMigrationError(
                    task_str, version, target,
                    'migration from version %s to version %s failed: %r'
                    % (version, version + 1, exc))

            try:
                args, kwargs = result
            except (TypeError, ValueError):
                raise TaskMigrationError(
                    task_str, version, target,
                    'migration from version %s to version %s must return an '
                    '(args, kwargs) 2-tuple' % (version, version + 1))
            if not isinstance(kwargs, dict):
                raise TaskMigrationError(
                    task_str, version, target,
                    'migration from version %s to version %s returned '
                    'invalid kwargs: %r' % (version, version + 1, kwargs))

            args = tuple(args)
            version += 1

        return message._replace(name=task_str, args=args, kwargs=kwargs,
                                version=target)

    def create_message(self, task):
        task_str = self.task_to_string(type(task))
        if task_str not in self._registry:
            raise HueyException('%s not found in TaskRegistry' % task_str)

        # Remove an injected context-task instance from the arguments before
        # serializing. User-provided "task" values are preserved.
        if task.kwargs and task.kwargs.get('task') is task:
            task.kwargs.pop('task')

        on_complete = None
        if task.on_complete is not None:
            on_complete = self.create_message(task.on_complete)

        on_error = None
        if task.on_error is not None:
            on_error = self.create_message(task.on_error)

        chord_config = None
        if task.chord_config is not None:
            chord_config = (
                task.chord_config.cid,
                task.chord_config.size,
                task.chord_config.idx,
                self.create_message(task.chord_config.callback))

        return Message(
            task.id,
            task_str,
            task.eta,
            task.retries,
            task.retry_delay,
            task.priority,
            task.args,
            task.kwargs,
            on_complete,
            on_error,
            task.expires,
            task.expires_resolved,
            task.timeout,
            chord_config,
            task.retry_backoff,
            getattr(type(task), 'version', 0) or 0)

    def create_task(self, message):
        TaskClass = self.string_to_task(message.name)
        message = self.migrate_message(message, TaskClass)

        on_complete = None
        if message.on_complete is not None:
            on_complete = self.create_task(message.on_complete)

        on_error = None
        if message.on_error is not None:
            on_error = self.create_task(message.on_error)

        chord_config = None
        if message.chord_config is not None:
            cid, size, idx, cb_ser = message.chord_config
            callback = self.create_task(cb_ser)
            chord_config = ChordConfig(cid, size, idx, callback)

        return TaskClass(
            message.args,
            message.kwargs,
            message.id,
            message.eta,
            message.retries,
            message.retry_delay,
            message.priority,
            message.expires,
            on_complete,
            on_error,
            message.expires_resolved,
            message.timeout,
            chord_config,
            message.retry_backoff)

    @property
    def periodic_tasks(self):
        return [task_class() for task_class in self._periodic_tasks]
