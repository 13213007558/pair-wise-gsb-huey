import copy

from collections import namedtuple

from huey.exceptions import ConfigurationError
from huey.exceptions import HueyException
from huey.exceptions import TaskMigrationError
from huey.utils import ChordConfig


Message = namedtuple('Message', ('id', 'name', 'eta', 'retries', 'retry_delay',
                                 'priority', 'args', 'kwargs', 'on_complete',
                                 'on_error', 'expires', 'expires_resolved',
                                 'timeout', 'chord_config', 'retry_backoff',
                                 'version'))

# Automatically set missing parameters to None. This is kind-of a hack, but it
# allows us to add new parameters layouts while continuing to be able to handle
# messages enqueued with a smaller set of arguments.
Message.__new__.__defaults__ = (None,) * len(Message._fields)


# Registry record describing a registered task implementation.
TaskRecord = namedtuple('TaskRecord', ('task_class', 'version', 'aliases',
                                       'migrations'))


class Registry(object):
    def __init__(self):
        # Canonical task name -> TaskRecord.
        self._registry = {}
        # Old/alternative task name -> canonical task name.
        self._aliases = {}
        self._periodic_tasks = []

    def task_to_string(self, task_class):
        return '%s.%s' % (task_class.__module__, task_class.__name__)

    def register(self, task_class, version=0, aliases=None, migrations=None):
        task_str = self.task_to_string(task_class)
        self._validate_registration(task_str, version, aliases, migrations)

        aliases = tuple(aliases or ())
        migrations = dict(migrations or {})

        self._registry[task_str] = TaskRecord(task_class, version, aliases,
                                              migrations)
        for alias in aliases:
            self._aliases[alias] = task_str
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks.append(task_class)
        return True

    def _validate_registration(self, task_str, version, aliases, migrations):
        if task_str in self._registry:
            raise ValueError('Attempting to register a task with the same '
                             'identifier as existing task. Specify a different'
                             ' name= to register this task. "%s"' % task_str)
        if task_str in self._aliases:
            raise ValueError(
                'Cannot register task "%s": that name is already claimed by '
                'an alias of "%s".' % (
                    task_str, self._aliases[task_str]))

        aliases = tuple(aliases or ())
        for alias in aliases:
            if not isinstance(alias, str) or not alias:
                raise ConfigurationError(
                    'Task "%s" declared an invalid alias %r; aliases must '
                    'be non-empty strings.' % (task_str, alias))
            if alias == task_str:
                raise ConfigurationError(
                    'Task "%s" cannot register itself as an alias.' %
                    task_str)
            if alias in self._registry:
                raise ValueError(
                    'Cannot register "%s" as an alias of "%s": a task is '
                    'already registered under that name.' % (
                        alias, task_str))
            if alias in self._aliases:
                raise ValueError(
                    'Cannot register alias "%s" for task "%s": it is '
                    'already registered as an alias of "%s".' % (
                        alias, task_str, self._aliases[alias]))
        if len(set(aliases)) != len(aliases):
            raise ConfigurationError(
                'Task "%s" declares duplicate aliases: %r.' % (
                    task_str, aliases))

        if (not isinstance(version, int) or isinstance(version, bool) or
                version < 0):
            raise ConfigurationError(
                'Task "%s" declared with invalid version %r; versions must '
                'be non-negative integers.' % (task_str, version))

        migrations = dict(migrations or {})
        for source_version, fn in list(migrations.items()):
            if (not isinstance(source_version, int) or
                    isinstance(source_version, bool) or
                    source_version < 0 or source_version >= version):
                raise ConfigurationError(
                    'Task "%s" declares a migration from invalid version '
                    '%r (current version is %s).' % (
                        task_str, source_version, version))
            if not callable(fn):
                raise ConfigurationError(
                    'Migration for task "%s" from version %s must be a '
                    'callable.' % (task_str, source_version))

        # The migration chain must be complete: every version from 0 up to
        # the current version requires an explicit transform. A missing link
        # would leave old messages with no safe upgrade path.
        missing = [v for v in range(version) if v not in migrations]
        if missing:
            raise ConfigurationError(
                'Task "%s" (version %s) is missing explicit migration(s) '
                'from version(s) %s.' % (
                    task_str, version,
                    ', '.join(str(v) for v in missing)))

    def unregister(self, task_class):
        task_str = self.task_to_string(task_class)
        if task_str not in self._registry:
            return False

        record = self._registry.pop(task_str)
        for alias in record.aliases:
            self._aliases.pop(alias, None)
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks = [t for t in self._periodic_tasks
                                    if t is not task_class]
        return True

    def _resolve(self, task_str):
        """Return the TaskRecord for a canonical name or alias."""
        if task_str in self._registry:
            return self._registry[task_str]
        canonical = self._aliases.get(task_str)
        if canonical is not None and canonical in self._registry:
            return self._registry[canonical]
        raise HueyException('%s not found in TaskRegistry' % task_str)

    def string_to_task(self, task_str):
        return self._resolve(task_str).task_class

    def task_classes(self):
        """Iterate over all registered task classes (canonical names)."""
        return [record.task_class for record in self._registry.values()]

    def create_message(self, task):
        task_class = type(task)
        task_str = self.task_to_string(task_class)
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
            getattr(task_class, 'version', 0))

    def create_task(self, message):
        message, TaskClass = self._migrate_message(message)

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

    def _message_version(self, message):
        # Messages enqueued before versioning existed carry no version field;
        # they are treated as version 0.
        version = message.version
        if version is None:
            return 0
        if (not isinstance(version, int) or isinstance(version, bool) or
                version < 0):
            raise TaskMigrationError(
                'Message for task "%s" has an invalid version %r.' % (
                    message.name, version))
        return version

    def _migrate_message(self, message):
        """Resolve aliases and run explicit migrations up to the current
        version. Returns a (possibly new) Message and the current task class.

        Only the message payload (args/kwargs) is transformed. Identity and
        scheduling metadata are preserved, and migrations never run against
        partially-migrated data: a failure aborts the whole message, which is
        left on the queue untouched.
        """
        record = self._resolve(message.name)
        task_class = record.task_class
        current_version = self._message_version(message)
        version = current_version

        if version > record.version:
            raise TaskMigrationError(
                'Message for task "%s" was enqueued with future version %s, '
                'but the registered task only supports up to version %s. '
                'Upgrade the worker to process this message.' % (
                    message.name, version, record.version))

        args, kwargs = message.args, message.kwargs
        while version < record.version:
            transform = record.migrations.get(version)
            if transform is None:
                raise TaskMigrationError(
                    'Broken migration chain for task "%s": no transform is '
                    'registered from version %s to version %s.' % (
                        message.name, version, version + 1))
            try:
                result = transform(copy.copy(args), copy.copy(kwargs))
            except Exception as exc:
                raise TaskMigrationError(
                    'Migration of task "%s" from version %s to %s failed: '
                    '%s' % (message.name, version, version + 1, exc)) from exc
            if not isinstance(result, tuple) or len(result) != 2:
                raise TaskMigrationError(
                    'Migration of task "%s" from version %s must return a '
                    '(args, kwargs) tuple.' % (message.name, version))
            args, kwargs = result
            version += 1

        if version != current_version:
            message = message._replace(args=args, kwargs=kwargs,
                                       version=version)
        return message, task_class

    @property
    def periodic_tasks(self):
        return [task_class() for task_class in self._periodic_tasks]
