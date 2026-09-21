import copy
from collections import namedtuple

from huey.exceptions import HueyException
from huey.exceptions import MessageDecodeError
from huey.schema import SchemaError
from huey.schema import TaskSchema


MessageBase = namedtuple('Message', ('id', 'name', 'eta', 'retries',
                                     'retry_delay', 'priority', 'args',
                                     'kwargs', 'on_complete', 'on_error',
                                     'expires', 'expires_resolved'))

class Message(MessageBase):
    """Wire message with optional, backward-compatible schema metadata."""

    def __new__(cls, *args, **kwargs):
        schema = kwargs.pop('schema', None)
        result = MessageBase.__new__(cls, *args, **kwargs)
        result.schema = schema
        return result

    def __reduce__(self):
        args = tuple(self)
        evidence_fields = (
            'schema', 'original_schema', 'original_args', 'original_kwargs')
        state = dict((name, getattr(self, name))
                     for name in evidence_fields
                     if getattr(self, name, None) is not None)
        if not state:
            return (Message, args)
        return (Message, args, state)

    def __setstate__(self, state):
        if isinstance(state, dict):
            self.__dict__.update(state)


Message.__new__.__defaults__ = (None,) * len(Message._fields)


class Registry(object):
    def __init__(self, name_collision='error'):
        if name_collision not in ('error', 'allow'):
            raise ValueError('name_collision must be either "error" or '
                             '"allow"')
        self._registry = {}
        self._task_names = {}
        self._periodic_tasks = []
        self.name_collision = name_collision

    def task_to_string(self, task_class):
        return '%s.%s' % (task_class.__module__, task_class.__name__)

    def register(self, task_class):
        task_str = self.task_to_string(task_class)
        if task_str in self._registry:
            raise ValueError('Attempting to register a task with the same '
                             'identifier as existing task. Specify a different'
                             ' name= to register this task. "%s"' % task_str)

        task_name = task_class.__name__
        existing = self._task_names.get(task_name)
        if existing is not None and existing != task_str:
            if self.name_collision == 'error':
                raise ValueError('Task name "%s" is already registered as '
                                 '"%s". Specify a different name= or '
                                 'initialize Registry(name_collision="allow")'
                                 ' to permit distinct task names.' %
                                 (task_name, existing))

        schema = getattr(task_class, 'schema', None)
        if schema is not None:
            if not isinstance(schema, TaskSchema):
                raise HueyException('task schema must be a TaskSchema '
                                    'instance')

        self._registry[task_str] = task_class
        self._task_names[task_name] = task_str
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks.append(task_class)
        return True

    def unregister(self, task_class):
        task_str = self.task_to_string(task_class)
        if task_str not in self._registry:
            return False

        del self._registry[task_str]
        if self._task_names.get(task_class.__name__) == task_str:
            del self._task_names[task_class.__name__]
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks = [t for t in self._periodic_tasks
                                    if t is not task_class]
        return True

    def string_to_task(self, task_str):
        if task_str not in self._registry:
            raise MessageDecodeError('task-not-registered', task_name=task_str)
        return self._registry[task_str]

    def create_message(self, task):
        task_str = self.task_to_string(type(task))
        if task_str not in self._registry:
            raise HueyException('%s not found in TaskRegistry' % task_str)

        # Remove the "task" instance from any arguments before serializing.
        if task.kwargs and 'task' in task.kwargs:
            task.kwargs.pop('task')

        on_complete = None
        if task.on_complete is not None:
            on_complete = self.create_message(task.on_complete)

        on_error = None
        if task.on_error is not None:
            on_error = self.create_message(task.on_error)

        schema = getattr(task, 'schema_version', None)
        message = Message(
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
            schema=schema)
        if schema is not None:
            message.original_schema = getattr(
                task, 'original_schema_version', schema)
            message.original_args = getattr(
                task, 'original_args', task.args)
            message.original_kwargs = getattr(
                task, 'original_kwargs', task.kwargs)
        return message

    def create_task(self, message):
        # Compatibility with Huey 1.11 message format.
        if not isinstance(message, Message) and isinstance(message, tuple):
            tid, name, eta, retries, retry_delay, (args, kwargs), oc = message
            message = Message(tid, name, eta, retries, retry_delay, None, args,
                              kwargs, oc, None, None, None, schema=None)

        if not isinstance(message, Message):
            raise MessageDecodeError(
                'invalid-message',
                message=message)

        try:
            TaskClass = self.string_to_task(message.name)
        except MessageDecodeError as exc:
            exc.message = message
            exc.schema_version = getattr(message, 'schema', None)
            raise

        schema = getattr(TaskClass, 'schema', None)
        schema_version = getattr(message, 'schema', None)
        original_schema_version = getattr(
            message, 'original_schema', schema_version)
        original_args = getattr(message, 'original_args', message.args)
        original_kwargs = getattr(message, 'original_kwargs', message.kwargs)
        args = copy.deepcopy(message.args)
        kwargs = copy.deepcopy(message.kwargs)

        if schema is not None:
            original = None
            try:
                schema_version, args, kwargs = schema.migrate(
                    schema_version, args, kwargs)
            except SchemaError as exc:
                reason = exc.reason
                original = exc
            except Exception as exc:
                reason = 'migration-error'
                original = exc
            if original is not None:
                raise MessageDecodeError(
                    reason,
                    message=message,
                    original=original,
                    task_name=message.name,
                    schema_version=schema_version,
                    supported_version=schema.version)
        elif schema_version is not None:
            raise MessageDecodeError(
                'schema-not-supported',
                message=message,
                task_name=message.name,
                schema_version=schema_version)

        on_complete = None
        if message.on_complete is not None:
            try:
                on_complete = self.create_task(message.on_complete)
            except MessageDecodeError as exc:
                exc.reason = 'on-complete-%s' % exc.reason
                raise

        on_error = None
        if message.on_error is not None:
            try:
                on_error = self.create_task(message.on_error)
            except MessageDecodeError as exc:
                exc.reason = 'on-error-%s' % exc.reason
                raise

        task = TaskClass(
            args,
            kwargs,
            message.id,
            message.eta,
            message.retries,
            message.retry_delay,
            message.priority,
            message.expires,
            on_complete,
            on_error,
            message.expires_resolved)
        task.schema_version = schema_version
        task.original_schema_version = original_schema_version
        task.original_args = original_args
        task.original_kwargs = original_kwargs
        return task

    @property
    def periodic_tasks(self):
        return [task_class() for task_class in self._periodic_tasks]
