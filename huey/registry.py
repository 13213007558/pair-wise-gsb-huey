from collections import namedtuple

from huey.exceptions import HueyException


Message = namedtuple('Message', ('id', 'name', 'eta', 'retries', 'retry_delay',
                                 'priority', 'args', 'kwargs', 'on_complete',
                                 'on_error', 'expires', 'expires_resolved'))

# Automatically set missing parameters to None. This is kind-of a hack, but it
# allows us to add new parameters while continuing to be able to handle
# messages enqueued with a smaller-set of arguments.
Message.__new__.__defaults__ = (None,) * len(Message._fields)

# Versioned payloads use a separate outer envelope. Workers that do not know
# this type fail while decoding instead of silently executing new payloads.
VersionedMessage = namedtuple('VersionedMessage', ('version', 'message'))


class Registry(object):
    def __init__(self):
        self._registry = {}
        self._periodic_tasks = []

    def task_to_string(self, task_class):
        return '%s.%s' % (task_class.__module__, task_class.__name__)

    def register(self, task_class):
        task_str = self.task_to_string(task_class)
        if task_str in self._registry:
            existing = self._registry[task_str]
            schema = getattr(task_class, 'task_schema', None)
            existing_schema = getattr(existing, 'task_schema', None)
            raise ValueError('Attempting to register task %s, but this '
                             'module-qualified identifier is already '
                             'registered with schema version %s. The new task '
                             'uses schema version %s. Unregister the old task '
                             'or choose a distinct name=.' % (
                                     task_str,
                                     getattr(existing_schema, 'version', None),
                                     getattr(schema, 'version', None)))

        self._registry[task_str] = task_class
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks.append(task_class)
        return True

    def unregister(self, task_class):
        task_str = self.task_to_string(task_class)
        if task_str not in self._registry:
            return False

        del self._registry[task_str]
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks = [t for t in self._periodic_tasks
                                    if t is not task_class]
        return True

    def string_to_task(self, task_str):
        if task_str not in self._registry:
            raise HueyException('%s not found in TaskRegistry' % task_str)
        return self._registry[task_str]

    def create_message(self, task):
        task_str = self.task_to_string(type(task))
        if task_str not in self._registry:
            raise HueyException('%s not found in TaskRegistry' % task_str)

        args = task.original_args
        kwargs = dict(task.original_kwargs)
        # Remove the "task" instance from any arguments before serializing.
        kwargs.pop('task', None)

        on_complete = None
        if task.on_complete is not None:
            on_complete = self.create_message(task.on_complete)

        on_error = None
        if task.on_error is not None:
            on_error = self.create_message(task.on_error)

        payload = Message(
            task.id,
            task_str,
            task.eta,
            task.retries,
            task.retry_delay,
            task.priority,
            args,
            kwargs,
            on_complete,
            on_error,
            task.expires,
            task.expires_resolved)
        if task.task_schema is not None:
            return VersionedMessage(task.message_schema_version, payload)
        return payload

    def create_task(self, message):
        if isinstance(message, VersionedMessage):
            schema_version = message.version
            message = message.message
        else:
            schema_version = None

        # Compatibility with Huey 1.11 message format.
        if not isinstance(message, Message) and isinstance(message, tuple):
            tid, name, eta, retries, retry_delay, (args, kwargs), oc = message
            message = Message(tid, name, eta, retries, retry_delay, None, args,
                              kwargs, oc, None)

        if not isinstance(message, Message):
            raise HueyException('Task message payload is invalid.')

        TaskClass = self.string_to_task(message.name)

        on_complete = None
        if message.on_complete is not None:
            on_complete = self.create_task(message.on_complete)

        on_error = None
        if message.on_error is not None:
            on_error = self.create_task(message.on_error)

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
            schema=getattr(TaskClass, 'task_schema', None),
            message_schema_version=schema_version)

    @property
    def periodic_tasks(self):
        return [task_class() for task_class in self._periodic_tasks]
