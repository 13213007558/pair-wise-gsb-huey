class HueyException(Exception): pass
class ConfigurationError(HueyException): pass
class TaskLockedException(HueyException): pass
class ResultTimeout(HueyException): pass


class MessageDecodeError(HueyException):
    def __init__(self, reason, message=None, data=None, original=None,
                 task_name=None, schema_version=None, supported_version=None):
        self.reason = reason
        self.message = message
        self.data = data
        self.original = original
        self.task_name = task_name
        self.schema_version = schema_version
        self.supported_version = supported_version
        super(MessageDecodeError, self).__init__(reason)

    @property
    def metadata(self):
        message = self.message
        original_args = getattr(message, 'args', None)
        original_kwargs = getattr(message, 'kwargs', None)
        return {
            'reason': self.reason,
            'task_name': self.task_name,
            'schema_version': self.schema_version,
            'original_schema': getattr(
                self.message, 'original_schema', self.schema_version),
            'supported_version': self.supported_version,
            'original_args': original_args,
            'original_kwargs': original_kwargs,
            'original': self.original,
        }

    @property
    def original_args(self):
        return getattr(self.message, 'args', None)

    @property
    def original_kwargs(self):
        return getattr(self.message, 'kwargs', None)

class CancelExecution(Exception):
    def __init__(self, retry=None, *args, **kwargs):
        self.retry = retry
        super(CancelExecution, self).__init__(*args, **kwargs)
class RetryTask(Exception):
    def __init__(self, msg=None, eta=None, delay=None, *args, **kwargs):
        self.eta, self.delay = eta, delay
        super(RetryTask, self).__init__(msg, *args, **kwargs)
class TaskException(Exception):
    def __init__(self, metadata=None, *args):
        self.metadata = metadata or {}
        super(TaskException, self).__init__(*args)

    def __unicode__(self):
        return self.metadata.get('error') or 'unknown error'
    __str__ = __unicode__
