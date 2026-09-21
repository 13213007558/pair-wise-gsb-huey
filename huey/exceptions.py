class HueyException(Exception): pass
class ConfigurationError(HueyException): pass
class TaskLockedException(HueyException): pass
class ResultTimeout(HueyException): pass

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

class TaskSchemaError(HueyException):
    def __init__(self, reason, schema=None, received_version=None,
                 effective_version=None, version=None, label=None,
                 field=None, fields=None, detail=None,
                 original_exception=None):
        self.reason = reason
        self.schema = schema
        self.received_version = received_version
        self.effective_version = effective_version
        self.version = version
        self.label = label
        self.field = field
        self.fields = fields
        self.detail = detail
        self.original_exception = original_exception
        current = getattr(schema, 'version', None)
        super(TaskSchemaError, self).__init__(
            '%s for schema version %s (received=%r): %s' % (
                reason, current, received_version, detail or reason))
