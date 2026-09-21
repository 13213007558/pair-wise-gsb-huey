import inspect


class SchemaError(ValueError):
    def __init__(self, reason, *args):
        self.reason = reason
        super(SchemaError, self).__init__(*args)


class TaskSchema(object):
    def __init__(self, version=1, migrations=None):
        if (isinstance(version, bool) or not isinstance(version, int) or
                version < 1):
            raise ValueError('schema version must be a positive integer')

        self.version = version
        self.migrations = {}
        for source, migration in (migrations or {}).items():
            if (isinstance(source, bool) or not isinstance(source, int) or
                    source < 1):
                raise ValueError('migration source version must be a '
                                 'positive integer')
            if not callable(migration):
                raise ValueError('migration for version %s must be callable' %
                                 source)
            self.migrations[source] = migration

        self.signature = None

    def bind(self, func, context=False):
        while getattr(func, '__wrapped__', None) is not None:
            func = func.__wrapped__
        self.signature = inspect.signature(func)
        self.context = context

    def normalize_data(self, args, kwargs):
        if not isinstance(args, tuple):
            if isinstance(args, list):
                args = tuple(args)
            else:
                raise SchemaError(
                    'invalid-args', 'task args must be a tuple or list')
        if not isinstance(kwargs, dict):
            raise SchemaError('invalid-kwargs', 'task kwargs must be a dict')
        return args, kwargs

    def migrate(self, version, args, kwargs):
        args, kwargs = self.normalize_data(args, kwargs)
        if version is None:
            version = 1

        if (isinstance(version, bool) or not isinstance(version, int) or
                version < 1):
            raise SchemaError(
                'invalid-schema-version',
                'message schema version must be a positive integer')
        if version > self.version:
            raise SchemaError(
                'unsupported-schema-version',
                'message schema version %s is newer than supported version '
                '%s' % (version, self.version))

        while version < self.version:
            migration = self.migrations.get(version)
            if migration is None:
                raise SchemaError(
                    'missing-migration',
                    'no migration registered from schema version %s' % version)
            try:
                result = migration(args, kwargs)
            except SchemaError:
                raise
            except Exception as exc:
                raise SchemaError('migration-error', str(exc))
            if result is None:
                args, kwargs = (), {}
            elif isinstance(result, dict):
                args, kwargs = (), result
            elif (isinstance(result, (tuple, list)) and len(result) == 2 and
                  isinstance(result[0], (tuple, list)) and
                  isinstance(result[1], dict)):
                args, kwargs = tuple(result[0]), result[1]
            else:
                raise SchemaError(
                    'invalid-migration-result',
                    'migration from schema version %s must return '
                    '(args, kwargs) or a dict' % version)
            args, kwargs = self.normalize_data(args, kwargs)
            version += 1

        self.validate(version, args, kwargs)
        return version, args, kwargs

    def validate(self, version, args, kwargs):
        validation_kwargs = dict(kwargs)
        if (getattr(self, 'context', False) and
                'task' in self.signature.parameters):
            validation_kwargs['task'] = None
        try:
            self.signature.bind(*args, **validation_kwargs)
        except TypeError as exc:
            message = str(exc)
            if 'keyword argument' in message:
                reason = 'unknown-keyword'
            elif 'argument' in message:
                reason = 'invalid-arguments'
            else:
                reason = 'invalid-arguments'
            raise SchemaError(reason, message)
