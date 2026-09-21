"""Task payload schema versioning and migration helpers."""
import inspect
import copy

from huey.exceptions import ConfigurationError
from huey.exceptions import TaskSchemaError


class SchemaSignature(object):
    def __init__(self, names, var_args=False, var_kwargs=False,
                 required=None, has_context=False, positional_names=None):
        self.names = names
        self.var_args = var_args
        self.var_kwargs = var_kwargs
        self.required = required or set()
        self.has_context = has_context
        self.positional_names = positional_names or names


def _get_signature(func):
    try:
        return inspect.signature(func)
    except AttributeError:
        return None


def _signature_fields(func, context=False, fields=None,
                      allow_extra_args=None, allow_extra_kwargs=None):
    signature = _get_signature(func)
    defaults = set()
    names = []
    positional_names = []
    kwonly = set()
    var_args = False
    var_kwargs = False

    if signature is not None:
        for parameter in signature.parameters.values():
            kind = parameter.kind
            if kind == inspect.Parameter.VAR_POSITIONAL:
                var_args = True
                continue
            elif kind == inspect.Parameter.VAR_KEYWORD:
                var_kwargs = True
                continue

            if context and parameter.name == 'task':
                continue

            names.append(parameter.name)
            if kind in (inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD):
                positional_names.append(parameter.name)
            if kind == inspect.Parameter.KEYWORD_ONLY:
                kwonly.add(parameter.name)
            if parameter.default is not inspect.Parameter.empty:
                defaults.add(parameter.name)
    else:
        try:
            spec = inspect.getfullargspec(func)
        except AttributeError:
            spec = inspect.getargspec(func)
        arg_names = list(spec.args)
        if context and 'task' in arg_names:
            arg_names.remove('task')
        names = arg_names
        var_args = bool(spec.varargs)
        var_kwargs = bool(spec.keywords if hasattr(spec, 'keywords')
                          else spec.varkw)
        if spec.defaults:
            defaults.update(arg_names[len(arg_names) - len(spec.defaults):])
        if hasattr(spec, 'kwonlydefaults') and spec.kwonlydefaults:
            defaults.update(spec.kwonlydefaults)
        if hasattr(spec, 'kwonlyargs'):
            kwonly.update(spec.kwonlyargs)
        positional_names = list(names)

    if fields is None:
        allow_args = var_args and allow_extra_args is True
        allow_kwargs = var_kwargs and allow_extra_kwargs is True
    else:
        names = list(fields)
        positional_names = list(fields)
        allow_args = allow_extra_args is True
        allow_kwargs = allow_extra_kwargs is True

    if fields is None:
        required = set(name for name in names if name not in defaults)
    else:
        required = set(names)
    return SchemaSignature(names, allow_args, allow_kwargs, required, context,
                           positional_names)


class TaskSchema(object):
    """Declare a task payload version and migrations for older versions."""
    def __init__(self, version, migrations=None, fields=None,
                 allow_extra_args=None, allow_extra_kwargs=None):
        if not isinstance(version, int) or isinstance(version, bool) or \
                version < 1:
            raise ConfigurationError('Schema version must be a positive '
                                     'integer.')

        self.version = version
        self.migrations = dict(migrations or ())
        if any(not isinstance(version, int) or isinstance(version, bool) or
               version < 1 for version in self.migrations):
            raise ConfigurationError('Schema migration versions must be '
                                     'positive integers.')

        self.declared_fields = fields
        self.allow_extra_args = allow_extra_args
        self.allow_extra_kwargs = allow_extra_kwargs
        self.func = None
        self.context = False
        self.final_signature = None
        self.migration_signatures = {}

    def bind(self, func, context=False):
        bound = copy.copy(self)
        extra = set(self.migrations) - set(range(1, self.version))
        if extra:
            raise ConfigurationError('Cannot register migrations for versions '
                                     'that do not precede the current schema '
                                     'version: %s' % ', '.join(map(str, sorted(
                                             extra))))

        bound.func = func
        bound.context = context
        bound.final_signature = _signature_fields(
            func, context, bound.declared_fields, bound.allow_extra_args,
            bound.allow_extra_kwargs)
        bound.migration_signatures = dict(
            (version, _signature_fields(migration))
            for version, migration in bound.migrations.items())
        return bound

    def validate_data(self, args, kwargs, signature, version, label):
        if not isinstance(args, (tuple, list)):
            raise TaskSchemaError('invalid_args', schema=self, version=version,
                                  label=label, detail='args must be a sequence')
        if not isinstance(kwargs, dict):
            raise TaskSchemaError('invalid_kwargs', schema=self,
                                  version=version, label=label,
                                  detail='kwargs must be a dict')

        positional_values = []
        keyword_values = {}
        for index, value in enumerate(args):
            if index < len(signature.positional_names):
                name = signature.positional_names[index]
                if name in kwargs:
                    raise TaskSchemaError('duplicate_field', schema=self,
                                          version=version, label=label,
                                          field=name)
                positional_values.append(value)
            elif signature.var_args:
                positional_values.append(value)
            else:
                raise TaskSchemaError('unexpected_positional', schema=self,
                                      version=version, label=label,
                                      field=index)

        for name in kwargs:
            if name in signature.names:
                keyword_values[name] = kwargs[name]
            elif name == 'task' and signature.has_context:
                raise TaskSchemaError('reserved_field', schema=self,
                                      version=version, label=label,
                                      field=name)
            elif not signature.var_kwargs:
                raise TaskSchemaError('unknown_field', schema=self,
                                      version=version, label=label,
                                      field=name)

        bound = dict((signature.positional_names[index], value)
                     for index, value in enumerate(positional_values)
                     if index < len(signature.positional_names))
        bound.update(keyword_values)
        missing = sorted(name for name in signature.required
                         if name not in bound)
        if missing:
            raise TaskSchemaError('missing_fields', schema=self,
                                  version=version, label=label,
                                  fields=missing)

        return tuple(positional_values), keyword_values

    def _normalized_result(self, result, version):
        if not isinstance(result, tuple) or len(result) != 2 or \
                not isinstance(result[0], (tuple, list)) or \
                not isinstance(result[1], dict):
            raise TaskSchemaError('invalid_migration_result', schema=self,
                                  version=version,
                                  detail='migration must return (args, '
                                         'kwargs)')
        return tuple(result[0]), result[1]

    def migrate_task(self, task):
        received_version = task.message_schema_version
        effective_version = 1 if received_version is None else received_version
        if not isinstance(effective_version, int) or \
                isinstance(effective_version, bool) or effective_version < 1:
            raise TaskSchemaError('invalid_version', schema=self,
                                  received_version=received_version,
                                  detail='schema version must be a positive '
                                         'integer')
        if effective_version > self.version:
            raise TaskSchemaError('unsupported_version', schema=self,
                                  received_version=received_version,
                                  effective_version=effective_version,
                                  detail='message was produced by a newer '
                                         'schema version')

        args = tuple(task.original_args)
        kwargs = dict(task.original_kwargs)
        current_version = effective_version

        try:
            while current_version < self.version:
                migration = self.migrations.get(current_version)
                if migration is None:
                    raise TaskSchemaError(
                        'unregistered_version', schema=self,
                        received_version=received_version,
                        effective_version=effective_version,
                        detail='no migration registered for version %s' %
                               current_version)

                signature = self.migration_signatures[current_version]
                self.validate_data(args, kwargs, signature,
                                   current_version, 'migration')
                result = migration(*args, **kwargs)
                args, kwargs = self._normalized_result(result, current_version)
                next_version = current_version + 1
                current_version = next_version \
                    if next_version in self.migrations else self.version
                next_signature = self.migration_signatures.get(
                    current_version, self.final_signature)
                self.validate_data(args, kwargs, next_signature,
                                   current_version, 'migration')

            args, kwargs = self.validate_data(
                args, kwargs, self.final_signature,
                current_version, 'task')
        except TaskSchemaError:
            raise
        except Exception as exc:
            raise TaskSchemaError('migration_failed', schema=self,
                                  received_version=received_version,
                                  effective_version=effective_version,
                                  detail=str(exc), original_exception=exc)

        task.args = args
        task.kwargs = kwargs
        task.schema_migrated = effective_version != self.version
        task.schema_effective_version = current_version
