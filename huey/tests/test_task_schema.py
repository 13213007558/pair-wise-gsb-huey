import logging
import pickle
import unittest
import uuid

from huey.api import MemoryHuey
from huey.exceptions import MessageDecodeError
from huey.registry import Message
from huey.schema import TaskSchema
from huey.serializer import SignedJSONSerializer
from huey.serializer import JSONSerializer
from huey.signals import SIGNAL_MESSAGE_REJECTED


class TaskSchemaTestCase(unittest.TestCase):
    def setUp(self):
        self.huey = MemoryHuey(utc=False)
        self._logger_level = logging.getLogger('huey').level
        logging.getLogger('huey').setLevel(logging.CRITICAL)

    def tearDown(self):
        logging.getLogger('huey').setLevel(self._logger_level)

    def message(self, task, args=(), kwargs=None, schema=None):
        instance = task.s(*args, **(kwargs or {}))
        base = task.huey._registry.create_message(instance)
        return Message(
            base.id,
            base.name,
            base.eta,
            base.retries,
            base.retry_delay,
            base.priority,
            args,
            kwargs or {},
            base.on_complete,
            base.on_error,
            base.expires,
            base.expires_resolved,
            schema=schema)

    def create_task(self, task, *args, **kwargs):
        message = self.message(task, args, kwargs)
        return self.huey._registry.create_task(message)

    def test_migrates_positional_and_keyword_fields(self):
        @self.huey.task(schema=TaskSchema(
            version=2,
            migrations={1: self._migrate_task_a_v1}))
        def task_a(old_field, new_field=None):
            return old_field, new_field

        positional = self.create_task(task_a, 'old', 'migrated')
        self.assertEqual(positional.args, ('old',))
        self.assertEqual(positional.kwargs, {'new_field': 'migrated'})
        self.assertEqual(positional.schema_version, 2)
        self.assertEqual(positional.original_args, ('old', 'migrated'))

        keyword = self.create_task(
            task_a,
            old_field='old',
            new_field='new')
        self.assertEqual(keyword.args, ('old',))
        self.assertEqual(keyword.kwargs, {'new_field': 'new'})

    def test_adds_and_removes_fields_through_migration(self):
        @self.huey.task(schema=TaskSchema(
            version=2,
            migrations={1: self._migrate_task_b_v1}))
        def task_a(value, added=None, removed=None):
            return value, added

        task = self.create_task(task_a, value='x', removed='gone')
        self.assertEqual(
            task.original_kwargs, {'value': 'x', 'removed': 'gone'})
        self.assertEqual(task.kwargs, {'value': 'x', 'added': 'default'})

    def _migrate_task_a_v1(self, args, kwargs):
        if args:
            return (args[0],), {'new_field': args[1]}
        return (kwargs.pop('old_field'),), kwargs

    def _migrate_task_b_v1(self, args, kwargs):
        kwargs.pop('removed', None)
        kwargs['added'] = 'default'
        return args, kwargs

    def test_rejects_unknown_fields(self):
        @self.huey.task(schema=TaskSchema(version=1))
        def task_a(value):
            return value

        message = self.message(task_a, kwargs={'value': 1, 'unknown': 2},
                               schema=1)
        with self.assertRaises(MessageDecodeError) as captured:
            self.huey._registry.create_task(message)

        error = captured.exception
        self.assertEqual(error.reason, 'unknown-keyword')
        self.assertIs(error.message, message)
        self.assertEqual(error.task_name, message.name)
        self.assertEqual(error.schema_version, 1)
        self.assertEqual(error.original_args, ())
        self.assertEqual(error.original_kwargs, {'value': 1, 'unknown': 2})

    def test_rejects_unregistered_and_unsupported_versions(self):
        @self.huey.task(schema=TaskSchema(
            version=2,
            migrations={1: lambda args, kwargs: (args, kwargs)}))
        def task_a(value):
            return value

        future = self.message(task_a, kwargs={'value': 1}, schema=3)
        with self.assertRaises(MessageDecodeError) as captured:
            self.huey._registry.create_task(future)
        self.assertEqual(captured.exception.reason,
                         'unsupported-schema-version')

        task_a.unregister()
        with self.assertRaises(MessageDecodeError) as captured:
            self.huey._registry.create_task(future)
        self.assertEqual(captured.exception.reason, 'task-not-registered')

    def test_rejects_missing_and_failing_migrations(self):
        @self.huey.task(schema=TaskSchema(
            version=3,
            migrations={2: lambda args, kwargs: (args, kwargs)}))
        def task_a(value):
            return value

        missing = self.message(task_a, kwargs={'value': 1}, schema=1)
        with self.assertRaises(MessageDecodeError) as captured:
            self.huey._registry.create_task(missing)
        self.assertEqual(captured.exception.reason, 'missing-migration')

        def boom(args, kwargs):
            raise ValueError('boom')

        @self.huey.task(name='task_b', schema=TaskSchema(
            version=2,
            migrations={1: boom}))
        def task_b(value):
            return value

        failing = self.message(task_b, kwargs={'value': 1}, schema=1)
        with self.assertRaises(MessageDecodeError) as captured:
            self.huey._registry.create_task(failing)
        error = captured.exception
        self.assertEqual(error.reason, 'migration-error')
        self.assertEqual(str(error.original), 'boom')

    def test_worker_requeues_rejected_message_once_without_execution(self):
        executed = []

        @self.huey.task(schema=TaskSchema(version=1))
        def task_a(value):
            executed.append(value)
            return value

        message = self.message(task_a, kwargs={'value': 1, 'unknown': 2},
                               schema=1)
        raw = self.huey.serializer.serialize(message)
        self.huey.storage.enqueue(raw)
        original_size = self.huey.pending_count()

        rejections = []
        @self.huey.signal(SIGNAL_MESSAGE_REJECTED)
        def on_rejected(signal, task, error):
            rejections.append((signal, task, error.reason))

        with self.assertRaises(MessageDecodeError):
            self.huey.deserialize_task(raw)

        self.huey.dequeue = lambda: (_ for _ in ()).throw(
            AssertionError('raw path must decode before returning task'))

        from huey.consumer import Worker
        worker = Worker(self.huey, 0.001, 0.001, 1)
        worker.loop()

        self.assertEqual(executed, [])
        self.assertEqual(self.huey.pending_count(), original_size)
        self.assertEqual(self.huey.storage.enqueued_items(), [raw])
        self.assertEqual(rejections, [
            (SIGNAL_MESSAGE_REJECTED, None, 'unknown-keyword')])

    def test_task_retry_and_result_paths_remain_unchanged(self):
        attempts = []

        @self.huey.task(
            retries=1,
            schema=TaskSchema(
                version=2,
                migrations={1: lambda args, kwargs: (
                    (),
                    dict(kwargs, should_retry=kwargs.pop('should_fail')))}))
        def task_a(should_retry):
            attempts.append(should_retry)
            if should_retry:
                raise ValueError('task failure')
            return 'ok'

        old_message = self.message(
            task_a,
            kwargs={'should_fail': True},
            schema=1)
        self.huey.storage.enqueue(self.huey.serializer.serialize(old_message))
        first = self.huey.dequeue()
        self.assertEqual(first.kwargs, {'should_retry': True})
        self.assertEqual(first.original_kwargs, {'should_fail': True})
        self.assertEqual(first.original_schema_version, 1)
        self.assertIsNone(self.huey.execute(first))
        self.assertEqual(self.huey.pending_count(), 1)

        retry = self.huey.dequeue()
        self.assertEqual(retry.retries, 0)
        self.assertEqual(retry.schema_version, 2)
        self.assertEqual(retry.original_schema_version, 1)
        self.assertEqual(retry.original_kwargs, {'should_fail': True})
        self.assertEqual(retry.kwargs, {'should_retry': True})
        retry.kwargs['should_retry'] = False
        self.assertEqual(self.huey.execute(retry), 'ok')
        self.assertEqual(attempts, [True, False])

    def test_same_task_name_in_different_modules_conflicts_by_default(self):
        class One(self.huey.task_wrapper_class.task_base):
            pass
        One.__name__ = 'shared'
        One.__module__ = 'module_one'

        class Two(self.huey.task_wrapper_class.task_base):
            pass
        Two.__name__ = 'shared'
        Two.__module__ = 'module_two'

        self.assertTrue(self.huey._registry.register(One))
        with self.assertRaises(ValueError):
            self.huey._registry.register(Two)

        permissive = MemoryHuey(name='permissive', name_collision='allow')
        self.assertTrue(permissive._registry.register(One))
        self.assertTrue(permissive._registry.register(Two))

    def test_pickle_wire_compatibility(self):
        @self.huey.task(schema=TaskSchema(version=2))
        def task_a(value):
            return value

        current = self.message(task_a, kwargs={'value': 1}, schema=2)
        restored = pickle.loads(pickle.dumps(current))
        self.assertIsInstance(restored, Message)
        self.assertEqual(restored.schema, 2)

        from collections import namedtuple
        import io
        import types
        old_registry = types.ModuleType('huey.registry')
        old_registry.Message = namedtuple('Message', Message._fields)
        import sys

        class OldWorkerUnpickler(pickle.Unpickler):
            def find_class(self, module, name):
                if module == 'huey.registry' and name == 'Message':
                    return old_registry.Message
                return super(OldWorkerUnpickler, self).find_class(
                    module, name)

        @self.huey.task()
        def unversioned(value):
            return value

        old_message = self.message(unversioned, kwargs={'value': 1})
        old_raw = pickle.dumps(old_message)
        decoded = OldWorkerUnpickler(io.BytesIO(old_raw)).load()
        self.assertEqual(decoded.kwargs, {'value': 1})
        self.assertFalse(hasattr(decoded, 'schema'))

        versioned_raw = pickle.dumps(current)
        with self.assertRaises(AttributeError):
            OldWorkerUnpickler(io.BytesIO(versioned_raw)).load()
        self.assertIn(b'schema', versioned_raw)

    def test_json_serializer_migrates_and_rejects_enveloped_message(self):
        serializer = JSONSerializer()
        self.huey.serializer = serializer

        @self.huey.task(schema=TaskSchema(
            version=2,
            migrations={1: lambda args, kwargs: (
                args,
                dict(kwargs, added='default'))}))
        def task_a(value, added=None):
            return value, added

        old_message = self.message(task_a, kwargs={'value': 1}, schema=1)
        raw = serializer.serialize(old_message)
        task = self.huey.deserialize_task(raw)
        self.assertEqual(task.kwargs, {'value': 1, 'added': 'default'})
        self.assertEqual(task.schema_version, 2)
        self.assertEqual(self.huey.execute(task), (1, 'default'))

        invalid_message = self.message(
            task_a,
            kwargs={'value': 1, 'unknown': 2},
            schema=2)
        with self.assertRaises(MessageDecodeError) as captured:
            self.huey.deserialize_task(
                serializer.serialize(invalid_message))
        self.assertEqual(captured.exception.reason, 'unknown-keyword')

        legacy_tuple = serializer._serialize(tuple(old_message))
        with self.assertRaises(MessageDecodeError) as captured:
            self.huey.deserialize_task(legacy_tuple)
        self.assertEqual(captured.exception.reason, 'invalid-message')

        pipeline = task_a.s(value=1).then(task_a)
        pipeline.on_complete.kwargs.pop('task', None)
        roundtrip = serializer.deserialize(serializer.serialize(
            self.huey._registry.create_message(pipeline)))
        self.assertIsInstance(roundtrip, Message)
        self.assertIsInstance(roundtrip.on_complete, Message)
        self.assertIsInstance(roundtrip.args, list)

        signed = SignedJSONSerializer(secret='secret', compression=True)
        signed_message = signed.deserialize(
            signed.serialize(self.huey._registry.create_message(
                task_a.s(value=2, added='signed'))))
        self.assertEqual(signed_message.kwargs, {'value': 2, 'added': 'signed'})

    def test_schema_supports_context_task(self):
        @self.huey.task(context=True, schema=TaskSchema(version=1))
        def task_a(value, task):
            return value, task.schema_version, task.id

        task = self.create_task(task_a, value='context')
        self.assertEqual(self.huey.execute(task)[:2], ('context', 1))

    def test_variable_arguments_accept_only_declared_dynamic_fields(self):
        @self.huey.task(schema=TaskSchema(version=1))
        def task_a(value, **extra):
            return value, extra

        task = self.create_task(task_a, value=1, dynamic='allowed')
        self.assertEqual(task.execute(), (1, {'dynamic': 'allowed'}))

    def test_rejected_message_survives_restart_with_original_evidence(self):
        from huey.api import SqliteHuey

        filename = 'huey-schema-test-%s.db' % uuid.uuid4().hex

        try:
            huey = SqliteHuey(filename=filename, results=False, utc=False)

            @huey.task(schema=TaskSchema(
                version=2,
                migrations={1: lambda args, kwargs: (_ for _ in ()).throw(
                    ValueError('disk-failure'))}))
            def task_a(value):
                return value

            message = self.message(task_a, kwargs={'value': 1}, schema=1)
            raw = huey.serializer.serialize(message)
            huey.storage.enqueue(raw)

            from huey.consumer import Worker
            Worker(huey, 0.001, 0.001, 1).loop()
            self.assertEqual(huey.storage.enqueued_items(), [raw])

            restarted = SqliteHuey(
                filename=filename,
                results=False,
                utc=False)

            @restarted.task(schema=TaskSchema(
                version=2,
                migrations={1: lambda args, kwargs: (_ for _ in ()).throw(
                    ValueError('disk-failure'))}))
            def task_a(value):
                return value

            with self.assertRaises(MessageDecodeError) as captured:
                restarted.deserialize_task(
                    restarted.storage.enqueued_items()[0])
            self.assertEqual(captured.exception.reason, 'migration-error')
            self.assertEqual(captured.exception.message, message)
        finally:
            import os
            for suffix in ('', '-wal', '-shm'):
                if os.path.exists(filename + suffix):
                    os.unlink(filename + suffix)

    def test_old_and_new_messages_interleave_without_silent_execution(self):
        state = []

        @self.huey.task(schema=TaskSchema(
            version=2,
            migrations={1: lambda args, kwargs: (
                args,
                dict(kwargs, new=kwargs.pop('old')))}))
        def task_a(new, renamed=None):
            state.append((new, renamed))
            return new, renamed

        old = self.message(task_a, kwargs={'old': 'old-value'}, schema=1)
        current = self.message(
            task_a,
            kwargs={'new': 'new-value', 'renamed': 'renamed'},
            schema=2)
        future = self.message(
            task_a,
            kwargs={'new': 'future', 'renamed': 'x'},
            schema=3)
        invalid = self.message(
            task_a,
            kwargs={'new': 'bad', 'renamed': 'x', 'unknown': True},
            schema=2)

        migrated = self.huey._registry.create_task(old)
        self.assertEqual(migrated.kwargs, {'new': 'old-value'})
        self.huey.execute(migrated)
        self.huey.execute(self.huey._registry.create_task(current))

        for message, reason in ((future, 'unsupported-schema-version'),
                                (invalid, 'unknown-keyword')):
            with self.assertRaises(MessageDecodeError) as captured:
                self.huey._registry.create_task(message)
            self.assertEqual(captured.exception.reason, reason)
            self.assertEqual(captured.exception.message, message)
        self.assertEqual(state, [
            ('old-value', None),
            ('new-value', 'renamed')])


if __name__ == '__main__':
    unittest.main()
