import pickle
import datetime

from huey import MemoryHuey
from huey import TaskSchema
from huey.exceptions import TaskSchemaError
from huey.registry import VersionedMessage
from huey.registry import Message
from huey.serializer import Serializer
from huey.serializer import JsonSerializer
from huey.serializer import SignedJsonSerializer
from huey.tests.base import BaseTestCase


class TestTaskSchema(BaseTestCase):
    def get_huey(self):
        return MemoryHuey(utc=False, serializer=JsonSerializer())

    def make_task(self, retries=0):
        def migrate_v1(old_value):
            return ((), {'new_value': old_value + 10})

        schema = TaskSchema(2, {1: migrate_v1})

        @self.huey.task(retries=retries, schema=schema)
        def task_v2(new_value, added=None):
            return new_value + (added or 0)

        return task_v2

    def test_old_positional_and_keyword_payloads_migrate(self):
        task_v2 = self.make_task()
        migrated = []
        self.huey.signal('schema-migrated')(
            lambda signal, task: migrated.append((task.id,
                                                  task.message_schema_version,
                                                  task.schema_effective_version)))

        positional = task_v2.task_class((1,), {}, id='p1',
                                        message_schema_version=1)
        self.assertEqual(self.huey.execute(positional), 11)
        self.assertTrue(positional.schema_migrated)
        self.assertEqual(positional.original_args, (1,))
        self.assertEqual(positional.args, ())
        self.assertEqual(positional.kwargs, {'new_value': 11})

        keyword = task_v2.task_class((), {'old_value': 2}, id='k1',
                                     message_schema_version=1)
        self.assertEqual(self.huey.execute(keyword), 12)
        self.assertEqual(keyword.original_kwargs, {'old_value': 2})
        self.assertEqual(keyword.kwargs, {'new_value': 12})
        self.assertEqual(migrated, [('p1', 1, 2), ('k1', 1, 2)])

    def test_current_and_old_messages_interleave_without_confusion(self):
        task_v2 = self.make_task()
        new_task = task_v2.s(new_value=3, added=1)
        old_message = Message('old', task_v2.task_class.__module__ + '.' +
                              task_v2.task_class.__name__, None, 0, 0, None,
                              (1,), {}, None, None, None, None)

        self.assertEqual(self.huey.serializer.__class__.__name__,
                         'JsonSerializer')
        old_raw = self.huey.serializer.serialize(old_message)
        new_raw = self.huey.serialize_task(new_task)
        self.huey.storage.enqueue(old_raw)
        self.huey.storage.enqueue(new_raw)

        first = self.huey.dequeue()
        second = self.huey.dequeue()
        self.assertIsNone(first.message_schema_version)
        self.assertEqual(second.message_schema_version, 2)
        self.assertEqual(self.huey.execute(first), 11)
        self.assertEqual(self.huey.execute(second), 4)

    def test_unknown_field_is_rejected(self):
        task_v2 = self.make_task()
        rejected = []
        self.huey.signal('schema-rejected')(
            lambda signal, task, exc: rejected.append((task.id, exc.reason)))

        task = task_v2.task_class((), {'new_value': 1, 'unknown': 2}, id='r1',
                                  message_schema_version=2)
        self.assertIsNone(self.huey.execute(task))
        self.assertEqual(rejected, [('r1', 'unknown_field')])
        self.assertEqual(task.original_kwargs, {'new_value': 1, 'unknown': 2})
        self.assertEqual(task.kwargs, {'new_value': 1, 'unknown': 2})

    def test_migration_failure_requeues_original_message_once(self):
        task_v2 = self.make_task(retries=0)
        enqueued = []
        original_enqueue = self.huey.storage.enqueue

        def track_enqueue(data, priority=None):
            enqueued.append(data)
            return original_enqueue(data, priority)

        self.huey.storage.enqueue = track_enqueue

        def fail(old_value):
            raise ValueError('cannot migrate %s' % old_value)

        task_v2.task_class.task_schema.migrations[1] = fail
        task_v2.task_class.task_schema.migration_signatures[1] = \
            task_v2.task_class.task_schema.migration_signatures[1]

        task = task_v2.task_class((7,), {}, id='fail1',
                                  message_schema_version=1)
        self.assertIsNone(self.huey.execute(task))
        self.assertEqual(len(enqueued), 1)
        retried = self.huey.deserialize_task(enqueued[0])
        self.assertEqual(retried.message_schema_version, 1)
        self.assertEqual(retried.original_args, (7,))
        self.assertEqual(retried.retries, 0)
        self.assertEqual(task.retries, 0)

    def test_restart_preserves_versioned_envelope(self):
        task_v2 = self.make_task()
        task = task_v2.s(new_value=5)
        raw = self.huey.serialize_task(task)
        decoded = self.huey.serializer.deserialize(raw)
        self.assertIsInstance(decoded, VersionedMessage)
        self.assertEqual(decoded.version, 2)

        restarted_huey = MemoryHuey(utc=False, serializer=JsonSerializer())
        restarted_huey._registry.register(task_v2.task_class)
        restarted_task = restarted_huey.deserialize_task(raw)
        self.assertEqual(restarted_task.message_schema_version, 2)
        self.assertEqual(restarted_huey.execute(restarted_task), 5)

    def test_unregistered_task_and_version_are_explicit(self):
        from huey.registry import Message
        unknown = Message('missing.task', 'missing.task', None, 0, 0, None,
                          (), {}, None, None, None, None)
        with self.assertRaisesRegex(Exception, 'not found in TaskRegistry'):
            self.huey.deserialize_task(
                    self.huey.serializer.serialize(unknown))

        invalid = VersionedMessage(9, None)
        with self.assertRaisesRegex(Exception, 'Task message payload is '
                                               'invalid'):
            self.huey.deserialize_task(
                    self.huey.serializer.serialize(invalid))

        task_v2 = self.make_task()
        future = task_v2.task_class((), {'new_value': 1}, id='future',
                                    message_schema_version=3)
        with self.assertRaises(TaskSchemaError) as cm:
            future.migrate_schema()
        self.assertEqual(cm.exception.reason, 'unsupported_version')

    def test_migration_rejection_does_not_store_task_error(self):
        task_v2 = self.make_task(retries=0)
        rejected = []
        self.huey.signal('schema-rejected')(
            lambda signal, task, exc: rejected.append(exc.reason))

        task = task_v2.task_class((), {'new_value': 1, 'removed': 2},
                                  id='invalid', message_schema_version=2)
        self.assertIsNone(self.huey.execute(task))
        self.assertEqual(rejected, ['unknown_field'])
        self.assertEqual(self.huey.result_count(), 0)
        self.assertEqual(len(self.huey), 1)

    def test_field_removal_requires_migration_to_drop_old_field(self):
        def migrate(old, removed):
            return ((old,), {})

        @self.huey.task(schema=TaskSchema(2, {1: migrate}))
        def renamed(old):
            return old

        task = renamed.task_class((1, 2), {}, id='removed',
                                  message_schema_version=1)
        self.assertEqual(self.huey.execute(task), 1)
        self.assertEqual(task.original_args, (1, 2))
        self.assertEqual(task.args, (1,))

    def test_migration_chain_applies_each_declared_version(self):
        def v1_to_v2(a):
            return ((), {'b': a + 1})

        def v2_to_v3(b):
            return ((), {'b': b, 'c': b + 1})

        @self.huey.task(schema=TaskSchema(3, {1: v1_to_v2, 2: v2_to_v3}))
        def chained(b, c):
            return b + c

        task = chained.task_class((1,), {}, id='chain',
                                  message_schema_version=1)
        self.assertEqual(self.huey.execute(task), 5)
        self.assertEqual(task.original_args, (1,))
        self.assertEqual(task.kwargs, {'b': 2, 'c': 3})
        self.assertEqual(task.schema_effective_version, 3)

    def test_sparse_migration_can_jump_to_current_version(self):
        def v1_to_v3(old):
            return ((), {'new': old + 2})

        @self.huey.task(schema=TaskSchema(3, {1: v1_to_v3}))
        def jumped(new):
            return new

        task = jumped.task_class((1,), {}, id='jump',
                                 message_schema_version=1)
        self.assertEqual(self.huey.execute(task), 3)
        self.assertEqual(task.schema_effective_version, 3)

        intermediate = jumped.task_class((1,), {}, id='intermediate',
                                         message_schema_version=2)
        with self.assertRaises(TaskSchemaError) as cm:
            intermediate.migrate_schema()
        self.assertEqual(cm.exception.reason, 'unregistered_version')

    def test_json_and_pickle_round_trip(self):
        json_task = self.make_task()
        task = json_task.s(new_value=4)
        decoded = self.huey.deserialize_task(self.huey.serialize_task(task))
        self.assertIsInstance(
                self.huey.serializer.deserialize(
                        self.huey.serialize_task(task)),
                VersionedMessage)
        self.assertEqual(decoded.message_schema_version, 2)
        self.assertEqual(decoded.kwargs, {'new_value': 4})

        pickle_huey = MemoryHuey(utc=False, serializer=Serializer())
        pickle_huey._registry.register(json_task.task_class)
        pickle_task = pickle_huey.deserialize_task(
                pickle_huey.serialize_task(task))
        self.assertEqual(pickle_task.kwargs, {'new_value': 4})

        signed_huey = MemoryHuey(utc=False,
                                 serializer=SignedJsonSerializer(secret='s'))
        signed_huey._registry.register(json_task.task_class)
        signed_task = signed_huey.deserialize_task(
                signed_huey.serialize_task(task))
        self.assertEqual(signed_task.kwargs, {'new_value': 4})

    def test_json_does_not_decode_arbitrary_type(self):
        malicious = {
            JsonSerializer.type_marker: 'arbitrary',
            JsonSerializer.value_marker: 'value',
        }
        raw = JsonSerializer().serialize(malicious)
        with self.assertRaisesRegex(ValueError, 'Unsupported Huey JSON type'):
            JsonSerializer().deserialize(raw)

    def test_json_primitive_extensions_round_trip(self):
        when = datetime.datetime(2026, 1, 2, 3, 4, 5)
        value = {'items': (1, 2), 'set': {3, 4}, 'when': when,
                 'binary': b'abc'}
        serializer = JsonSerializer()
        decoded = serializer.deserialize(serializer.serialize(value))
        self.assertEqual(decoded['items'], (1, 2))
        self.assertEqual(decoded['set'], {3, 4})
        self.assertEqual(decoded['when'], when)
        self.assertEqual(decoded['binary'], b'abc')

    def test_rejection_callback_can_dead_letter_without_requeue(self):
        task_v2 = self.make_task()
        dead_letter = []

        def reject(task, exc):
            dead_letter.append((task.original_kwargs, exc.reason))
            return False

        self.huey.on_schema_rejected(reject)
        task = task_v2.task_class((), {'unknown': 1}, id='dlq',
                                  message_schema_version=2)
        self.assertIsNone(self.huey.execute(task))
        self.assertEqual(dead_letter, [
            ({'unknown': 1}, 'unknown_field'),
        ])
        self.assertEqual(len(self.huey), 0)

    def test_rejection_callback_failure_still_requeues_message(self):
        task_v2 = self.make_task()

        def reject(task, exc):
            raise RuntimeError('dead letter unavailable')

        self.huey.on_schema_rejected(reject)
        task = task_v2.task_class((), {'unknown': 1}, id='callback-fail',
                                  message_schema_version=2)
        self.assertIsNone(self.huey.execute(task))
        self.assertEqual(len(self.huey), 1)
        retried = self.huey.dequeue()
        self.assertEqual(retried.id, 'callback-fail')
        self.assertEqual(retried.original_kwargs, {'unknown': 1})
        self.assertEqual(self.huey.result_count(), 0)

    def test_same_task_name_in_different_modules_is_distinct(self):
        def create(module):
            schema = TaskSchema(1)
            def shared(value):
                return value
            shared.__module__ = module
            shared = self.huey.task(name='shared', schema=schema)(shared)
            return shared

        task_v1 = create('service.v1.tasks')
        task_v2 = create('service.v2.tasks')
        self.assertNotEqual(
                self.huey._registry.task_to_string(task_v1.task_class),
                self.huey._registry.task_to_string(task_v2.task_class))
        self.assertIsNot(
                self.huey._registry.string_to_task(
                        'service.v1.tasks.shared'),
                self.huey._registry.string_to_task(
                        'service.v2.tasks.shared'))

    def test_pickle_versioned_envelope_rejects_on_old_worker(self):
        pickle_huey = MemoryHuey(utc=False, serializer=Serializer())
        task_v2 = self.make_task()
        pickle_huey._registry.register(task_v2.task_class)
        task = task_v2.s(new_value=1)
        raw = pickle_huey.serialize_task(task)
        decoded = pickle.loads(raw)
        self.assertIsInstance(decoded, VersionedMessage)

        class OldWorkerUnpickler(pickle.Unpickler):
            def find_class(self, module, name):
                if name == 'VersionedMessage':
                    raise AttributeError('old worker does not know versioned '
                                         'message envelopes')
                return super(OldWorkerUnpickler, self).find_class(module, name)

        import io
        with self.assertRaises(AttributeError):
            OldWorkerUnpickler(io.BytesIO(raw)).load()
