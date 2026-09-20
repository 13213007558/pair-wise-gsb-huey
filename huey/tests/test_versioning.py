import collections
import datetime
import pickle

from huey.api import MemoryHuey
from huey.utils import ChordConfig
from huey.exceptions import ConfigurationError
from huey.exceptions import TaskMigrationError
from huey.registry import Message
from huey.tests.base import BaseTestCase


# A Message with the original 15 fields (no version), exactly as produced by
# releases that pre-date task versioning.
LEGACY_FIELDS = ('id', 'name', 'eta', 'retries', 'retry_delay', 'priority',
                 'args', 'kwargs', 'on_complete', 'on_error', 'expires',
                 'expires_resolved', 'timeout', 'chord_config',
                 'retry_backoff')
LegacyMessage = collections.namedtuple('Message', LEGACY_FIELDS)


def pickle_legacy_message(message):
    """Pickle a 15-field message using the pre-versioning layout, then
    unpickle it against the current Message class."""
    import huey.registry as registry_module

    LegacyMessage.__module__ = 'huey.registry'
    original = registry_module.Message
    registry_module.Message = LegacyMessage
    try:
        data = pickle.dumps(message)
    finally:
        registry_module.Message = original
    return pickle.loads(data)


# Explicitly registered transforms (module-level keeps tasks picklable).
def add_v0_to_v1(args, kwargs):
    # add(a, b) -> add(values=[a, b])
    a, b = args
    return (), {'values': [a, b] + list(kwargs.get('extra', ()))}


def add_v1_to_v2(args, kwargs):
    # add(values=[...]) -> add(values=[...], doubled=False)
    kwargs['doubled'] = False
    return args, kwargs


def add_v0_to_v1_keep(args, kwargs):
    first, second = args
    return (), {'values': [first, second]}


def add_v1_to_v2_keep(args, kwargs):
    kwargs.setdefault('doubled', False)
    return args, kwargs


def boom_v0(args, kwargs):
    raise ValueError('cannot migrate this payload')


def bad_return_v0(args, kwargs):
    return ['not', 'a', 'tuple']


def pipeline_v0(args, kwargs):
    value, = args
    return (), {'value': value}


def report_v0(args, kwargs):
    return args, dict(kwargs, annotated=True)


class TestTaskVersioning(BaseTestCase):
    def get_huey(self):
        return MemoryHuey(utc=False)

    # -- defaults / round-trip --

    def test_default_version_is_zero(self):
        @self.huey.task(name='tv_default')
        def task_a(a, b):
            return a + b

        self.assertEqual(task_a.version, 0)
        self.assertEqual(task_a.task_class.version, 0)

        task = task_a.s(2, 3)
        message = self.huey._registry.create_message(task)
        self.assertEqual(message.version, 0)

        restored = self.huey._registry.create_task(message)
        self.assertEqual(restored.version, 0)
        self.assertEqual(restored.execute(), 5)

    def test_serialization_roundtrip(self):
        @self.huey.task(version=2, name='tv_add',
                        aliases=('tests.old_add',),
                        migrations={0: add_v0_to_v1_keep,
                                    1: add_v1_to_v2_keep})
        def add(values, doubled=False):
            result = sum(values)
            return result * 2 if doubled else result

        task = add.s([1, 2, 3], doubled=True, id='msg-id', retries=4,
                     retry_delay=7, priority=3, retry_backoff=2,
                     timeout=11)
        task.eta = datetime.datetime(2030, 1, 1)
        task.expires = 60
        task.resolve_expires(False)
        expires_resolved = task.expires_resolved

        data = self.huey.serialize_task(task)
        restored = self.huey.deserialize_task(data)

        self.assertEqual(restored.id, 'msg-id')
        self.assertEqual(restored.args, ([1, 2, 3],))
        self.assertEqual(restored.kwargs, {'doubled': True})
        self.assertEqual(restored.eta, datetime.datetime(2030, 1, 1))
        self.assertEqual(restored.retries, 4)
        self.assertEqual(restored.retry_delay, 7)
        self.assertEqual(restored.retry_backoff, 2)
        self.assertEqual(restored.priority, 3)
        self.assertEqual(restored.timeout, 11)
        self.assertEqual(restored.expires, 60)
        self.assertEqual(restored.expires_resolved, expires_resolved)
        self.assertEqual(restored.execute(), 12)

    # -- old / unversioned messages --

    def test_unversioned_message_treated_as_version_zero(self):
        @self.huey.task(version=2, name='tv_legacy_add',
                        aliases=('tests.legacy_add',),
                        migrations={0: add_v0_to_v1,
                                    1: add_v1_to_v2})
        def add(values, doubled=False):
            result = sum(values)
            return result * 2 if doubled else result

        eta = datetime.datetime(2031, 5, 6)
        expires = datetime.datetime(2032, 1, 1)
        legacy = LegacyMessage(
            'legacy-id', 'tests.legacy_add', eta, 5, 9, 7,
            (2, 8), {'extra': [3]}, None, None, expires, expires, 30,
            None, 4)

        message = pickle_legacy_message(legacy)
        self.assertIsNone(message.version)
        self.assertEqual(len(message._fields), 16)

        task = self.huey._registry.create_task(message)
        self.assertEqual(task.args, ())
        self.assertEqual(task.kwargs,
                         {'values': [2, 8, 3], 'doubled': False})
        self.assertEqual(task.execute(), 13)

        self.assertEqual(task.id, 'legacy-id')
        self.assertEqual(task.eta, eta)
        self.assertEqual(task.retries, 5)
        self.assertEqual(task.retry_delay, 9)
        self.assertEqual(task.retry_backoff, 4)
        self.assertEqual(task.priority, 7)
        self.assertEqual(task.expires, expires)
        self.assertEqual(task.expires_resolved, expires)
        self.assertEqual(task.timeout, 30)

        # The migration never mutates the original message.
        self.assertEqual(legacy.args, (2, 8))
        self.assertEqual(legacy.kwargs, {'extra': [3]})

    def test_legacy_message_through_queue(self):
        @self.huey.task(version=1, name='tv_queued',
                        aliases=('tests.queued_v0',),
                        migrations={0: add_v0_to_v1_keep})
        def add(values):
            return sum(values)

        legacy = LegacyMessage(
            'queued-id', 'tests.queued_v0', None, 0, 0, None,
            (40, 2), {}, None, None, None, None, None, None, None)
        message = pickle_legacy_message(legacy)
        data = self.huey.serializer.serialize(message)
        self.huey.storage.enqueue(data)

        task = self.huey.dequeue()
        self.assertEqual(task.id, 'queued-id')
        self.assertEqual(task.execute(), 42)

    # -- aliases --

    def test_alias_resolves_to_current_task(self):
        @self.huey.task(version=1, name='tv_renamed',
                        aliases=('tests.old_name',),
                        migrations={0: add_v0_to_v1_keep})
        def add(values):
            return sum(values)

        message = Message('id', 'tests.old_name', None, 0, 0, None,
                          (1, 9), {}, None, None, None, None, None, None,
                          None, 0)
        task = self.huey._registry.create_task(message)
        self.assertEqual(type(task).__name__, 'tv_renamed')
        self.assertEqual(task.execute(), 10)

    def test_alias_conflicts_are_rejected(self):
        def fn():
            pass

        self.huey.task(name='tv_ac_a',
                       aliases=('tests.ac_old',))(fn)

        # Same alias registered by a second task.
        with self.assertRaises(ValueError):
            self.huey.task(name='tv_ac_b',
                           aliases=('tests.ac_old',))(fn)

        # An alias that claims a registered canonical name.
        with self.assertRaises(ValueError):
            self.huey.task(
                name='tv_ac_c',
                aliases=('huey.tests.test_versioning.tv_ac_a',))(fn)

        # A new canonical name that claims a registered alias.
        with self.assertRaises(ValueError):
            self.huey.task(name='tv_ac_d',
                           aliases=('tests.ac_old',))(fn)

        # Duplicate aliases within one registration.
        with self.assertRaises(ConfigurationError):
            self.huey.task(name='tv_ac_e',
                           aliases=('tests.x', 'tests.x'))(fn)

        # A task cannot alias itself.
        with self.assertRaises(ConfigurationError):
            self.huey.task(
                name='tv_ac_f',
                aliases=('huey.tests.test_versioning.tv_ac_f',))(fn)

        # Two tasks still cannot share the canonical name.
        with self.assertRaises(ValueError):
            self.huey.task(name='tv_ac_a')(fn)

    def test_unregister_frees_alias(self):
        def fn():
            pass

        wrapper = self.huey.task(name='tv_unreg',
                                 aliases=('tests.unreg_old',))(fn)
        self.assertTrue(wrapper.unregister())
        # The alias can be reused once the owner is unregistered.
        self.huey.task(name='tv_unreg2',
                       aliases=('tests.unreg_old',))(fn)

    def test_periodic_alias_schedules_once(self):
        def fn():
            pass

        self.huey.periodic_task(lambda ts: False, name='tv_nightly',
                                aliases=('tests.nightly_v0',))(fn)
        names = sorted(t.name for t in self.huey._registry.periodic_tasks)
        self.assertEqual(names, ['tv_nightly'])

        # An extra (non-periodic) task does not duplicate scheduling.
        self.huey.task(name='tv_nightly_helper')(fn)
        names = sorted(t.name for t in self.huey._registry.periodic_tasks)
        self.assertEqual(names, ['tv_nightly'])

    # -- consecutive upgrades --

    def test_two_consecutive_version_bumps(self):
        calls = []

        def step_0(args, kwargs):
            calls.append(0)
            return add_v0_to_v1_keep(args, kwargs)

        def step_1(args, kwargs):
            calls.append(1)
            return add_v1_to_v2_keep(args, kwargs)

        @self.huey.task(version=2, name='tv_v2',
                        aliases=('tests.v0_name',),
                        migrations={0: step_0, 1: step_1})
        def add(values, doubled=False):
            return sum(values) * (2 if doubled else 1)

        # A v0 message walks both transforms in order.
        v0 = Message('id0', 'tests.v0_name', None, 0, 0, None,
                     (3, 4), {}, None, None, None, None, None, None, None, 0)
        task = self.huey._registry.create_task(v0)
        self.assertEqual(calls, [0, 1])
        self.assertEqual(task.kwargs, {'values': [3, 4], 'doubled': False})
        self.assertEqual(task.execute(), 7)

        # A v1 message only applies the second transform.
        calls[:] = []
        v1 = v0._replace(version=1, args=(),
                         kwargs={'values': [5, 6]})
        task = self.huey._registry.create_task(v1)
        self.assertEqual(calls, [1])
        self.assertEqual(task.execute(), 11)

        # A current-version message applies no transform.
        calls[:] = []
        v2 = v0._replace(version=2,
                         name='huey.tests.test_versioning.tv_v2',
                         args=([9],), kwargs={'doubled': True})
        task = self.huey._registry.create_task(v2)
        self.assertEqual(calls, [])
        self.assertEqual(task.execute(), 18)

    def test_broken_migration_chain_rejected_at_registration(self):
        with self.assertRaises(ConfigurationError):
            self.huey.task(
                version=2,
                migrations={1: add_v1_to_v2_keep})(lambda: None)

        with self.assertRaises(ConfigurationError):
            self.huey.task(version=1, migrations={})(lambda: None)

        # A migration sourced at an invalid version is rejected.
        with self.assertRaises(ConfigurationError):
            self.huey.task(
                version=1,
                migrations={0: add_v0_to_v1_keep,
                            1: add_v1_to_v2_keep})(lambda: None)

        # Non-callable migrations are rejected.
        with self.assertRaises(ConfigurationError):
            self.huey.task(
                version=1, migrations={0: 'not-callable'})(lambda: None)

        with self.assertRaises(ConfigurationError):
            self.huey.task(version=-1)(lambda: None)

    def test_future_version_is_an_error(self):
        @self.huey.task(version=1, name='tv_future',
                        migrations={0: add_v0_to_v1_keep})
        def add(values):
            return sum(values)

        future = Message('id', 'huey.tests.test_versioning.tv_future',
                         None, 0, 0, None, (), {}, None, None, None, None,
                         None, None, None, 99)
        with self.assertRaises(TaskMigrationError) as cm:
            self.huey._registry.create_task(future)
        text = str(cm.exception)
        self.assertIn('future version', text)
        self.assertIn('99', text)

    # -- migration failures --

    def test_migration_exception_is_locatable(self):
        @self.huey.task(version=2, name='tv_boom',
                        aliases=('tests.boom_v0',),
                        migrations={0: boom_v0, 1: add_v1_to_v2_keep})
        def add(values, doubled=False):
            return sum(values)

        message = Message('id', 'tests.boom_v0', None, 0, 0, None,
                          (1, 2), {}, None, None, None, None, None, None,
                          None, 0)
        with self.assertRaises(TaskMigrationError) as cm:
            self.huey._registry.create_task(message)
        text = str(cm.exception)
        self.assertIn('tests.boom_v0', text)
        self.assertIn('version 0', text)
        self.assertIn('cannot migrate this payload', text)
        self.assertIsInstance(cm.exception.__cause__, ValueError)

        # Nothing was executed and the queued payload is untouched.
        self.assertEqual(message.args, (1, 2))
        self.assertEqual(message.kwargs, {})
        self.assertEqual(message.version, 0)

    def test_migration_returning_wrong_shape_fails(self):
        @self.huey.task(version=1, name='tv_badret',
                        aliases=('tests.badret_v0',),
                        migrations={0: bad_return_v0})
        def add(a, b):
            return a + b

        message = Message('id', 'tests.badret_v0', None, 0, 0, None,
                          (1, 2), {}, None, None, None, None, None, None,
                          None, 0)
        with self.assertRaises(TaskMigrationError):
            self.huey._registry.create_task(message)

    def test_failed_migration_does_not_execute_or_fallback(self):
        executed = []

        def flaky(args, kwargs):
            # First transform succeeds, the second raises: no task may run
            # with the intermediate v1 payload.
            return (), {'values': list(args)}

        def explode(args, kwargs):
            raise RuntimeError('second step failed')

        @self.huey.task(version=2, name='tv_flaky',
                        aliases=('tests.flaky_v0',),
                        migrations={0: flaky, 1: explode})
        def add(values):
            executed.append(values)
            return sum(values)

        message = Message('id', 'tests.flaky_v0', None, 0, 0, None,
                          (1, 2), {}, None, None, None, None, None, None,
                          None, 0)
        with self.assertRaises(TaskMigrationError) as cm:
            self.huey._registry.create_task(message)
        self.assertIn('second step failed', str(cm.exception))
        self.assertEqual(executed, [])
        # Original v0 args remain available for inspection/re-queue.
        self.assertEqual(message.args, (1, 2))

    # -- nested pipelines and chords --

    def test_nested_pipeline_migrated_per_own_version(self):
        # Parent is a v2 task; its on_complete/on_error callbacks are two
        # *different* v1 tasks still carrying v0 payloads under old names.
        @self.huey.task(version=2, name='tv_parent',
                        aliases=('tests.parent_v0',),
                        migrations={0: add_v0_to_v1_keep,
                                    1: add_v1_to_v2_keep})
        def parent(values, doubled=False):
            return sum(values)

        @self.huey.task(version=1, name='tv_complete',
                        aliases=('tests.complete_v0',),
                        migrations={0: pipeline_v0})
        def on_complete_task(value=None):
            return ('complete', value)

        @self.huey.task(version=1, name='tv_error',
                        aliases=('tests.error_v0',),
                        migrations={0: report_v0})
        def on_error_task(info=None, annotated=False):
            return ('error', info, annotated)

        nested_complete = Message(
            'oc-id', 'tests.complete_v0', None, 0, 0, None,
            (77,), {}, None, None, None, None, None, None, None, 0)
        nested_error = Message(
            'oe-id', 'tests.error_v0', None, 0, 0, None,
            ('boom',), {}, None, None, None, None, None, None, None, 0)
        parent_message = Message(
            'p-id', 'tests.parent_v0', None, 2, 0, None,
            (1, 2), {}, nested_complete, nested_error, None, None, None,
            None, None, 0)

        task = self.huey._registry.create_task(parent_message)
        self.assertEqual(task.execute(), 3)
        self.assertEqual(type(task.on_complete).__name__, 'tv_complete')
        self.assertEqual(task.on_complete.kwargs, {'value': 77})
        self.assertEqual(task.on_complete.id, 'oc-id')
        self.assertEqual(task.on_complete.execute(), ('complete', 77))
        self.assertEqual(type(task.on_error).__name__, 'tv_error')
        self.assertEqual(task.on_error.args, ('boom',))
        self.assertEqual(task.on_error.kwargs, {'annotated': True})
        self.assertEqual(task.on_error.execute(),
                         ('error', 'boom', True))

    def test_nested_pipeline_roundtrip_through_serializer(self):
        @self.huey.task(version=1, name='tv_rt_parent',
                        aliases=('tests.rt_parent',),
                        migrations={0: add_v0_to_v1_keep})
        def parent(values):
            return sum(values)

        @self.huey.task(version=1, name='tv_rt_child',
                        aliases=('tests.rt_child',),
                        migrations={0: pipeline_v0})
        def child(value=None):
            return value

        child_message = Message(
            'c-id', 'tests.rt_child', None, 0, 0, None,
            (5,), {}, None, None, None, None, None, None, None, 0)
        parent_message = Message(
            'r-id', 'tests.rt_parent', None, 0, 0, None,
            (1, 4), {}, child_message, None, None, None, None, None, None, 0)
        data = self.huey.serializer.serialize(parent_message)
        task = self.huey.deserialize_task(data)
        self.assertEqual(task.execute(), 5)
        self.assertEqual(task.on_complete.execute(), 5)

    def test_chord_callback_migrated_per_own_version(self):
        @self.huey.task(version=1, name='tv_chord_head',
                        aliases=('tests.head_v0',),
                        migrations={0: add_v0_to_v1_keep})
        def head(values):
            return sum(values)

        @self.huey.task(version=1, name='tv_chord_cb',
                        aliases=('tests.cb_v0',),
                        migrations={0: pipeline_v0})
        def callback(value=None):
            return ('callback', value)

        # A chord member carries a ChordConfig whose callback is still a v0
        # message enqueued under the callback's previous name.
        head_task = head.s([1, 2])
        head_task.chord_config = ChordConfig('cid', 1, 0, callback.s(99))
        message = self.huey._registry.create_message(head_task)
        cid, size, idx, cb_message = message.chord_config
        old_cb = cb_message._replace(name='tests.cb_v0', version=0,
                                     args=(99,), kwargs={})
        message = message._replace(
            chord_config=(cid, size, idx, old_cb))

        task = self.huey._registry.create_task(message)
        self.assertEqual(type(task).__name__, 'tv_chord_head')
        cb = task.chord_config.callback
        self.assertEqual(type(cb).__name__, 'tv_chord_cb')
        self.assertEqual(cb.kwargs, {'value': 99})
        self.assertEqual(cb.id, old_cb.id)
        self.assertEqual(cb.execute(), ('callback', 99))

    def test_nested_failure_identifies_failing_task(self):
        @self.huey.task(name='tv_nf_parent')
        def parent():
            return 'parent'

        @self.huey.task(version=1, name='tv_nf_child',
                        aliases=('tests.nf_child_v0',),
                        migrations={0: boom_v0})
        def child(values):
            return sum(values)

        bad_child = Message(
            'c-id', 'tests.nf_child_v0', None, 0, 0, None,
            (), {}, None, None, None, None, None, None, None, 0)
        parent_message = Message(
            'p-id', 'huey.tests.test_versioning.tv_nf_parent', None, 0, 0,
            None, (), {}, bad_child, None, None, None, None, None, None, 0)
        with self.assertRaises(TaskMigrationError) as cm:
            self.huey._registry.create_task(parent_message)
        self.assertIn('tests.nf_child_v0', str(cm.exception))

    # -- registration discipline --

    def test_only_explicitly_registered_transforms_run(self):
        seen = []

        def explicit(args, kwargs):
            seen.append('explicit')
            return args, kwargs

        @self.huey.task(version=1, name='tv_explicit',
                        migrations={0: explicit})
        def task_a():
            return 'ok'

        record = self.huey._registry._registry[
            'huey.tests.test_versioning.tv_explicit']
        self.assertEqual(set(record.migrations), {0})
        message = Message('id',
                          'huey.tests.test_versioning.tv_explicit', None, 0,
                          0, None, (), {}, None, None, None, None, None,
                          None, None, 0)
        self.huey._registry.create_task(message)
        self.assertEqual(seen, ['explicit'])
