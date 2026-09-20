import datetime

from huey.exceptions import HueyException
from huey.exceptions import TaskMigrationError
from huey.registry import Message
from huey.tests.base import BaseTestCase


def make_message(name, args=(), kwargs=None, version=None, **overrides):
    # Builds a message the way an older Huey would have written it: the
    # "version" field is absent (None) unless explicitly provided.
    fields = dict(
        id='task-id', name=name, eta=None, retries=0, retry_delay=0,
        priority=None, args=args, kwargs=kwargs or {}, on_complete=None,
        on_error=None, expires=None, expires_resolved=None, timeout=None,
        chord_config=None, retry_backoff=None, version=version)
    fields.update(overrides)
    return Message(**fields)


class TestTaskMigration(BaseTestCase):
    def task_name(self, task_wrapper):
        return self.huey._registry.task_to_string(task_wrapper.task_class)

    def trap_migration(self, msg):
        return self.trap_exception(
            lambda: self.huey._registry.create_task(msg), TaskMigrationError)

    def test_unversioned_task_unchanged(self):
        @self.huey.task()
        def task_d(a, b=None):
            return (a, b)

        task = task_d.s(1, b=2)
        message = self.huey._registry.create_message(task)
        self.assertEqual(message.version, 0)

        task2 = self.huey.deserialize_task(self.huey.serialize_task(task))
        self.assertEqual(task2.id, task.id)
        self.assertEqual(task2.args, (1,))
        self.assertEqual(task2.kwargs, {'b': 2})
        self.assertEqual(task2.execute(), (1, 2))

    def test_versioned_serialization_roundtrip(self):
        @self.huey.task(version=2)
        def task_v(a, b=1):
            return a + b

        task = task_v.s(5, b=6)
        message = self.huey._registry.create_message(task)
        self.assertEqual(message.version, 2)

        task2 = self.huey.deserialize_task(self.huey.serialize_task(task))
        self.assertEqual(task2.id, task.id)
        self.assertEqual(task2.args, (5,))
        self.assertEqual(task2.kwargs, {'b': 6})
        self.assertEqual(task2.execute(), 11)

    def test_unversioned_message_treated_as_v0(self):
        @self.huey.task(version=1)
        def task_s(a, b=None):
            return (a, b)

        @self.huey.migration(task_s, 0)
        def migrate_0_1(args, kwargs):
            # v0 took two positional args, v1 made the second a keyword.
            return (args[0],), {'b': args[1]}

        name = self.task_name(task_s)

        # Simulate a message written by an older Huey: no "version" field.
        old = Message('id-9', name, None, 1, 0, None, ('x', 'y'), {},
                      None, None, None, None, None, None, None)
        self.assertTrue(old.version is None)

        data = self.huey.serializer.serialize(old)
        task = self.huey.deserialize_task(data)
        self.assertEqual(task.id, 'id-9')
        self.assertEqual(task.retries, 1)
        self.assertEqual(task.args, ('x',))
        self.assertEqual(task.kwargs, {'b': 'y'})
        self.assertEqual(task.execute(), ('x', 'y'))

    def test_alias_resolves_old_function_name(self):
        @self.huey.task(version=1, aliases=['myapp.tasks.old_name'])
        def new_name(a, b):
            return a + b

        @self.huey.migration(new_name, 0)
        def migrate_0_1(args, kwargs):
            return (args[0], args[1]), {}

        msg = make_message('myapp.tasks.old_name', args=(1, 2))
        task = self.huey._registry.create_task(msg)
        self.assertTrue(isinstance(task, new_name.task_class))
        self.assertEqual(task.execute(), 3)

    def test_alias_conflicts(self):
        @self.huey.task()
        def task_a():
            pass

        name_a = self.task_name(task_a)

        # An alias may not collide with an existing canonical task name.
        def task_b(): pass
        self.assertRaises(ValueError,
                          self.huey.task(aliases=[name_a]), task_b)

        # Two tasks may not register the same alias.
        @self.huey.task(aliases=['old.mod.shared'])
        def task_c():
            pass

        def task_d(): pass
        self.assertRaises(ValueError,
                          self.huey.task(aliases=['old.mod.shared']), task_d)

        # A canonical name may not collide with an existing alias. The
        # canonical name is the module path plus the function (or name=)
        # name, so the alias must use the full path to collide.
        @self.huey.task(aliases=['huey.tests.test_migration.shared'])
        def task_s():
            pass

        def shared(): pass
        self.assertRaises(ValueError, self.huey.task(), shared)

        # An alias may not duplicate the task's own canonical name, nor
        # appear twice on the same task.
        def task_e(): pass
        name_e = 'huey.tests.test_migration.task_e'
        self.assertRaises(ValueError,
                          self.huey.task(aliases=[name_e]), task_e)
        self.assertRaises(ValueError,
                          self.huey.task(aliases=['x.y', 'x.y']), task_e)

        # Re-registering the same canonical name is still an error.
        self.assertRaises(ValueError, self.huey.task(), task_a)

    def test_invalid_task_version(self):
        def task_i(): pass
        self.assertRaises(ValueError, self.huey.task(version=-1), task_i)
        self.assertRaises(ValueError, self.huey.task(version='1'), task_i)
        self.assertRaises(ValueError, self.huey.task(version=True), task_i)

    def test_periodic_task_alias_registered_once(self):
        @self.huey.periodic_task(lambda ts: True, aliases=['old.mod.pt'])
        def task_pt():
            pass

        registry = self.huey._registry
        self.assertEqual(len(registry.periodic_tasks), 1)
        self.assertTrue(
            registry.string_to_task('old.mod.pt') is task_pt.task_class)
        # Resolving through the alias does not schedule the task twice.
        self.assertEqual(len(registry.periodic_tasks), 1)

    def test_two_consecutive_upgrades(self):
        @self.huey.task(version=2)
        def task_2(a, b, c):
            return (a, b, c)

        @self.huey.migration(task_2, 0)
        def migrate_0_1(args, kwargs):
            # v0: single positional arg. v1: added "b" as a keyword.
            return args, dict(kwargs, b=args[0] * 10)

        @self.huey.migration(task_2, 1)
        def migrate_1_2(args, kwargs):
            # v2: "b" moved to positional args, added "c".
            b = kwargs.pop('b')
            return (args[0], b, b * 10), kwargs

        name = self.task_name(task_2)

        # A v0 message passes through both migrations, in order.
        task = self.huey._registry.create_task(make_message(name, args=(2,)))
        self.assertEqual(task.args, (2, 20, 200))
        self.assertEqual(task.kwargs, {})
        self.assertEqual(task.execute(), (2, 20, 200))

        # A v1 message only runs the second migration.
        msg_v1 = make_message(name, args=(3,), kwargs={'b': 30}, version=1)
        task_v1 = self.huey._registry.create_task(msg_v1)
        self.assertEqual(task_v1.args, (3, 30, 300))

    def test_migration_preserves_metadata(self):
        eta = datetime.datetime(2026, 1, 1, 12)
        expires_resolved = datetime.datetime(2026, 1, 2, 12)

        @self.huey.task(version=1)
        def task_p(a, b):
            return a + b

        @self.huey.migration(task_p, 0)
        def migrate_0_1(args, kwargs):
            return args + (10,), {}

        name = self.task_name(task_p)
        msg = make_message(
            name, args=(1,), id='abc', eta=eta, retries=3, retry_delay=7,
            retry_backoff=2, priority=5, expires=60,
            expires_resolved=expires_resolved, timeout=30)
        task = self.huey._registry.create_task(msg)

        self.assertEqual(task.args, (1, 10))
        self.assertEqual(task.id, 'abc')
        self.assertEqual(task.eta, eta)
        self.assertEqual(task.retries, 3)
        self.assertEqual(task.retry_delay, 7)
        self.assertEqual(task.retry_backoff, 2)
        self.assertEqual(task.priority, 5)
        self.assertEqual(task.expires, 60)
        self.assertEqual(task.expires_resolved, expires_resolved)
        self.assertEqual(task.timeout, 30)

    def test_unknown_future_version(self):
        @self.huey.task(version=1)
        def task_f(a):
            pass

        name = self.task_name(task_f)
        msg = make_message(name, args=(1,), version=5)
        exc = self.trap_migration(msg)
        self.assertEqual(exc.task_name, name)
        self.assertEqual(exc.msg_version, 5)
        self.assertEqual(exc.task_version, 1)

    def test_broken_migration_chain(self):
        @self.huey.task(version=2)
        def task_b(a):
            pass

        @self.huey.migration(task_b, 0)
        def migrate_0_1(args, kwargs):
            return args, kwargs

        # The migration from version 1 to 2 is missing.
        name = self.task_name(task_b)
        msg = make_message(name, args=(1,))
        exc = self.trap_migration(msg)
        self.assertEqual(exc.task_name, name)
        self.assertEqual(exc.msg_version, 1)
        self.assertEqual(exc.task_version, 2)
        self.assertIn('no migration registered', str(exc))

    def test_migration_exception(self):
        @self.huey.task(version=1)
        def task_x(a):
            pass

        @self.huey.migration(task_x, 0)
        def migrate_0_1(args, kwargs):
            raise KeyError('missing')

        name = self.task_name(task_x)
        msg = make_message(name, args=(1,))
        exc = self.trap_migration(msg)
        self.assertEqual(exc.task_name, name)
        self.assertEqual(exc.msg_version, 0)
        self.assertEqual(exc.task_version, 1)
        self.assertIn('KeyError', str(exc))

    def test_migration_invalid_return(self):
        @self.huey.task(version=1)
        def task_r(a):
            pass

        name = self.task_name(task_r)

        @self.huey.migration(task_r, 0)
        def bad_shape(args, kwargs):
            return args  # Not an (args, kwargs) 2-tuple.

        msg = make_message(name, args=(1,))
        self.assertRaises(TaskMigrationError,
                          self.huey._registry.create_task, msg)

        self.huey._registry._migrations.clear()

        @self.huey.migration(task_r, 0)
        def bad_kwargs(args, kwargs):
            return args, None

        self.assertRaises(TaskMigrationError,
                          self.huey._registry.create_task, msg)

    def test_failed_migration_not_executed(self):
        calls = []

        @self.huey.task(version=1)
        def task_fail(a, b):
            calls.append((a, b))

        @self.huey.migration(task_fail, 0)
        def migrate_0_1(args, kwargs):
            raise RuntimeError('boom')

        name = self.task_name(task_fail)
        old = Message('f1', name, None, 0, 0, None, (1, 2), {},
                      None, None, None, None, None, None, None)
        self.huey.storage.enqueue(self.huey.serializer.serialize(old), None)

        # The message cannot be migrated, so the error surfaces on dequeue
        # and the task is never executed with stale arguments.
        self.assertRaises(TaskMigrationError, self.huey.dequeue)
        self.assertEqual(calls, [])
        self.assertEqual(self.huey.pending_count(), 0)

    def test_dequeue_migrates_old_message(self):
        @self.huey.task(version=1)
        def task_q(a, b):
            return a + b

        @self.huey.migration(task_q, 0)
        def migrate_0_1(args, kwargs):
            return (args[0], args[0] * 2), {}

        name = self.task_name(task_q)
        old = Message('q1', name, None, 0, 0, None, (4,), {}, None, None,
                      None, None, None, None, None)
        self.huey.storage.enqueue(self.huey.serializer.serialize(old), None)

        task = self.huey.dequeue()
        self.assertEqual(task.id, 'q1')
        self.assertEqual(task.args, (4, 8))
        self.assertEqual(self.huey.execute(task), 12)

    def test_nested_pipeline_migration(self):
        @self.huey.task(version=1)
        def child(a, b):
            return a + b

        @self.huey.migration(child, 0)
        def migrate_child(args, kwargs):
            # v0 took a single argument, v1 takes two.
            return (args[0], args[0] * 2), {}

        @self.huey.task()
        def parent():
            pass

        child_name = self.task_name(child)
        parent_name = self.task_name(parent)
        msg = make_message(
            parent_name,
            on_complete=make_message(child_name, args=(3,)),
            on_error=make_message(child_name, args=(5,)),
            chord_config=('cid', 2, 0, make_message(child_name, args=(4,))))

        task = self.huey._registry.create_task(msg)
        self.assertEqual(task.on_complete.args, (3, 6))
        self.assertEqual(task.on_error.args, (5, 10))
        self.assertEqual(task.chord_config.cid, 'cid')
        self.assertEqual(task.chord_config.callback.args, (4, 8))

    def test_nested_migration_failure_identifies_task(self):
        @self.huey.task(version=1)
        def child_n(a, b):
            pass

        # No migration registered for the child task.
        @self.huey.task()
        def parent_n():
            pass

        child_name = self.task_name(child_n)
        parent_name = self.task_name(parent_n)
        msg = make_message(
            parent_name,
            on_complete=make_message(child_name, args=(1,)))
        exc = self.trap_migration(msg)
        self.assertEqual(exc.task_name, child_name)
        self.assertEqual(exc.msg_version, 0)
        self.assertEqual(exc.task_version, 1)

    def test_migration_registration_errors(self):
        @self.huey.task(version=2)
        def task_m(a):
            pass

        # Cannot register a migration for an unknown task.
        self.assertRaises(HueyException,
                          self.huey.migration('no.such.task', 0),
                          lambda a, k: (a, k))

        # from_version must be a non-negative integer.
        self.assertRaises(ValueError,
                          self.huey.migration(task_m, -1),
                          lambda a, k: (a, k))
        self.assertRaises(ValueError,
                          self.huey.migration(task_m, '0'),
                          lambda a, k: (a, k))

        @self.huey.migration(task_m, 0)
        def migrate_0_1(args, kwargs):
            return args, kwargs

        # Duplicate migration for the same task and version.
        self.assertRaises(ValueError,
                          self.huey.migration(task_m, 0),
                          lambda a, k: (a, k))

    def test_migration_registered_by_name_or_alias(self):
        @self.huey.task(version=1, aliases=['old.mod.renamed'])
        def task_n(a, b):
            return a + b

        # Migrations may be registered using the canonical name or an alias.
        @self.huey.migration('old.mod.renamed', 0)
        def migrate_0_1(args, kwargs):
            return (args[0], args[0] * 3), {}

        name = self.task_name(task_n)
        msg = make_message(name, args=(3,))
        task = self.huey._registry.create_task(msg)
        self.assertEqual(task.args, (3, 9))

    def test_unregister_removes_aliases_and_migrations(self):
        @self.huey.task(version=1, aliases=['old.mod.u'])
        def task_u(a):
            pass

        @self.huey.migration(task_u, 0)
        def migrate_0_1(args, kwargs):
            return args, kwargs

        registry = self.huey._registry
        self.assertTrue(task_u.unregister())
        self.assertRaises(HueyException, registry.string_to_task,
                          'old.mod.u')
        self.assertEqual(registry._aliases, {})
        self.assertEqual(registry._migrations, {})
