import datetime
import importlib.util
import io
import os
import shutil
import tempfile
import threading
import unittest
from unittest import mock


django_available = importlib.util.find_spec('django') is not None

if django_available:
    import django
    from django.conf import settings

    from huey import MemoryHuey

    if not settings.configured:
        _tmpdir = tempfile.mkdtemp(prefix='huey-outbox-tests-')
        settings.configure(
            USE_TZ=True,
            DATABASES={
                'default': {
                    'ENGINE': 'django.db.backends.sqlite3',
                    'NAME': os.path.join(_tmpdir, 'default.sqlite3')},
                'other': {
                    'ENGINE': 'django.db.backends.sqlite3',
                    'NAME': os.path.join(_tmpdir, 'other.sqlite3')},
            },
            INSTALLED_APPS=[
                'django.contrib.contenttypes',
                'django.contrib.auth',
                'huey.contrib.djhuey.outbox',
            ],
            TASKS={'default': {
                'BACKEND': 'huey.contrib.djhuey.tasks_backend.HueyBackend'}},
            HUEY=MemoryHuey('outbox-test', immediate=False, results=False))
    django.setup()

    from django.apps import apps
    from django.core.management import call_command
    from django.db import connections
    from django.db import transaction
    from django.utils import timezone

    from huey.contrib.djhuey import HUEY
    from huey.contrib.djhuey import on_commit_task
    from huey.contrib.djhuey.outbox import dispatch_outbox
    from huey.contrib.djhuey.outbox import outbox_task
    from huey.contrib.djhuey.outbox import dispatcher as outbox_dispatcher
    from huey.contrib.djhuey.outbox.dispatcher import OutboxDispatcher
    from huey.contrib.djhuey.outbox.models import OutboxTask

    outbox_installed = apps.is_installed('huey.contrib.djhuey.outbox')

    executed = []

    @outbox_task()
    def record(value):
        executed.append(value)
        return value

    @outbox_task(using='other')
    def record_other(value):
        executed.append(('other', value))
        return value

    @on_commit_task()
    def plain_on_commit(value):
        executed.append(('plain', value))
        return value


@unittest.skipIf(not django_available or not outbox_installed,
                 'requires django')
class TestOutbox(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestOutbox, cls).setUpClass()
        for alias in ('default', 'other'):
            call_command('migrate', database=alias, verbosity=0,
                         run_syncdb=True)

    def setUp(self):
        super(TestOutbox, self).setUp()
        HUEY.immediate = False
        HUEY.storage.flush_all()
        del executed[:]
        for alias in ('default', 'other'):
            OutboxTask.objects.using(alias).all().delete()

    def make_row(self, alias='default', **kwargs):
        huey_task = record.task_wrapper.s('manual')
        return OutboxTask.objects.using(alias).create(
            task_id=huey_task.id,
            task_name=huey_task.name,
            payload=HUEY.serialize_task(huey_task),
            **kwargs)

    def dequeue_ids(self):
        ids = []
        while True:
            task = HUEY.dequeue()
            if task is None:
                break
            ids.append(task.id)
        return ids

    def test_enqueue_after_commit(self):
        with transaction.atomic():
            result = record('a')
            self.assertEqual(OutboxTask.objects.count(), 1)
            self.assertEqual(HUEY.pending_count(), 0)

        # The post-commit nudge dispatched the row immediately.
        self.assertEqual(HUEY.pending_count(), 1)
        row = OutboxTask.objects.get()
        self.assertEqual(row.status, OutboxTask.SENT)
        self.assertTrue(row.sent_at is not None)
        self.assertEqual(row.task_id, result.id)

        task = HUEY.dequeue()
        self.assertEqual(task.id, row.task_id)
        HUEY.execute(task)
        self.assertEqual(executed, ['a'])

    def test_enqueue_without_transaction(self):
        record('auto')
        self.assertEqual(HUEY.pending_count(), 1)
        row = OutboxTask.objects.get()
        self.assertEqual(row.status, OutboxTask.SENT)

    def test_rollback_discards_task(self):
        def boom():
            with transaction.atomic():
                record('x')
                raise ValueError('rollback')
        self.assertRaises(ValueError, boom)

        self.assertEqual(OutboxTask.objects.count(), 0)
        stats = dispatch_outbox()
        self.assertEqual(stats['claimed'], 0)
        self.assertEqual(HUEY.pending_count(), 0)

    def test_nested_atomic_savepoint(self):
        with transaction.atomic():
            record('outer')
            try:
                with transaction.atomic():
                    record('inner')
                    raise ValueError('savepoint rollback')
            except ValueError:
                pass

        # Only the outer task survived the savepoint rollback.
        self.assertEqual(OutboxTask.objects.count(), 1)
        row = OutboxTask.objects.get()
        self.assertEqual(HUEY.pending_count(), 1)
        task = HUEY.dequeue()
        self.assertEqual(task.id, row.task_id)
        HUEY.execute(task)
        self.assertEqual(executed, ['outer'])

    def test_nested_atomic_outer_rollback(self):
        def boom():
            with transaction.atomic():
                record('outer')
                try:
                    with transaction.atomic():
                        record('inner')
                        raise ValueError('savepoint rollback')
                except ValueError:
                    pass
                raise ValueError('outer rollback')
        self.assertRaises(ValueError, boom)
        self.assertEqual(OutboxTask.objects.count(), 0)
        self.assertEqual(HUEY.pending_count(), 0)

    def test_non_default_alias(self):
        with transaction.atomic(using='other'):
            record_other('z')
            self.assertEqual(OutboxTask.objects.using('other').count(), 1)
            self.assertEqual(OutboxTask.objects.using('default').count(), 0)

        # The nudge dispatched against the correct alias.
        self.assertEqual(HUEY.pending_count(), 1)
        row = OutboxTask.objects.using('other').get()
        self.assertEqual(row.status, OutboxTask.SENT)

        # A dispatcher for the default alias sees nothing.
        stats = dispatch_outbox(using='default')
        self.assertEqual(stats['claimed'], 0)

    def test_alias_rollback_only_affects_its_own_database(self):
        with transaction.atomic():
            record('kept')
        self.assertEqual(HUEY.pending_count(), 1)

        def boom():
            with transaction.atomic(using='other'):
                record_other('discarded')
                raise ValueError('rollback other')
        self.assertRaises(ValueError, boom)

        self.assertEqual(OutboxTask.objects.using('other').count(), 0)
        self.assertEqual(OutboxTask.objects.using('default').count(), 1)
        self.assertEqual(HUEY.pending_count(), 1)

    def test_commit_then_crash_before_dispatch_is_recovered(self):
        # Simulate the process exiting after the commit but before the
        # post-commit nudge could dispatch the row.
        with mock.patch.object(outbox_dispatcher, 'dispatch_outbox') as nudge:
            with transaction.atomic():
                record('d')
            nudge.assert_called_once_with(using='default')

        self.assertEqual(HUEY.pending_count(), 0)
        row = OutboxTask.objects.get()
        self.assertEqual(row.status, OutboxTask.PENDING)

        # A later dispatcher run (e.g. manage.py dispatch_outbox) recovers
        # the row and enqueues the same task id.
        stats = dispatch_outbox()
        self.assertEqual(stats['sent'], 1)
        row.refresh_from_db()
        self.assertEqual(row.status, OutboxTask.SENT)
        self.assertEqual(self.dequeue_ids(), [row.task_id])

    def test_competing_dispatchers_send_exactly_once(self):
        row = self.make_row()
        barrier = threading.Barrier(2)
        results = []

        def worker():
            dispatcher = OutboxDispatcher(huey=HUEY, using='default')
            barrier.wait(timeout=5)
            results.append(dispatcher.dispatch_batch())
            connections.close_all()

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly one dispatcher won the claim and sent the task.
        self.assertEqual(sum(r['claimed'] for r in results), 1)
        self.assertEqual(sum(r['sent'] for r in results), 1)
        self.assertEqual(HUEY.pending_count(), 1)
        self.assertEqual(self.dequeue_ids(), [row.task_id])

    def test_claim_timeout_recovery(self):
        row = self.make_row()
        first = OutboxDispatcher(huey=HUEY, claim_timeout=60)
        claimed = first.claim_batch()
        self.assertEqual(len(claimed), 1)
        # "first" crashes here without ever dispatching.

        # A live claim is respected by other dispatchers.
        second = OutboxDispatcher(huey=HUEY, claim_timeout=60)
        self.assertEqual(second.dispatch_batch()['claimed'], 0)

        # Once the claim times out, the row is recovered and sent with the
        # same task id.
        OutboxTask.objects.filter(pk=row.pk).update(
            claimed_at=timezone.now() - datetime.timedelta(seconds=120))
        stats = second.dispatch_batch()
        self.assertEqual(stats['sent'], 1)
        self.assertEqual(self.dequeue_ids(), [row.task_id])

    def test_send_failure_records_diagnostics_and_retries(self):
        row = self.make_row()
        dispatcher = OutboxDispatcher(huey=HUEY, retry_delay=30,
                                      max_attempts=3)
        with mock.patch.object(HUEY, 'enqueue',
                               side_effect=RuntimeError('boom')):
            stats = dispatcher.dispatch_batch()

        self.assertEqual(stats['retry'], 1)
        row.refresh_from_db()
        self.assertEqual(row.status, OutboxTask.PENDING)
        self.assertEqual(row.attempts, 1)
        self.assertIn('RuntimeError', row.last_error)
        self.assertIn('boom', row.last_error)
        self.assertTrue(row.available_at > timezone.now())
        self.assertEqual(HUEY.pending_count(), 0)

        # The row is not retried before its backoff window elapses.
        self.assertEqual(dispatch_outbox()['claimed'], 0)

        # After the backoff window, the retry succeeds.
        OutboxTask.objects.filter(pk=row.pk).update(
            available_at=timezone.now())
        stats = dispatch_outbox()
        self.assertEqual(stats['sent'], 1)
        self.assertEqual(self.dequeue_ids(), [row.task_id])

    def test_send_failure_gives_up_after_max_attempts(self):
        row = self.make_row()
        dispatcher = OutboxDispatcher(huey=HUEY, retry_delay=0,
                                      max_attempts=2)
        with mock.patch.object(HUEY, 'enqueue',
                               side_effect=RuntimeError('boom')):
            self.assertEqual(dispatcher.dispatch_batch()['retry'], 1)
            self.assertEqual(dispatcher.dispatch_batch()['failed'], 1)

        row.refresh_from_db()
        self.assertEqual(row.status, OutboxTask.FAILED)
        self.assertEqual(row.attempts, 2)
        self.assertIn('RuntimeError', row.last_error)

        # A failed row is terminal: it is never claimed again.
        self.assertEqual(dispatch_outbox()['claimed'], 0)
        self.assertEqual(HUEY.pending_count(), 0)

    def test_ack_failure_redelivers_with_same_task_id(self):
        row = self.make_row()
        first = OutboxDispatcher(huey=HUEY, claim_timeout=60)

        # The send succeeds but the acknowledgement is lost (process killed
        # or database error before the row is marked sent).
        with mock.patch.object(OutboxDispatcher, '_ack_sent',
                               side_effect=Exception('ack lost')):
            self.assertRaises(Exception, first.dispatch_batch)

        row.refresh_from_db()
        self.assertEqual(row.status, OutboxTask.CLAIMED)
        self.assertEqual(HUEY.pending_count(), 1)

        # After the claim expires, a new dispatcher recovers the row and
        # re-enqueues it. The duplicate carries the same stable task id:
        # delivery is at-least-once.
        OutboxTask.objects.filter(pk=row.pk).update(
            claimed_at=timezone.now() - datetime.timedelta(seconds=120))
        stats = OutboxDispatcher(huey=HUEY).dispatch_batch()
        self.assertEqual(stats['sent'], 1)

        ids = self.dequeue_ids()
        self.assertEqual(len(ids), 2)
        self.assertEqual(ids, [row.task_id, row.task_id])

    def test_batch_size_bounds_work(self):
        for _ in range(5):
            self.make_row()
        dispatcher = OutboxDispatcher(huey=HUEY, batch_size=2)
        stats = dispatcher.dispatch_batch()
        self.assertEqual(stats['claimed'], 2)
        self.assertEqual(stats['sent'], 2)
        self.assertEqual(
            OutboxTask.objects.filter(status=OutboxTask.SENT).count(), 2)
        self.assertEqual(
            OutboxTask.objects.filter(status=OutboxTask.PENDING).count(), 3)
        self.assertEqual(HUEY.pending_count(), 2)

    def test_management_command(self):
        with mock.patch.object(outbox_dispatcher, 'dispatch_outbox'):
            record('cmd')
        self.assertEqual(
            OutboxTask.objects.filter(status=OutboxTask.PENDING).count(), 1)

        out = io.StringIO()
        call_command('dispatch_outbox', using='default', stdout=out)
        row = OutboxTask.objects.get()
        self.assertEqual(row.status, OutboxTask.SENT)
        self.assertEqual(self.dequeue_ids(), [row.task_id])

    def test_on_commit_task_behavior_unchanged(self):
        with transaction.atomic():
            plain_on_commit('oc')
            self.assertEqual(HUEY.pending_count(), 0)
        self.assertEqual(HUEY.pending_count(), 1)
        # on_commit_task does not involve the outbox at all.
        self.assertEqual(OutboxTask.objects.count(), 0)
        task = HUEY.dequeue()
        HUEY.execute(task)
        self.assertEqual(executed, [('plain', 'oc')])
