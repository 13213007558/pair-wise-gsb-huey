"""
Fault-injection tests for the Huey transactional outbox.

These tests use only the local SQLite backend (file-backed, so a "new
process" can open the same committed database) and MemoryHuey, so no external
queue is required.
"""
import importlib.util
import os
import tempfile
import threading
import time
import unittest


django_available = importlib.util.find_spec('django') is not None

if django_available:
    import django
    from django.conf import settings
    from huey import MemoryHuey

    _db_dir = tempfile.mkdtemp(prefix='huey-outbox-test-')
    _default_db = os.path.join(_db_dir, 'default.sqlite3')
    _other_db = os.path.join(_db_dir, 'other.sqlite3')

    settings.configure(
        USE_TZ=True,
        DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': _default_db,
                'OPTIONS': {'timeout': 20},
            },
            'other': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': _other_db,
                'OPTIONS': {'timeout': 20},
            },
        },
        INSTALLED_APPS=[
            'huey.contrib.djhuey_outbox',
        ],
        HUEY=MemoryHuey('outbox-test'))
    django.setup()

    from django.core.management import call_command
    from django.db import transaction
    from django.utils import timezone

    from huey.contrib.djhuey_outbox.dispatcher import OutboxDispatcher
    from huey.contrib.djhuey_outbox.models import OutboxTask
    from huey.contrib.djhuey_outbox.tasks import outbox_task
    from huey.contrib.djhuey import HUEY

    HUEY.immediate = False

    call_command('migrate', run_syncdb=False, verbosity=0,
                 database='default')
    call_command('migrate', run_syncdb=False, verbosity=0,
                 database='other')

    def _make_order_task():
        def process_order(order_id):
            return order_id
        return process_order

    def _register_outbox_task(huey_inst, fn_name, task_name):
        fn = _make_order_task()
        fn.__name__ = fn_name
        return outbox_task(huey=huey_inst, name=fn_name)(fn)


@unittest.skipIf(not django_available, 'requires Django')
class OutboxTestCase(unittest.TestCase):
    def setUp(self):
        OutboxTask.objects.all().delete()
        OutboxTask.objects.using('other').all().delete()
        HUEY.storage.flush_all()
        HUEY.immediate = False

    def make_decorated(self, task_name):
        return _register_outbox_task(
            HUEY, 'fn_' + task_name, task_name)

    def test_rollback_discards_task(self):
        process_order = self.make_decorated('rolled_back')
        with self.assertRaises(ValueError):
            with transaction.atomic():
                process_order(1)
                self.assertEqual(OutboxTask.objects.count(), 1)
                raise ValueError('business txn rolled back')
        self.assertEqual(OutboxTask.objects.count(), 0)
        self.assertEqual(HUEY.pending_count(), 0)

    def test_nested_atomic_savepoint(self):
        process_order = self.make_decorated('nested')
        with transaction.atomic():
            with self.assertRaises(RuntimeError):
                with transaction.atomic():
                    process_order(1)
                    raise RuntimeError('savepoint rollback')
            process_order(2)
        self.assertEqual(OutboxTask.objects.count(), 1)

        dispatcher = OutboxDispatcher(huey=HUEY, batch_size=10)
        self.assertEqual(dispatcher.dispatch_once(), 1)
        self.assertEqual(HUEY.pending_count(), 1)

    def test_alternate_database_alias(self):
        process_order = self.make_decorated('on_other')
        with transaction.atomic(using='other'):
            process_order(7, using='other')
        self.assertEqual(OutboxTask.objects.count(), 0)
        self.assertEqual(OutboxTask.objects.using('other').count(), 1)

        dispatcher = OutboxDispatcher(huey=HUEY, using='other',
                                      batch_size=10)
        self.assertEqual(dispatcher.dispatch_once(), 1)
        row = OutboxTask.objects.using('other').get()
        self.assertEqual(row.status, OutboxTask.SENT)
        self.assertEqual(HUEY.pending_count(), 1)

    def test_commit_then_dispatch_uses_stable_id(self):
        process_order = self.make_decorated('happy')
        with transaction.atomic():
            result = process_order(42)
            task_id = result.id
            self.assertEqual(HUEY.pending_count(), 0)

        row = OutboxTask.objects.get(task_id=task_id)
        self.assertEqual(row.status, OutboxTask.PENDING)

        dispatcher = OutboxDispatcher(huey=HUEY, batch_size=10,
                                      owner='disp-a')
        self.assertEqual(dispatcher.dispatch_once(), 1)

        row.refresh_from_db()
        self.assertEqual(row.status, OutboxTask.SENT)
        self.assertEqual(row.attempts, 1)
        self.assertTrue(row.sent_at is not None)
        self.assertEqual(HUEY.pending_count(), 1)
        enqueued = HUEY.dequeue()
        self.assertEqual(enqueued.id, task_id)
        self.assertEqual(enqueued.args, (42,))

    def test_recovery_after_process_dies_before_enqueue(self):
        # "Process 1" owns a private MemoryHuey and commits the outbox row.
        huey1 = MemoryHuey('process-1')
        huey1.immediate = False
        process_order_1 = _register_outbox_task(
            huey1, 'recoverable_order', 'recoverable_order')
        with transaction.atomic():
            process_order_1(99)

        # Process 1 crashes BEFORE enqueuing: its in-memory queue vanishes,
        # while SQLite retains the committed row.
        del huey1
        self.assertEqual(HUEY.pending_count(), 0)
        self.assertEqual(OutboxTask.objects.count(), 1)

        # A fresh process with a fresh MemoryHuey runs the dispatcher.
        huey2 = MemoryHuey('process-2')
        huey2.immediate = False
        _register_outbox_task(huey2, 'recoverable_order', 'recoverable_order')

        dispatcher = OutboxDispatcher(huey=huey2, batch_size=10)
        self.assertEqual(dispatcher.dispatch_once(), 1)

        self.assertEqual(huey2.pending_count(), 1)
        recovered = huey2.dequeue()
        self.assertEqual(recovered.args, (99,))
        self.assertEqual(OutboxTask.objects.get().status, OutboxTask.SENT)

    def test_two_dispatchers_cannot_claim_same_row(self):
        process_order = self.make_decorated('race')
        with transaction.atomic():
            for i in range(20):
                process_order(i)

        disp_a = OutboxDispatcher(huey=HUEY, batch_size=50,
                                  owner='disp-a')
        disp_b = OutboxDispatcher(huey=HUEY, batch_size=50,
                                  owner='disp-b')
        errors = []

        def run(dispatcher):
            try:
                for _ in range(10):
                    if dispatcher.dispatch_once() == 0:
                        break
            except Exception as exc:  # pragma: no cover - diagnostic only
                errors.append(exc)

        thread_a = threading.Thread(target=run, args=(disp_a,))
        thread_b = threading.Thread(target=run, args=(disp_b,))
        thread_a.start()
        thread_b.start()
        thread_a.join()
        thread_b.join()

        self.assertEqual(errors, [])
        self.assertEqual(
            OutboxTask.objects.filter(status=OutboxTask.SENT).count(), 20)
        self.assertEqual(HUEY.pending_count(), 20)
        ids = [HUEY.dequeue().id for _ in range(20)]
        self.assertEqual(len(set(ids)), 20)

    def test_stale_claim_is_recovered(self):
        process_order = self.make_decorated('stale')
        with transaction.atomic():
            process_order(1)

        doomed = OutboxDispatcher(huey=HUEY, batch_size=10, claim_timeout=0.1,
                                  owner='crashed-disp')
        claimed = doomed.claim_batch()
        self.assertEqual(len(claimed), 1)
        self.assertEqual(HUEY.pending_count(), 0)

        # A fresh claim must not be stolen before the timeout.
        other = OutboxDispatcher(huey=HUEY, batch_size=10,
                                 claim_timeout=0.1, owner='disp-b')
        self.assertEqual(other.claim_batch(), [])

        # After the timeout the stale claim is reclaimed and delivered.
        time.sleep(0.15)
        self.assertEqual(other.dispatch_once(), 1)
        self.assertEqual(HUEY.pending_count(), 1)
        self.assertEqual(OutboxTask.objects.get().status, OutboxTask.SENT)

    def test_send_success_then_confirm_failure_duplicates(self):
        process_order = self.make_decorated('confirm_crash')
        with transaction.atomic():
            process_order(1)

        dispatcher = OutboxDispatcher(huey=HUEY, batch_size=10,
                                      claim_timeout=0.05,
                                      owner='disp-1')
        original_mark_sent = dispatcher.mark_sent

        def crash_after_send(outbox_task):
            # The message is already safely in the queue...
            self.assertEqual(HUEY.pending_count(), 1)
            raise SystemExit('process killed before confirm')

        dispatcher.mark_sent = crash_after_send
        claimed = dispatcher.claim_batch()
        # SystemExit is intentionally not swallowed: it models process death.
        with self.assertRaises(SystemExit):
            dispatcher._deliver(claimed[0])

        row = OutboxTask.objects.get()
        self.assertEqual(row.status, OutboxTask.IN_PROGRESS)
        self.assertEqual(HUEY.pending_count(), 1)

        # After the claim timeout the SAME stable id is delivered again:
        # this is the at-least-once duplicate window.
        time.sleep(0.1)
        recovery = OutboxDispatcher(huey=HUEY, batch_size=10,
                                    claim_timeout=0.05, owner='disp-2')
        recovery.mark_sent = original_mark_sent
        self.assertEqual(recovery.dispatch_once(), 1)

        self.assertEqual(HUEY.pending_count(), 2)
        first = HUEY.dequeue()
        second = HUEY.dequeue()
        self.assertEqual(first.id, second.id)
        row.refresh_from_db()
        self.assertEqual(row.status, OutboxTask.SENT)

    def test_send_failures_record_diagnostic_and_retry(self):
        process_order = self.make_decorated('flaky_queue')
        with transaction.atomic():
            process_order(1)

        dispatcher = OutboxDispatcher(
            huey=HUEY, batch_size=10, max_attempts=3,
            base_delay=0.0, max_delay=0.0, owner='disp')
        attempts = {'n': 0}

        def flaky_send(outbox_task, task):
            attempts['n'] += 1
            if attempts['n'] < 3:
                raise ConnectionError('queue is down')
            dispatcher.huey.storage.enqueue(bytes(outbox_task.message),
                                            task.priority)

        dispatcher.send = flaky_send

        self.assertEqual(dispatcher.dispatch_once(), 0)
        row = OutboxTask.objects.get()
        self.assertEqual(row.status, OutboxTask.PENDING)
        self.assertEqual(row.attempts, 1)
        self.assertTrue('ConnectionError: queue is down' in row.last_error)
        self.assertEqual(HUEY.pending_count(), 0)

        self.assertEqual(dispatcher.dispatch_once(), 0)
        self.assertEqual(OutboxTask.objects.get().attempts, 2)

        self.assertEqual(dispatcher.dispatch_once(), 1)
        row.refresh_from_db()
        self.assertEqual(row.status, OutboxTask.SENT)
        self.assertEqual(HUEY.pending_count(), 1)

    def test_future_next_attempt_is_not_due(self):
        process_order = self.make_decorated('waiting')
        with transaction.atomic():
            process_order(1)
        future = timezone.now() + timezone.timedelta(hours=1)
        OutboxTask.objects.update(next_attempt_at=future)

        dispatcher = OutboxDispatcher(huey=HUEY, batch_size=10)
        self.assertEqual(dispatcher.dispatch_once(), 0)
        self.assertEqual(HUEY.pending_count(), 0)

    def test_exhausted_attempts_dead_letter(self):
        process_order = self.make_decorated('always_failing')
        with transaction.atomic():
            process_order(1)

        dispatcher = OutboxDispatcher(
            huey=HUEY, batch_size=10, max_attempts=3,
            base_delay=0.0, max_delay=0.0, owner='disp')

        def always_failing_send(outbox_task, task):
            raise ConnectionError('queue is down')

        dispatcher.send = always_failing_send
        for _ in range(3):
            self.assertEqual(dispatcher.dispatch_once(), 0)

        row = OutboxTask.objects.get()
        self.assertEqual(row.status, OutboxTask.FAILED)
        self.assertEqual(row.attempts, 3)
        self.assertTrue('ConnectionError' in row.last_error)
        self.assertEqual(HUEY.pending_count(), 0)

        # Failed rows are never claimed again.
        self.assertEqual(dispatcher.claim_batch(), [])

    def test_batch_is_bounded(self):
        process_order = self.make_decorated('bulk')
        with transaction.atomic():
            for i in range(5):
                process_order(i)

        dispatcher = OutboxDispatcher(huey=HUEY, batch_size=2)
        self.assertEqual(dispatcher.dispatch_once(), 2)
        self.assertEqual(
            OutboxTask.objects.filter(status=OutboxTask.SENT).count(), 2)
        self.assertEqual(
            OutboxTask.objects.filter(status=OutboxTask.PENDING).count(), 3)
        self.assertEqual(HUEY.pending_count(), 2)
