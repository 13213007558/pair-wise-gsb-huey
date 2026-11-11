import importlib.util
import os
import shutil
import tempfile
import unittest


django_available = importlib.util.find_spec('django') is not None

if django_available:
    import django
    from django.conf import settings

    from huey import MemoryHuey

    DB_DIR = tempfile.mkdtemp(prefix='huey-djhuey-on-commit-')

    def sqlite_db(name):
        # A complete connection settings dict, so the entry is valid even
        # when added to an already-configured settings.DATABASES.
        return {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': os.path.join(DB_DIR, '%s.sqlite3' % name),
            'ATOMIC_REQUESTS': False,
            'AUTOCOMMIT': True,
            'CONN_MAX_AGE': 0,
            'CONN_HEALTH_CHECKS': False,
            'OPTIONS': {},
            'TIME_ZONE': None,
            'USER': '',
            'PASSWORD': '',
            'HOST': '',
            'PORT': '',
            'TEST': {
                'CHARSET': None,
                'COLLATION': None,
                'NAME': None,
                'MIRROR': None,
            },
        }

    if not settings.configured:
        settings.configure(
            USE_TZ=True,
            DATABASES={
                'default': sqlite_db('default'),
                'audit': sqlite_db('audit'),
            },
            HUEY=MemoryHuey('djhuey-on-commit-test'))
        django.setup()
    elif 'audit' not in settings.DATABASES:
        # Another test module configured settings first (e.g.
        # test_django_tasks); add the second connection in-place.
        settings.DATABASES['audit'] = sqlite_db('audit')

    from django.db import transaction

    from huey.contrib import djhuey
    from huey.contrib.djhuey import on_commit_task

    @on_commit_task(using_db='audit')
    def scan_accounts(accounts, using=None):
        # "using" is a legitimate task parameter and must not be consumed
        # by the decorator's own database selection.
        return sorted(accounts), using

    @on_commit_task()
    def record_event(name):
        return name


@unittest.skipUnless(django_available, 'django is not installed')
class TestOnCommitTask(unittest.TestCase):
    def setUp(self):
        super(TestOnCommitTask, self).setUp()
        self.huey = djhuey.HUEY
        self.huey.storage.flush_queue()
        self.huey.storage.flush_results()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(DB_DIR, ignore_errors=True)
        super(TestOnCommitTask, cls).tearDownClass()

    def pending_names(self):
        return [task.name for task in self.huey.pending()]

    def test_enqueues_on_commit_of_selected_database(self):
        with transaction.atomic(using='audit'):
            result = scan_accounts(['acct-1'], using='replica')
            self.assertEqual(self.huey.pending_count(), 0)
            self.assertFalse(result.is_ready())
            self.assertIsNone(result.get())

        self.assertEqual(self.huey.pending_count(), 1)
        task = self.huey.dequeue()
        self.assertEqual(task.name, 'scan_accounts')
        self.assertEqual(task.args, (['acct-1'],))
        # The task function's own "using" kwarg is forwarded untouched.
        self.assertEqual(task.kwargs, {'using': 'replica'})

    def test_interleaved_connections_commit_independently(self):
        with transaction.atomic(using='audit'):
            with transaction.atomic(using='default'):
                scan_accounts(['acct-1'])
                record_event('login')
                self.assertEqual(self.huey.pending_count(), 0)
            # "default" committed: only the default-bound task is enqueued.
            self.assertEqual(self.pending_names(), ['record_event'])
            self.assertEqual(self.huey.pending_count(), 1)
        # "audit" committed: the audit-bound task follows.
        self.assertEqual(
            sorted(self.pending_names()),
            ['record_event', 'scan_accounts'])

    def test_outer_rollback_discards_task(self):
        with transaction.atomic(using='audit'):
            scan_accounts(['acct-1'])
            transaction.set_rollback(True, using='audit')
        self.assertEqual(self.huey.pending_count(), 0)

    def test_inner_savepoint_rollback_discards_only_its_own_tasks(self):
        class Boom(Exception):
            pass

        with transaction.atomic(using='audit'):
            scan_accounts(['outer'])
            try:
                with transaction.atomic(using='audit'):
                    scan_accounts(['inner'])
                    raise Boom()
            except Boom:
                pass
            self.assertEqual(self.huey.pending_count(), 0)

        self.assertEqual(self.huey.pending_count(), 1)
        task = self.huey.dequeue()
        self.assertEqual(task.args, (['outer'],))

    def test_arguments_snapshot_at_call_time(self):
        accounts = ['acct-1']
        with transaction.atomic(using='audit'):
            scan_accounts(accounts)
            accounts.append('acct-2')

        task = self.huey.dequeue()
        self.assertEqual(task.args, (['acct-1'],))

    def test_no_transaction_enqueues_immediately(self):
        result = record_event('signup')
        self.assertEqual(self.huey.pending_count(), 1)
        task = self.huey.dequeue()
        self.assertEqual(task.name, 'record_event')
        self.assertEqual(task.args, ('signup',))
        self.assertEqual(self.huey.execute(task), 'signup')
        self.assertEqual(result.get(), 'signup')

    def test_result_handle_never_resolves_after_rollback(self):
        with transaction.atomic(using='audit'):
            result = scan_accounts(['acct-1'])
            transaction.set_rollback(True, using='audit')

        self.assertEqual(self.huey.pending_count(), 0)
        self.assertFalse(result.is_ready())
        self.assertIsNone(result.get())

    def test_enqueue_failure_after_commit_is_logged_and_raised(self):
        def broken_enqueue(data, priority=None):
            raise IOError('queue is down')

        original = self.huey.storage.enqueue
        self.huey.storage.enqueue = broken_enqueue
        try:
            with self.assertLogs('huey', level='ERROR') as logged:
                with self.assertRaises(IOError):
                    with transaction.atomic(using='audit'):
                        scan_accounts(['acct-1'])
        finally:
            self.huey.storage.enqueue = original

        self.assertEqual(self.huey.pending_count(), 0)
        self.assertTrue(any(
            'committed but the task was not enqueued' in message
            for message in logged.output))
