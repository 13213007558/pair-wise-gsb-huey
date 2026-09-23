import datetime
import os
import unittest

from huey.api import MemoryHuey
from huey.api import RedisHuey
from huey.api import SqliteHuey
from huey.expiration import CleanupReport
from huey.expiration import ExpirationPolicy
from huey.expiration import ResultKind
from huey.expiration import ResultMetadata
from huey.expiration import normalize_ttl
from huey.tests.base import BaseTestCase
from huey.tests.test_storage import redis_required


T0 = 1000000.0  # Fixed base timestamp for deterministic tests.


class TestNormalizeTTL(unittest.TestCase):
    def test_none_means_never_expires(self):
        self.assertTrue(normalize_ttl(None) is None)

    def test_zero_means_immediate(self):
        self.assertEqual(normalize_ttl(0), 0.0)

    def test_numbers_and_timedelta(self):
        self.assertEqual(normalize_ttl(10), 10.0)
        self.assertEqual(normalize_ttl(1.5), 1.5)
        self.assertEqual(normalize_ttl(datetime.timedelta(seconds=30)), 30.0)

    def test_negative_rejected(self):
        self.assertRaises(ValueError, normalize_ttl, -1)
        self.assertRaises(ValueError, normalize_ttl,
                          datetime.timedelta(seconds=-1))

    def test_invalid_type_rejected(self):
        self.assertRaises(ValueError, normalize_ttl, '10')


class TestExpirationPolicy(unittest.TestCase):
    def test_negative_ttl_rejected(self):
        self.assertRaises(ValueError, ExpirationPolicy, error=-1)
        self.assertRaises(ValueError, ExpirationPolicy, default=-1)
        policy = ExpirationPolicy()
        self.assertRaises(ValueError, policy.set_ttl, 'complete', -1)
        self.assertRaises(ValueError, policy.set_task_ttl, 't', 'error', -1)

    def test_unknown_kind_rejected(self):
        self.assertRaises(ValueError, ExpirationPolicy, bogus=10)
        policy = ExpirationPolicy()
        self.assertRaises(ValueError, policy.ttl_for, 'bogus')

    def test_resolution_order(self):
        policy = ExpirationPolicy(default=1000, complete=100,
                                  task_ttls={'special': {'complete': 5}})
        # Per-task override wins over per-kind and default.
        self.assertEqual(policy.ttl_for('complete', 'special'), 5.0)
        # Per-kind wins over default.
        self.assertEqual(policy.ttl_for('complete'), 100.0)
        self.assertEqual(policy.ttl_for('complete', 'other'), 100.0)
        # Default is the fallback.
        self.assertEqual(policy.ttl_for('error'), 1000.0)

    def test_default_is_none(self):
        policy = ExpirationPolicy()
        for kind in ResultKind.all():
            self.assertTrue(policy.ttl_for(kind) is None)

    def test_is_expired(self):
        policy = ExpirationPolicy(complete=10, error=None, retry=0)
        complete = ResultMetadata('k1', 'complete', timestamp=T0)
        error = ResultMetadata('k2', 'error', timestamp=T0)
        retry = ResultMetadata('k3', 'retry', timestamp=T0)
        self.assertFalse(policy.is_expired(complete, now=T0 + 9))
        self.assertTrue(policy.is_expired(complete, now=T0 + 10))
        self.assertFalse(policy.is_expired(error, now=T0 + 10 ** 9))
        self.assertTrue(policy.is_expired(retry, now=T0))


class ExpirationTests(object):
    """
    Backend-agnostic expiration and cleanup tests. Subclasses provide a
    Huey instance via get_huey().
    """
    def get_policy(self, **kwargs):
        defaults = {
            'complete': 100.,
            'error': 10.,
            'retry': 20.,
            'group': 50.,
            'revoked': 5.,
            'pending': 30.,
        }
        defaults.update(kwargs)
        return ExpirationPolicy(**defaults)

    def get_huey(self, **policy_kwargs):
        raise NotImplementedError

    def setUp(self):
        super(ExpirationTests, self).setUp()
        self.storage = self.huey.storage

    def make_entry(self, key, kind, timestamp=T0, task_name=None,
                   references=(), value=b'v'):
        metadata = ResultMetadata(key, kind, task_name, timestamp,
                                  references)
        self.storage.put_result_data(key, value, metadata)

    def metadata(self, key):
        return self.storage.get_result_metadata(key)

    def test_kinds_expire_independently(self):
        # Each result kind has its own TTL, computed independently.
        self.make_entry('k-complete', 'complete')
        self.make_entry('k-error', 'error')
        self.make_entry('k-retry', 'retry')
        self.make_entry('k-group', 'group')
        self.make_entry('k-revoked', 'revoked')
        self.make_entry('k-pending', 'pending')

        def kinds_remaining(now):
            report = self.huey.cleanup_results(now=now)
            remaining = set()
            for key, meta in self.storage.result_metadata_items().items():
                remaining.add(meta.kind)
            return report, remaining

        report, remaining = kinds_remaining(T0 + 4)
        self.assertEqual(report.deleted, 0)
        self.assertEqual(remaining, set(ResultKind.all()))

        report, remaining = kinds_remaining(T0 + 5)   # revoked expires.
        self.assertEqual(remaining, {'complete', 'error', 'retry', 'group',
                                     'pending'})
        report, remaining = kinds_remaining(T0 + 10)  # error expires.
        self.assertNotIn('error', remaining)
        report, remaining = kinds_remaining(T0 + 20)  # retry expires.
        self.assertNotIn('retry', remaining)
        report, remaining = kinds_remaining(T0 + 30)  # pending expires.
        self.assertNotIn('pending', remaining)
        report, remaining = kinds_remaining(T0 + 50)  # group expires.
        self.assertNotIn('group', remaining)
        report, remaining = kinds_remaining(T0 + 100)  # complete expires.
        self.assertEqual(remaining, set())

    def test_ttl_zero_not_retained(self):
        huey = self.get_huey(complete=0)
        self.assertFalse(huey.put_result('k1', 1))
        self.assertFalse(huey.storage.has_data_for_key('k1'))
        self.assertTrue(self.metadata('k1') is None)

    def test_ttl_none_never_expires(self):
        huey = self.get_huey(complete=None)
        huey.put_result('k1', 1)
        report = huey.cleanup_results(now=T0 + 10 ** 9)
        self.assertEqual(report.deleted, 0)
        self.assertEqual(huey.get('k1', peek=True), 1)

    def test_live_group_protects_members(self):
        # Members referenced by an unexpired group are kept, even when
        # their own TTL has elapsed.
        self.make_entry('g:1', 'group', references=('m1', 'm2'))
        self.make_entry('m1', 'complete')
        self.make_entry('m2', 'complete')

        # Use a policy where the group outlives its members.
        self.huey.expiration_policy = self.get_policy(complete=10,
                                                      group=100)
        report = self.huey.cleanup_results(now=T0 + 50)
        self.assertEqual(report.deleted, 0)
        self.assertEqual(report.skipped_referenced, 2)
        self.assertTrue(self.storage.has_data_for_key('m1'))
        self.assertTrue(self.storage.has_data_for_key('m2'))

        # Once the group itself expires, the group and its members are
        # removed together.
        report = self.huey.cleanup_results(now=T0 + 100)
        self.assertEqual(report.deleted, 3)
        self.assertFalse(self.storage.has_data_for_key('g:1'))
        self.assertFalse(self.storage.has_data_for_key('m1'))
        self.assertFalse(self.storage.has_data_for_key('m2'))

    def test_expired_group_deleted_before_members(self):
        self.make_entry('g:1', 'group', references=('m1', 'm2'))
        self.make_entry('m1', 'complete')
        self.make_entry('m2', 'complete')

        deleted_order = []
        original = self.storage.delete_result_if_unmodified
        def spy(key, timestamp):
            deleted_order.append(key)
            return original(key, timestamp)
        self.storage.delete_result_if_unmodified = spy
        try:
            report = self.huey.cleanup_results(now=T0 + 10 ** 6)
        finally:
            self.storage.delete_result_if_unmodified = original

        self.assertEqual(report.deleted, 3)
        # The group key is deleted first, so readers never see a group
        # whose members have already been removed.
        self.assertEqual(deleted_order[0], 'g:1')

    def test_pending_entries(self):
        self.make_entry('p1', 'pending')
        # Unexpired pending entries are kept and reported.
        report = self.huey.cleanup_results(now=T0 + 10)
        self.assertEqual(report.deleted, 0)
        self.assertEqual(report.skipped_pending, 1)
        # Once the pending TTL elapses, the placeholder is removed.
        report = self.huey.cleanup_results(now=T0 + 30)
        self.assertEqual(report.deleted, 1)
        self.assertTrue(self.metadata('p1') is None)

    def test_lost_update_protection(self):
        self.make_entry('k1', 'error', timestamp=T0)
        # Simulate a worker rewriting the result after the cleanup scan
        # has read the metadata: the timestamp changes.
        self.make_entry('k1', 'error', timestamp=T0 + 1, value=b'v2')
        # A delete using the stale timestamp must not remove the new value.
        self.assertFalse(
            self.storage.delete_result_if_unmodified('k1', T0))
        self.assertEqual(self.storage.peek_data('k1'), b'v2')

    def test_segmented_cleanup_with_cursor(self):
        for i in range(5):
            self.make_entry('k%d' % i, 'error')

        # First segment examines and deletes two entries.
        report = self.huey.cleanup_results(limit=2, now=T0 + 100)
        self.assertEqual((report.scanned, report.deleted), (2, 2))
        self.assertEqual(report.cursor, 'k1')

        # Resuming from the cursor continues where we left off.
        report = self.huey.cleanup_results(limit=2, cursor=report.cursor,
                                           now=T0 + 100)
        self.assertEqual((report.scanned, report.deleted), (2, 2))
        self.assertEqual(report.cursor, 'k3')

        # Final segment: fewer entries than the limit, cursor is None.
        report = self.huey.cleanup_results(limit=2, cursor=report.cursor,
                                           now=T0 + 100)
        self.assertEqual((report.scanned, report.deleted), (1, 1))
        self.assertTrue(report.cursor is None)

        # A full pass confirms everything is gone; reports are stable.
        report = self.huey.cleanup_results(now=T0 + 100)
        self.assertEqual(report, CleanupReport(0, 0, 0, 0, None))
        self.assertEqual(report, self.huey.cleanup_results(now=T0 + 100))

    def test_zero_limit_is_noop(self):
        self.make_entry('kz', 'error')
        report = self.huey.cleanup_results(limit=0, now=T0 + 100)
        self.assertEqual(report.scanned, 0)
        self.assertEqual(report.deleted, 0)

    def test_cursor_is_repeatable(self):
        # A segmented cleanup can be safely retried from the same cursor.
        for i in range(4):
            self.make_entry('k%d' % i, 'error')
        first = self.huey.cleanup_results(limit=2, now=T0 + 100)
        self.assertEqual(first.cursor, 'k1')
        # The first attempt deletes the remaining entries; since the scan
        # is then complete, the cursor is None.
        attempt = self.huey.cleanup_results(limit=2, cursor=first.cursor,
                                            now=T0 + 100)
        self.assertEqual((attempt.deleted, attempt.cursor), (2, None))
        # Retrying the same segment is a no-op and returns a stable,
        # empty report.
        retry = self.huey.cleanup_results(limit=2, cursor=first.cursor,
                                          now=T0 + 100)
        self.assertEqual(retry, CleanupReport(0, 0, 0, 0, None))
        self.assertEqual(retry, self.huey.cleanup_results(
            limit=2, cursor=first.cursor, now=T0 + 100))

    def test_enqueue_writes_pending_metadata(self):
        executed = []
        @self.huey.task()
        def task_ok():
            executed.append(1)
            return 3

        result = task_ok()
        # Enqueueing records a pending placeholder for the unfinished
        # result.
        metadata = self.metadata(result.id)
        self.assertEqual(metadata.kind, 'pending')
        self.assertEqual(metadata.task_name, 'task_ok')

        # Executing the task replaces the placeholder with the result.
        task = self.huey.dequeue()
        self.huey.execute(task)
        metadata = self.metadata(result.id)
        self.assertEqual(metadata.kind, 'complete')
        self.assertEqual(result(), 3)

    def test_error_and_retry_kinds(self):
        @self.huey.task()
        def task_fail():
            raise ValueError('nope')

        @self.huey.task(retries=1)
        def task_retry():
            raise ValueError('nope')

        result = task_fail()
        self.huey.execute(self.huey.dequeue())
        self.assertEqual(self.metadata(result.id).kind, 'error')

        result = task_retry()
        self.huey.execute(self.huey.dequeue())
        self.assertEqual(self.metadata(result.id).kind, 'retry')

    def test_revocation_metadata(self):
        @self.huey.task()
        def task_a():
            pass

        self.huey.revoke_by_id('task-id-1')
        metadata = self.metadata('r:task-id-1')
        self.assertEqual(metadata.kind, 'revoked')

        task_a.revoke()
        key = self.huey._task_key(task_a.task_class, 'rt')
        metadata = self.metadata(key)
        self.assertEqual(metadata.kind, 'revoked')

        # Restoring removes the marker and its metadata.
        task_a.restore()
        self.assertTrue(self.metadata(key) is None)

    def test_put_group_records_references(self):
        self.huey.put_group('group-1', ['t1', 't2'], data='summary')
        metadata = self.metadata('g:group-1')
        self.assertEqual(metadata.kind, 'group')
        self.assertEqual(metadata.references, ('t1', 't2'))


class TestMemoryExpiration(ExpirationTests, BaseTestCase):
    def get_huey(self, **policy_kwargs):
        return MemoryHuey(utc=False,
                          result_expiration=self.get_policy(**policy_kwargs))


class TestSqliteExpiration(ExpirationTests, BaseTestCase):
    filename = 'huey_expiration_test.db'

    def get_huey(self, **policy_kwargs):
        return SqliteHuey(filename=self.filename, utc=False,
                          result_expiration=self.get_policy(**policy_kwargs))

    def tearDown(self):
        super(TestSqliteExpiration, self).tearDown()
        self.huey.storage.close()
        for suffix in ('', '-wal', '-shm'):
            path = self.filename + suffix
            if os.path.exists(path):
                os.unlink(path)

    def test_metadata_survives_restart(self):
        self.make_entry('k1', 'error')
        self.make_entry('k2', 'complete')
        self.huey.storage.close()

        # A new Huey instance over the same database sees the metadata and
        # can continue the cleanup, e.g. resuming from a cursor.
        huey2 = self.get_huey()
        try:
            items = huey2.storage.result_metadata_items()
            self.assertEqual(sorted(items), ['k1', 'k2'])
            report = huey2.cleanup_results(limit=1, now=T0 + 50)
            self.assertEqual((report.scanned, report.deleted), (1, 1))
            self.assertEqual(report.cursor, 'k1')
            report = huey2.cleanup_results(limit=1, cursor=report.cursor,
                                           now=T0 + 50)
            self.assertEqual((report.scanned, report.deleted), (1, 0))
            self.assertTrue(report.cursor is None)
            report = huey2.cleanup_results(now=T0 + 200)
            self.assertEqual(report.deleted, 1)
            self.assertEqual(huey2.storage.result_metadata_items(), {})
        finally:
            huey2.storage.close()


@redis_required
class TestRedisExpiration(ExpirationTests, BaseTestCase):
    def get_huey(self, **policy_kwargs):
        return RedisHuey(utc=False,
                         result_expiration=self.get_policy(**policy_kwargs))

    def setUp(self):
        super(TestRedisExpiration, self).setUp()
        self.storage.flush_all()

    def tearDown(self):
        self.storage.flush_all()
        super(TestRedisExpiration, self).tearDown()
        self.assertTrue(
            self.storage.delete_result_if_unmodified('k1', T0 + 1))
        self.assertFalse(self.storage.has_data_for_key('k1'))

    def test_cleanup_uses_conditional_delete(self):
        self.make_entry('k1', 'error', timestamp=T0)
        original = self.storage.delete_result_if_unmodified
        def rewrite_then_delete(key, timestamp):
            # Worker rewrites the entry between scan and delete.
            self.make_entry(key, 'error', timestamp=T0 + 1, value=b'v2')
            return original(key, timestamp)
        self.storage.delete_result_if_unmodified = rewrite_then_delete
        try:
            report = self.huey.cleanup_results(now=T0 + 100)
        finally:
            self.storage.delete_result_if_unmodified = original
        # The entry was rewritten, so the stale delete did not happen.
        self.assertEqual(report.deleted, 0)
        self.assertEqual(self.storage.peek_data('k1'), b'v2')

    def test_per_task_ttl_overrides_kind(self):
        policy = self.get_policy()
        policy.set_task_ttl('special', 'complete', 1)
        huey = self.get_huey()
        huey.expiration_policy = policy
        huey.put_result('k1', 1, task_name='special')
        huey.put_result('k2', 2, task_name='normal')
        report = huey.cleanup_results(now=self.metadata('k1').timestamp + 2)
        self.assertEqual(report.deleted, 1)
        self.assertTrue(self.metadata('k1') is None)
        self.assertEqual(self.metadata('k2').kind, 'complete')

    def test_cleanup_is_idempotent(self):
        self.make_entry('k1', 'error')
        self.make_entry('k2', 'error')
        first = self.huey.cleanup_results(now=T0 + 100)
        self.assertEqual(first.deleted, 2)
        self.assertEqual(first.scanned, 2)
        # Repeating the call is safe and returns a stable, empty report.
        second = self.huey.cleanup_results(now=T0 + 100)
        self.assertEqual(second.deleted, 0)
        self.assertEqual(second.scanned, 0)
        self.assertEqual(second, self.huey.cleanup_results(now=T0 + 100))

    def test_cleanup_without_policy_deletes_nothing(self):
        huey = self.get_huey()
        huey.expiration_policy = None
        huey.put_result('k1', 1)
        report = huey.cleanup_results(now=T0 + 10 ** 9)
        self.assertEqual(report.deleted, 0)
        self.assertEqual(huey.get('k1', peek=True), 1)
