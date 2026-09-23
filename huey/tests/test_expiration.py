import datetime
import os
import time
import unittest

from huey.api import MemoryHuey
from huey.api import SqliteHuey
from huey.constants import EmptyData
from huey.expiration import RESULT_CATEGORIES
from huey.expiration import ResultExpirationPolicy
from huey.expiration import decode_result_meta
from huey.expiration import encode_result_meta
from huey.expiration import normalize_ttl


def redis_available():
    try:
        from redis import Redis
    except ImportError:
        return False
    try:
        Redis(socket_connect_timeout=1).ping()
    except Exception:
        return False
    return True


REDIS = redis_available()


class TestExpirationPolicy(unittest.TestCase):
    def test_normalize_ttl(self):
        self.assertEqual(normalize_ttl(None), None)
        self.assertEqual(normalize_ttl(0), 0.0)
        self.assertEqual(normalize_ttl(10), 10.0)
        self.assertEqual(normalize_ttl(datetime.timedelta(minutes=2)), 120.0)
        self.assertRaises(ValueError, normalize_ttl, -1)
        self.assertRaises(ValueError, normalize_ttl, -0.5)
        self.assertRaises(ValueError, normalize_ttl, 'x')
        self.assertRaises(ValueError, normalize_ttl, True)

    def test_from_config_scalar(self):
        policy = ResultExpirationPolicy.from_config(60)
        for category in RESULT_CATEGORIES:
            self.assertEqual(policy.ttl_for(category), 60.0)
        policy = ResultExpirationPolicy.from_config(
            datetime.timedelta(hours=1))
        self.assertEqual(policy.ttl_for('complete'), 3600.0)

    def test_from_config_none_means_no_expiration(self):
        policy = ResultExpirationPolicy.from_config(None)
        for category in RESULT_CATEGORIES:
            self.assertEqual(policy.ttl_for(category), None)
            self.assertFalse(policy.is_expired(category, 0, 10 ** 12))

    def test_from_config_dict(self):
        policy = ResultExpirationPolicy.from_config({
            'default': 100,
            'complete': 1000,
            'retry': 10,
            'group': None,
        })
        self.assertEqual(policy.ttl_for('complete'), 1000.0)
        self.assertEqual(policy.ttl_for('retry'), 10.0)
        self.assertEqual(policy.ttl_for('group'), None)
        self.assertEqual(policy.ttl_for('error'), 100.0)
        self.assertEqual(policy.ttl_for('pending'), 100.0)

    def test_from_config_per_task(self):
        policy = ResultExpirationPolicy.from_config({
            'default': 100,
            'complete': 1000,
            'tasks': {
                'myapp.tasks.feed': 5,
                'other': {'complete': 1, 'retry': 2},
            },
        })
        self.assertEqual(policy.ttl_for('complete', 'myapp.tasks.feed'), 5.0)
        self.assertEqual(policy.ttl_for('error', 'myapp.tasks.feed'), 5.0)
        self.assertEqual(policy.ttl_for('complete', 'other'), 1.0)
        self.assertEqual(policy.ttl_for('retry', 'other'), 2.0)
        self.assertEqual(policy.ttl_for('error', 'other'), 100.0)
        self.assertEqual(policy.ttl_for('complete', 'unknown'), 1000.0)

    def test_task_config_argument(self):
        policy = ResultExpirationPolicy.from_config({'complete': 10})
        self.assertEqual(policy.ttl_for('complete', 't', 5), 5.0)
        self.assertEqual(
            policy.ttl_for('complete', 't', {'complete': 3}), 3.0)
        self.assertEqual(
            policy.ttl_for('error', 't', {'complete': 3}), None)

    def test_invalid_config(self):
        self.assertRaises(ValueError, ResultExpirationPolicy.from_config,
                          {'complete': -1})
        self.assertRaises(ValueError, ResultExpirationPolicy.from_config,
                          {'default': -1})
        self.assertRaises(ValueError, ResultExpirationPolicy.from_config,
                          {'tasks': {'t': -1}})
        self.assertRaises(ValueError, ResultExpirationPolicy.from_config,
                          {'bogus-category': 1})
        self.assertRaises(ValueError, ResultExpirationPolicy.from_config,
                          {'tasks': {'t': {'bogus': 1}}})
        self.assertRaises(ValueError, ResultExpirationPolicy.from_config,
                          object())

    def test_is_expired(self):
        policy = ResultExpirationPolicy.from_config({'complete': 10})
        self.assertFalse(policy.is_expired('complete', 100, 109.9))
        self.assertTrue(policy.is_expired('complete', 100, 110))
        policy = ResultExpirationPolicy.from_config({'complete': 0})
        self.assertTrue(policy.is_expired('complete', 100, 100))
        policy = ResultExpirationPolicy.from_config({'complete': None})
        self.assertFalse(policy.is_expired('complete', 100, 10 ** 12))
        policy = ResultExpirationPolicy.from_config({'complete': 10})
        self.assertFalse(policy.is_expired('complete', None, 10 ** 12))

    def test_meta_codec(self):
        meta = {'ts': 1.5, 'cat': 'complete', 'task': 'a.b.c'}
        encoded = encode_result_meta(meta)
        self.assertEqual(decode_result_meta(encoded), meta)
        self.assertEqual(decode_result_meta(encoded.encode('utf8')), meta)
        flipped = dict((k, meta[k]) for k in reversed(list(meta)))
        self.assertEqual(encoded, encode_result_meta(flipped))
        self.assertTrue(decode_result_meta('not json') is None)
        self.assertTrue(decode_result_meta('[1, 2]') is None)
        self.assertTrue(decode_result_meta(None) is None)


class CleanupTests(object):
    # Shared behavior tests for backends that support result metadata.
    # Subclasses must provide get_huey().

    def get_huey(self, **kwargs):
        raise NotImplementedError

    def setUp(self):
        super(CleanupTests, self).setUp()
        self.huey = self.get_huey()
        self.huey.flush()

    def tearDown(self):
        self.huey.flush()
        super(CleanupTests, self).tearDown()

    def execute_next(self):
        task = self.huey.dequeue()
        self.assertTrue(task is not None)
        self.huey.execute(task)
        return task

    def test_categories_assigned_on_write(self):
        huey = self.huey

        @huey.task()
        def ok_task():
            return 'ok'

        @huey.task()
        def err_task():
            raise Exception('boom')

        @huey.task(retries=1)
        def retry_task():
            raise Exception('again')

        r_ok = ok_task()
        r_err = err_task()
        r_retry = retry_task()
        t_ok = self.execute_next()
        t_err = self.execute_next()
        t_retry = self.execute_next()

        storage = huey.storage
        self.assertEqual(storage.get_result_meta(t_ok.id)['cat'], 'complete')
        self.assertEqual(storage.get_result_meta(t_err.id)['cat'], 'error')
        self.assertEqual(storage.get_result_meta(t_retry.id)['cat'], 'retry')
        self.assertEqual(r_ok.get(), 'ok')
        self.assertRaises(Exception, r_err.get)

    def test_category_ttls_are_independent(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'retry': 10,
            'error': 100,
            'complete': 1000,
        })
        now = time.time()
        storage = huey.storage
        storage.put_result('k-retry', b'v', {
            'ts': now - 50, 'cat': 'retry', 'task': 't'})
        storage.put_result('k-error', b'v', {
            'ts': now - 50, 'cat': 'error', 'task': 't'})
        storage.put_result('k-complete', b'v', {
            'ts': now - 50, 'cat': 'complete', 'task': 't'})

        report = huey.cleanup_expired_results(now=now)
        self.assertEqual(report['deleted'], ['k-retry'])
        self.assertEqual(report['deleted_count'], 1)
        self.assertTrue(report['done'])
        self.assertTrue(storage.peek_data('k-retry') is EmptyData)
        self.assertEqual(storage.peek_data('k-error'), b'v')
        self.assertEqual(storage.peek_data('k-complete'), b'v')

        # Second run is a stable no-op.
        report2 = huey.cleanup_expired_results(now=now)
        self.assertEqual(report2['deleted'], [])
        self.assertEqual(report2['deleted_count'], 0)
        self.assertEqual(report2['groups_expired'], [])
        self.assertTrue(report2['done'])

        # Advance past the error TTL.
        report3 = huey.cleanup_expired_results(now=now + 60)
        self.assertEqual(report3['deleted'], ['k-error'])
        report4 = huey.cleanup_expired_results(now=now + 1000)
        self.assertEqual(report4['deleted'], ['k-complete'])

    def test_ttl_zero_and_none(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'complete': 0,
            'error': None,
        })
        now = time.time()
        storage = huey.storage
        storage.put_result('k-zero', b'v', {
            'ts': now, 'cat': 'complete', 'task': 't'})
        storage.put_result('k-never', b'v', {
            'ts': now, 'cat': 'error', 'task': 't'})

        report = huey.cleanup_expired_results(now=now)
        self.assertEqual(report['deleted'], ['k-zero'])
        report = huey.cleanup_expired_results(now=now + 10 ** 9)
        self.assertEqual(report['deleted'], [])
        self.assertEqual(storage.peek_data('k-never'), b'v')

    def test_per_task_ttl(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'default': 1000,
            'tasks': {'special': 5},
        })

        @huey.task(name='special')
        def special():
            return 1

        @huey.task()
        def ordinary():
            return 2

        special()
        ordinary()
        t_special = self.execute_next()
        t_ordinary = self.execute_next()

        now = time.time()
        report = huey.cleanup_expired_results(now=now + 10)
        self.assertEqual(report['deleted'], [t_special.id])
        report = huey.cleanup_expired_results(now=now + 2000)
        self.assertEqual(report['deleted'], [t_ordinary.id])

    def test_task_class_result_ttl_override(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'complete': 1000,
        })

        @huey.task(result_ttl=5)
        def custom():
            return 1

        custom()
        task = self.execute_next()
        now = time.time()
        report = huey.cleanup_expired_results(now=now + 10)
        self.assertEqual(report['deleted'], [task.id])

    def test_revoked_category(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'revoked': 10,
        })

        @huey.task()
        def some_task():
            return 1

        task = some_task.s()
        huey.revoke(task)
        meta = huey.storage.get_result_meta(task.revoke_id)
        self.assertEqual(meta['cat'], 'revoked')

        now = time.time()
        report = huey.cleanup_expired_results(now=now + 5)
        self.assertEqual(report['deleted'], [])
        report = huey.cleanup_expired_results(now=now + 20)
        self.assertEqual(report['deleted'], [task.revoke_id])
        self.assertFalse(huey.storage.has_data_for_key(task.revoke_id))

    def test_pending_entries_are_preserved(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'default': 0,
        })
        # Entry with no metadata at all (e.g. a lock or arbitrary put()).
        huey.storage.put_data('no-meta', b'v')
        report = huey.cleanup_expired_results(now=time.time() + 10 ** 6)
        self.assertEqual(report['deleted'], [])
        self.assertEqual(huey.storage.peek_data('no-meta'), b'v')

    def test_group_protects_members(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'complete': 0,  # Member results expire immediately...
            'group': None,  # ...but the group lives forever.
        })

        @huey.task()
        def add(a, b):
            return a + b

        group = huey.enqueue_group([add.s(1, 2), add.s(3, 4)])
        self.assertTrue(group.group_id.startswith('g:'))
        t1 = self.execute_next()
        t2 = self.execute_next()
        self.assertEqual(group.get(), [3, 7])

        # Even though the member TTL is 0, the live group protects them.
        report = huey.cleanup_expired_results(now=time.time() + 100)
        self.assertEqual(report['deleted'], [])
        self.assertEqual(report['groups_expired'], [])
        self.assertEqual(report['skipped_referenced'], 2)
        self.assertEqual(group.get(), [3, 7])

    def test_group_expires_before_members(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'complete': 0,
            'group': 10,
        })

        @huey.task()
        def add(a, b):
            return a + b

        group = huey.enqueue_group([add.s(1, 2), add.s(3, 4)])
        t1 = self.execute_next()
        t2 = self.execute_next()

        # Once the group itself expires, the group metadata is deleted in
        # the same run as (and reported separately from) the members.
        report = huey.cleanup_expired_results(now=time.time() + 100)
        self.assertEqual(report['groups_expired'], [group.group_id])
        self.assertEqual(sorted(report['deleted']), sorted([t1.id, t2.id]))
        self.assertTrue(huey.storage.get_result_meta(group.group_id) is None)

    def test_cleanup_is_idempotent_across_calls(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'complete': 0,
        })
        storage = huey.storage
        now = time.time()
        for i in range(3):
            storage.put_result('k%d' % i, b'v', {
                'ts': now, 'cat': 'complete', 'task': 't'})

        first = huey.cleanup_expired_results(now=now)
        self.assertEqual(first['deleted'], ['k0', 'k1', 'k2'])
        self.assertEqual(first['deleted_count'], 3)
        # Repeated calls (e.g. ops retries) return a stable, empty report.
        for _ in range(2):
            again = huey.cleanup_expired_results(now=now)
            self.assertEqual(again['deleted'], [])
            self.assertEqual(again['deleted_count'], 0)
            self.assertEqual(again['groups_expired'], [])
            self.assertTrue(again['done'])
            self.assertTrue(again['next_cursor'] is None)


    def test_segmented_cleanup_cursors(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'complete': 10,
        })
        storage = huey.storage
        now = time.time()
        keys = ['k%d' % i for i in range(5)]
        for key in keys:
            storage.put_result(key, b'v', {
                'ts': now, 'cat': 'complete', 'task': 't'})

        # Nothing is expired yet: the cursor advances over the scanned
        # entries without deleting anything, and a given cursor always
        # identifies the same segment (repeatable).
        seg1 = huey.cleanup_expired_results(now=now, limit=2)
        self.assertEqual(seg1['deleted'], [])
        self.assertEqual(seg1['scanned'], 2)
        self.assertFalse(seg1['done'])
        self.assertTrue(seg1['next_cursor'] is not None)
        seg1b = huey.cleanup_expired_results(now=now, limit=2)
        self.assertEqual(seg1['next_cursor'], seg1b['next_cursor'])

        # Resuming from the cursor continues with the next segment.
        seg2 = huey.cleanup_expired_results(now=now, limit=2,
                                          cursor=seg1['next_cursor'])
        self.assertEqual(seg2['deleted'], [])
        self.assertEqual(seg2['scanned'], 2)
        self.assertFalse(seg2['done'])

        # Once the entries expire, drain them segment by segment.
        deleted = []
        cursor = None
        for _ in range(5):
            seg = huey.cleanup_expired_results(now=now + 100, limit=2,
                                               cursor=cursor)
            deleted.extend(seg['deleted'])
            cursor = seg['next_cursor']
            if seg['done']:
                break
        self.assertEqual(sorted(deleted), sorted(keys))
        self.assertTrue(cursor is None)

        # Re-running a completed sweep (even from an old cursor) is a
        # stable no-op.
        seg = huey.cleanup_expired_results(now=now + 100, limit=2,
                                           cursor=seg1['next_cursor'])
        self.assertEqual(seg['deleted'], [])
        self.assertTrue(seg['done'])

    def test_lost_update_is_prevented(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'complete': 10,
        })
        storage = huey.storage
        now = time.time()
        storage.put_result('k', b'old', {
            'ts': now - 100, 'cat': 'complete', 'task': 't'})
        meta = storage.get_result_meta('k')

        # Simulate a worker re-writing the result after cleanup read it.
        storage.put_result('k', b'new', {
            'ts': now, 'cat': 'complete', 'task': 't'})
        self.assertFalse(storage.delete_result_if_matches('k', meta))
        self.assertEqual(storage.peek_data('k'), b'new')

        # And the cleanup itself leaves the fresh result alone.
        report = huey.cleanup_expired_results(now=now)
        self.assertEqual(report['deleted'], [])
        self.assertEqual(storage.peek_data('k'), b'new')

        # Once the fresh write also expires, cleanup removes it.
        report = huey.cleanup_expired_results(now=now + 20)
        self.assertEqual(report['deleted'], ['k'])

    def test_flush_results_clears_meta(self):
        huey = self.huey
        huey.storage.put_result('k', b'v', {
            'ts': time.time(), 'cat': 'complete', 'task': 't'})
        huey.storage.flush_results()
        self.assertTrue(huey.storage.get_result_meta('k') is None)
        report = huey.cleanup_expired_results()
        self.assertEqual(report['scanned'], 0)


class TestMemoryCleanup(CleanupTests, unittest.TestCase):
    def get_huey(self, **kwargs):
        kwargs.setdefault('utc', False)
        return MemoryHuey('test-expiration', **kwargs)



class TestSqliteCleanup(CleanupTests, unittest.TestCase):
    filename = 'huey_expiration_test.db'

    def tearDown(self):
        super(TestSqliteCleanup, self).tearDown()
        self.huey.storage.close()
        for suffix in ('', '-wal', '-shm'):
            if os.path.exists(self.filename + suffix):
                os.unlink(self.filename + suffix)

    def get_huey(self, **kwargs):
        kwargs.setdefault('utc', False)
        return SqliteHuey('test-expiration', filename=self.filename,
                          timeout=3, **kwargs)


@unittest.skipUnless(REDIS, 'redis server not available')
class TestRedisCleanup(CleanupTests, unittest.TestCase):
    def get_huey(self, **kwargs):
        from huey.api import RedisHuey
        kwargs.setdefault('utc', False)
        return RedisHuey('test-expiration', blocking=False, **kwargs)


@unittest.skipUnless(REDIS, 'redis server not available')
class TestRedisExpireCleanup(CleanupTests, unittest.TestCase):
    def get_huey(self, **kwargs):
        from huey.api import RedisExpireHuey
        kwargs.setdefault('utc', False)
        return RedisExpireHuey('test-expiration', expire_time=3600,
                               blocking=False, **kwargs)

    def test_native_ttl_applied_from_policy(self):
        huey = self.huey
        huey.result_store_expiration = ResultExpirationPolicy.from_config({
            'complete': 30,
        })
        storage = huey.storage
        storage.put_result('k', b'v', {
            'ts': time.time(), 'cat': 'complete', 'task': 't'}, ttl=30)
        self.assertTrue(0 < storage.conn.ttl(storage.result_key('k')) <= 30)


if __name__ == '__main__':
    unittest.main()
