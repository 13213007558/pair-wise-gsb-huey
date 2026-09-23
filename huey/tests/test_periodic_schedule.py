import calendar
import datetime
import os
import tempfile
import unittest
try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        ZoneInfo = None

from huey.api import MemoryHuey
from huey.api import SqliteHuey
from huey.api import crontab
from huey.constants import EmptyData
from huey.utils import normalize_time
from huey.utils import pack_periodic_state
from huey.utils import unpack_periodic_state


class PeriodicScheduleTests(unittest.TestCase):
    def setUp(self):
        self.huey = MemoryHuey('periodic-tests', utc=True)

    def test_interval_does_not_duplicate_across_restart_or_clock_rollback(self):
        @self.huey.periodic_task(interval_seconds=60, timezone='UTC')
        def tick():
            pass

        now = datetime.datetime(2024, 1, 1, 0, 0, 30)
        first = self.huey.enqueue_due_periodic(now)
        self.assertEqual([task.eta for task in first],
                         [datetime.datetime(2024, 1, 1, 0, 0)])

        second = self.huey.enqueue_due_periodic(
            now - datetime.timedelta(seconds=30))
        self.assertEqual(second, [])

        restarted = MemoryHuey('periodic-tests', utc=True)
        wrapper = type(tick.s())
        restarted._registry.register(wrapper)
        restarted.storage._periodic = dict(self.huey.storage._periodic)
        self.assertEqual(restarted.enqueue_due_periodic(now), [])

        next_now = now + datetime.timedelta(seconds=31)
        self.assertEqual([task.eta for task in restarted.enqueue_due_periodic(
            next_now)], [datetime.datetime(2024, 1, 1, 0, 1)])

        def make_compare_huey():
            huey = MemoryHuey('timezone-compare')

            @huey.periodic_task(interval_seconds=60, timezone='UTC',
                                name='stable-tick')
            def stable_tick():
                pass

            huey.storage._periodic[huey._periodic_state_key(
                huey._registry.periodic_tasks[0])] = pack_periodic_state(
                    datetime.datetime(2024, 1, 1))
            return huey

        compare_a = make_compare_huey()
        compare_b = make_compare_huey()
        task_a, = compare_a.enqueue_due_periodic(next_now)
        task_b, = compare_b.enqueue_due_periodic(next_now)
        self.assertEqual(task_a.id, task_b.id)
        self.assertEqual(compare_a.serialize_task(task_a),
                         compare_b.serialize_task(task_b))

    def test_cron_dst_gap_and_fold_have_distinct_utc_runs(self):
        if ZoneInfo is None:
            raise unittest.SkipTest('ZoneInfo is required')
        @self.huey.periodic_task(crontab(minute='30', hour='2'),
                                 timezone='America/New_York')
        def task():
            pass

        wrapper, = self.huey._registry.periodic_tasks
        schedule = wrapper.periodic_schedule.resolve('UTC')

        spring = [run for run in schedule.due(
            None,
            datetime.datetime(2024, 3, 10, 8),
            datetime.datetime(2024, 3, 10, 6, 30))]
        self.assertEqual(spring, [])

        fall = [run for run in schedule.due(
            None,
            datetime.datetime(2024, 11, 3, 7, 31),
            datetime.datetime(2024, 11, 3, 6, 29))]
        self.assertEqual(fall, [
            datetime.datetime(2024, 11, 3, 6, 30),
            datetime.datetime(2024, 11, 3, 7, 30),
        ])

    def test_start_and_end_do_not_reset_last_run(self):
        start = datetime.datetime(2024, 1, 1, 0, 0,
                                  tzinfo=ZoneInfo('Asia/Shanghai'))
        end = datetime.datetime(2024, 1, 1, 0, 2,
                                tzinfo=ZoneInfo('Asia/Shanghai'))

        @self.huey.periodic_task(interval_seconds=60, start_time=start,
                                 end_time=end,
                                 timezone='Asia/Shanghai')
        def bounded():
            pass

        runs = self.huey.enqueue_due_periodic(
            datetime.datetime(2023, 12, 31, 16, 3),
            horizon=datetime.datetime(2023, 12, 31, 15))
        self.assertEqual([task.eta for task in runs], [
            datetime.datetime(2023, 12, 31, 16, 0),
            datetime.datetime(2023, 12, 31, 16, 1),
            datetime.datetime(2023, 12, 31, 16, 2),
        ])
        self.assertEqual(self.huey.enqueue_due_periodic(
            datetime.datetime(2023, 12, 31, 16, 3)), [])

    def test_retry_eta_does_not_change_periodic_state(self):
        @self.huey.periodic_task(interval_seconds=60, timezone='UTC')
        def retry_task():
            pass

        now = datetime.datetime(2024, 1, 1, 0, 0, 30)
        task, = self.huey.enqueue_due_periodic(now)
        task.eta = now + datetime.timedelta(hours=1)
        self.huey.add_schedule(task)
        self.assertEqual(self.huey.enqueue_due_periodic(now), [])

    def test_concurrent_scheduler_claims_are_exclusive(self):
        @self.huey.periodic_task(interval_seconds=60, timezone='UTC')
        def task():
            pass

        key = self.huey._periodic_state_key(
            self.huey._registry.periodic_tasks[0])
        current = self.huey.storage.read_periodic_task(key)
        self.assertIs(current, EmptyData)
        value = pack_periodic_state(datetime.datetime(2024, 1, 1))
        self.assertTrue(self.huey.storage.claim_periodic_task(
            key, current, value))
        self.assertFalse(self.huey.storage.claim_periodic_task(
            key, current, value))

    def test_schedule_and_interval_aliases(self):
        @self.huey.periodic_task(cron=crontab(minute='0'),
                                 timezone='UTC', name='cron-alias')
        def cron_task():
            pass

        cron_class, = [task for task in self.huey._registry.periodic_tasks
                       if task.name == 'cron-alias']
        self.assertIsNotNone(cron_class.periodic_schedule.validator)

        @self.huey.periodic_task(period=60, timezone='UTC',
                                 name='interval-alias')
        def interval_task():
            pass

        interval_class, = [task for task in self.huey._registry.periodic_tasks
                           if task.name == 'interval-alias']
        self.assertEqual(
            interval_class.periodic_schedule.interval_seconds, 60)


class SqlitePeriodicPersistenceTests(unittest.TestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(suffix='.db', delete=False,
                                             dir=os.getcwd())
        handle.close()
        os.unlink(handle.name)
        self.filename = os.path.abspath(handle.name)
        self.huey = SqliteHuey(filename=self.filename, utc=True)

        @self.huey.periodic_task(interval_seconds=60, timezone='UTC',
                                 name='persistent-tick')
        def tick():
            pass

    def tearDown(self):
        self.huey.storage.close()
        if os.path.exists(self.filename):
            os.unlink(self.filename)

    def test_restart_bytes_eta_queue_and_enqueue_count_are_stable(self):
        now = datetime.datetime(2024, 1, 1, 0, 0, 30)
        task, = self.huey.enqueue_due_periodic(now)
        first_bytes = self.huey.serialize_task(task)
        state_key = self.huey._periodic_state_key(task)
        state_bytes = self.huey.storage.read_periodic_task(state_key)
        self.assertEqual(unpack_periodic_state(state_bytes),
                         datetime.datetime(2024, 1, 1, 0, 0))

        self.huey.storage.close()
        restarted = SqliteHuey(filename=self.filename, utc=True)

        @restarted.periodic_task(interval_seconds=60, timezone='UTC',
                                 name='persistent-tick')
        def tick():
            pass

        self.assertEqual(restarted.enqueue_due_periodic(now), [])
        restarted.storage.close()

        reopened = SqliteHuey(filename=self.filename, utc=True)

        @reopened.periodic_task(interval_seconds=60, timezone='UTC',
                                name='persistent-tick')
        def tick():
            pass

        next_task, = reopened.enqueue_due_periodic(
            now + datetime.timedelta(seconds=31))
        self.assertEqual(next_task.eta,
                         datetime.datetime(2024, 1, 1, 0, 1))
        self.assertEqual(reopened.serialize_task(next_task),
                         reopened.serialize_task(next_task))
        reopened.storage.close()

    def test_schedule_timestamps_are_interpreted_as_utc(self):
        run_at = datetime.datetime(2024, 1, 1, 0, 0)
        self.huey.storage.add_to_schedule(b'message', run_at)
        cursor = self.huey.storage.conn.execute(
            'select timestamp from schedule where queue=?',
            (self.huey.name,))
        stored, = cursor.fetchone()
        self.assertEqual(stored, calendar.timegm(run_at.timetuple()))
        self.assertEqual(self.huey.storage.read_schedule(run_at), [b'message'])

    def test_naive_and_aware_eta_use_configured_default_timezone(self):
        if ZoneInfo is None:
            raise unittest.SkipTest('ZoneInfo is required')
        naive = datetime.datetime(2024, 1, 1, 8, 0)
        aware = naive.replace(tzinfo=ZoneInfo('Asia/Shanghai'))
        expected = datetime.datetime(2024, 1, 1, 0, 0)
        self.assertEqual(normalize_time(eta=naive, utc=True,
                                        timezone='Asia/Shanghai'), expected)
        self.assertEqual(normalize_time(eta=aware, utc=True,
                                        timezone='UTC'), expected)


if __name__ == '__main__':
    unittest.main()
