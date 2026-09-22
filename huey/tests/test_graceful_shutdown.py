import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from huey import signals as S
from huey.api import MemoryHuey
from huey.api import SqliteHuey
from huey.consumer import Consumer
from huey.consumer_options import ConsumerConfig
from huey.exceptions import ConfigurationError
from huey.exceptions import RetryTask
from huey.tests.base import BaseTestCase
from huey.utils import Error


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))


class TestUnackedTaskTracking(BaseTestCase):
    """
    Unit tests for the dequeue/ack/release semantics (Memory backend).
    """
    def get_huey(self):
        return MemoryHuey(utc=False)

    def test_release_unacked_returns_task_to_queue(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        r1 = task_a(1)
        r2 = task_a(2)
        self.assertEqual(len(self.huey), 2)

        task = self.huey.dequeue()
        self.assertEqual(task.id, r1.id)
        self.assertEqual(len(self.huey), 1)

        released = self.huey.release_unacked_tasks()
        self.assertEqual([t.id for t in released], [task.id])
        self.assertEqual(len(self.huey), 2)

        # The released task is returned to the queue (behind any tasks that
        # were already enqueued) and can be executed normally.
        next_task = self.huey.dequeue()
        self.assertEqual(next_task.id, r2.id)
        self.assertEqual(self.huey.execute(next_task), 3)
        released_task = self.huey.dequeue()
        self.assertEqual(released_task.id, task.id)
        self.assertEqual(self.huey.execute(released_task), 2)
        self.assertEqual(self.huey.release_unacked_tasks(), [])
        self.assertEqual(len(self.huey), 0)

    def test_executed_task_is_acknowledged(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        task_a(1)
        task = self.huey.dequeue()
        self.assertEqual(self.huey.execute(task), 2)

        # The task was acknowledged, so it is not released (re-queued).
        self.assertEqual(self.huey.release_unacked_tasks(), [])
        self.assertEqual(len(self.huey), 0)

    def test_retried_task_is_not_requeued_twice(self):
        @self.huey.task(retries=1)
        def task_a():
            raise RetryTask()

        task_a()
        task = self.huey.dequeue()
        self.huey.execute(task)

        # The task was re-queued for retry exactly once, and acknowledged --
        # releasing unacked tasks must not re-queue it a second time.
        self.assertEqual(len(self.huey), 1)
        self.assertEqual(self.huey.release_unacked_tasks(), [])
        self.assertEqual(len(self.huey), 1)

    def test_failed_task_is_acknowledged(self):
        @self.huey.task()
        def task_a():
            raise Exception('uh-oh')

        task_a()
        task = self.huey.dequeue()
        self.huey.execute(task)

        # Error was stored and the task acknowledged.
        self.assertTrue(isinstance(self.huey.get(task.id, peek=True), Error))
        self.assertEqual(self.huey.release_unacked_tasks(), [])
        self.assertEqual(len(self.huey), 0)

    def test_interrupted_task_is_released(self):
        @self.huey.task()
        def task_a():
            raise KeyboardInterrupt()

        interrupted = []

        @self.huey.signal(S.SIGNAL_INTERRUPTED)
        def on_interrupted(signal, task, *args):
            interrupted.append(task.id)

        task_a()
        task = self.huey.dequeue()
        self.assertTrue(self.huey.execute(task) is None)
        self.assertEqual(interrupted, [task.id])

        # The interrupted task was not acknowledged, and is released back to
        # the queue. The INTERRUPTED signal is not emitted a second time.
        released = self.huey.release_unacked_tasks()
        self.assertEqual([t.id for t in released], [task.id])
        self.assertEqual(len(self.huey), 1)
        self.assertEqual(interrupted, [task.id])

    def test_disposing_task_is_not_released(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        task_a(1)
        task = self.huey.dequeue()

        # Simulate a task whose outcome is being finalized (result store,
        # retry re-queue, callbacks) when the drain occurs: it must not be
        # both acknowledged and re-queued.
        self.huey._mark_task_disposing(task)
        self.assertEqual(self.huey.release_unacked_tasks(), [])
        self.assertEqual(len(self.huey), 0)

        self.huey._ack_task(task)
        self.assertEqual(self.huey.release_unacked_tasks(), [])

    def test_revoked_task_is_acknowledged(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        r = task_a(1)
        r.revoke()
        task = self.huey.dequeue()
        self.assertTrue(self.huey.execute(task) is None)
        self.assertEqual(self.huey.release_unacked_tasks(), [])
        self.assertEqual(len(self.huey), 0)

    def test_scheduled_task_is_acknowledged(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        # Enqueue a task whose ETA is in the future; execute() moves it to
        # the schedule, which constitutes an acknowledgement.
        task_a.schedule((1,), delay=60)
        task = self.huey.dequeue()
        self.assertTrue(self.huey.execute(task) is None)
        self.assertEqual(self.huey.scheduled_count(), 1)
        self.assertEqual(self.huey.release_unacked_tasks(), [])
        self.assertEqual(len(self.huey), 0)


class TestConsumerDrain(BaseTestCase):
    """
    Consumer-level tests for the drain timeout (thread workers, Memory).
    """
    def get_huey(self):
        return MemoryHuey(utc=False)

    def test_drain_timeout_config(self):
        cfg = ConsumerConfig(drain_timeout=5.0)
        cfg.validate()
        consumer = self.huey.create_consumer(**cfg.values)
        self.assertEqual(consumer.drain_timeout, 5.0)

        cfg = ConsumerConfig()
        cfg.validate()
        consumer = self.huey.create_consumer(**cfg.values)
        self.assertTrue(consumer.drain_timeout is None)

        self.assertRaises(ValueError,
                          ConsumerConfig(drain_timeout=-1).validate)
        self.assertRaises(ConfigurationError, Consumer, self.huey,
                          drain_timeout=-1)

    def test_drain_waits_for_inflight_task(self):
        started = threading.Event()
        proceed = threading.Event()

        @self.huey.task()
        def task_a():
            started.set()
            proceed.wait(10)
            return 'done'

        consumer = self.consumer(workers=1, drain_timeout=5)
        consumer.start()
        try:
            result = task_a()
            self.assertTrue(started.wait(5))

            stopper = threading.Thread(target=consumer.stop, args=(True,))
            stopper.start()
            time.sleep(0.5)
            # The consumer is draining: it stops dequeueing but waits for the
            # in-flight task instead of exiting immediately.
            self.assertTrue(stopper.is_alive())
            proceed.set()
            stopper.join(10)
            self.assertFalse(stopper.is_alive())

            # The task finished and its result was persisted.
            self.assertEqual(result.get(blocking=True, timeout=2), 'done')
        finally:
            proceed.set()
            consumer.stop(graceful=True)

        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.huey.release_unacked_tasks(), [])

    def test_drain_timeout_releases_unacked(self):
        started = threading.Event()
        proceed = threading.Event()

        @self.huey.task()
        def task_a():
            started.set()
            proceed.wait(10)

        consumer = self.consumer(workers=1, drain_timeout=0.3)
        consumer.start()
        try:
            task_a()
            self.assertTrue(started.wait(5))

            start_ts = time.time()
            consumer.stop(graceful=True)
            self.assertTrue(time.time() - start_ts < 5)

            # The task did not finish in time and was released back to the
            # queue (and is not lost).
            self.assertEqual(len(self.huey), 1)
        finally:
            proceed.set()
            consumer.stop(graceful=True)

    def test_no_release_without_drain_timeout(self):
        @self.huey.task()
        def task_a():
            pass

        task_a()
        self.huey.dequeue()

        # Legacy behavior is preserved when no drain timeout is configured:
        # stopping does not release the unacknowledged task.
        consumer = self.consumer(workers=1)
        consumer.stop()
        self.assertEqual(len(self.huey), 0)


class TestSqliteClaimSemantics(BaseTestCase):
    """
    Storage-level lease/ack semantics for the SQLite backend.
    """
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='.huey-claim-', dir=REPO_ROOT)
        self.addCleanup(shutil.rmtree, self.tempdir, True)
        self.db = os.path.join(self.tempdir, 'test.db')
        super(TestSqliteClaimSemantics, self).setUp()

    def get_huey(self):
        return SqliteHuey('testq', filename=self.db, utc=False)

    def test_dequeue_and_ack(self):
        storage = self.huey.storage
        storage.enqueue(b't1')
        self.assertEqual(storage.dequeue(), b't1')
        self.assertEqual(storage.queue_size(), 0)
        self.assertEqual(storage.unacked_owners(), [storage.owner])
        storage.ack(b't1')
        self.assertEqual(storage.unacked_owners(), [])

    def test_release_task(self):
        storage = self.huey.storage
        storage.enqueue(b't1')
        storage.dequeue()
        storage.release_task(b't1')
        self.assertEqual(storage.queue_size(), 1)
        self.assertEqual(storage.unacked_owners(), [])
        self.assertEqual(storage.dequeue(), b't1')

    def test_reclaim_unacked(self):
        storage = self.huey.storage
        storage.enqueue(b't1')
        storage.enqueue(b't2')
        storage.dequeue()
        self.assertEqual(storage.reclaim_unacked(['no-such-owner']), 0)
        self.assertEqual(storage.queue_size(), 1)
        self.assertEqual(storage.reclaim_unacked([storage.owner]), 1)
        self.assertEqual(storage.queue_size(), 2)
        self.assertEqual(storage.unacked_owners(), [])

    def test_execute_acknowledges_task(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        task_a(1)
        task = self.huey.dequeue()
        storage = self.huey.storage
        self.assertEqual(storage.unacked_owners(), [storage.owner])
        self.assertEqual(self.huey.execute(task), 2)
        self.assertEqual(storage.unacked_owners(), [])
        self.assertEqual(self.huey.release_unacked_tasks(), [])

    def test_queue_isolation(self):
        other = SqliteHuey('otherq', filename=self.db, utc=False)
        storage_a = self.huey.storage
        storage_b = other.storage
        storage_a.enqueue(b'a')
        storage_b.enqueue(b'b')

        storage_a.dequeue()
        self.assertEqual(storage_b.queue_size(), 1)
        self.assertEqual(storage_b.unacked_owners(), [])

        # Reclaiming queue A's claims does not touch queue B's tasks.
        self.assertEqual(storage_a.reclaim_unacked([storage_a.owner]), 1)
        self.assertEqual(storage_b.queue_size(), 1)
        self.assertEqual(storage_b.dequeue(), b'b')


CONSUMER_SCRIPT = '''
import os
import sys
import time

sys.path.insert(0, __REPO_ROOT__)
WORKDIR = __WORKDIR__

mode, queue, dbfile, drain = sys.argv[1:5]
worker_type = sys.argv[5] if mode == 'run' else None

if worker_type == 'process':
    import multiprocessing
    try:
        multiprocessing.set_start_method('fork')
    except RuntimeError:
        pass

import logging
logging.basicConfig(
    filename=os.path.join(WORKDIR, 'consumer-%s.log' % queue),
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(name)s %(message)s')

from huey import SqliteHuey

huey = SqliteHuey(queue, filename=dbfile, utc=False)


def mark(name):
    with open(os.path.join(WORKDIR, name), 'a') as fh:
        fh.write('x\\n')


@huey.task()
def wait_task(name):
    mark(name + '.started')
    crash = os.path.join(WORKDIR, name + '.crash')
    proceed = os.path.join(WORKDIR, name + '.proceed')
    deadline = time.time() + 120
    while time.time() < deadline:
        if os.path.exists(crash):
            # Simulate a hard crash in the middle of task execution.
            os._exit(1)
        if os.path.exists(proceed):
            break
        time.sleep(0.05)
    mark(name + '.done')
    return name


if mode == 'enqueue':
    result = wait_task(sys.argv[5])
    print(result.id)
    sys.exit(0)

consumer = huey.create_consumer(
    workers=1,
    periodic=False,
    initial_delay=0.05,
    max_delay=0.05,
    check_worker_health=False,
    worker_type=worker_type or 'thread',
    drain_timeout=None if drain == '-' else float(drain))
consumer.run()
mark('consumer.exited')
'''


class TestGracefulShutdownSubprocess(unittest.TestCase):
    """
    Integration tests that run real consumer processes (SQLite backend) and
    verify graceful-drain behavior via log and marker-file traces.
    """
    queue = 'drainq'

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='.huey-drain-', dir=REPO_ROOT)
        self.addCleanup(shutil.rmtree, self.tempdir, True)
        self.db = os.path.join(self.tempdir, 'test.db')
        self.script = os.path.join(self.tempdir, 'consumer_script.py')
        with open(self.script, 'w') as fh:
            fh.write(CONSUMER_SCRIPT
                     .replace('__REPO_ROOT__', repr(REPO_ROOT))
                     .replace('__WORKDIR__', repr(self.tempdir)))
        self.huey = SqliteHuey(self.queue, filename=self.db, utc=False)
        self.procs = []
        self.addCleanup(self._stop_procs)

    # -- helpers -----------------------------------------------------------

    def _stop_procs(self):
        for proc in self.procs:
            if proc.poll() is None:
                try:
                    proc.kill()
                except OSError:
                    pass
                proc.wait()

    def _start_consumer(self, queue=None, drain='-', worker_type='thread'):
        queue = queue or self.queue
        out = open(os.path.join(self.tempdir, 'out-%s.log' % queue), 'ab')
        proc = subprocess.Popen(
            [sys.executable, self.script, 'run', queue, self.db, str(drain),
             worker_type], stdout=out, stderr=subprocess.STDOUT)
        self.procs.append(proc)
        return proc

    def _enqueue(self, name, queue=None):
        queue = queue or self.queue
        output = subprocess.check_output(
            [sys.executable, self.script, 'enqueue', queue, self.db, '-',
             name])
        return output.decode('utf-8').strip()

    def _path(self, name):
        return os.path.join(self.tempdir, name)

    def _touch(self, name):
        with open(self._path(name), 'a') as fh:
            fh.write('x\n')

    def _count(self, name):
        path = self._path(name)
        if not os.path.exists(path):
            return 0
        with open(path) as fh:
            return len(fh.read().splitlines())

    def _wait_for(self, predicate, message, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        self.fail('timed out waiting for %s\nconsumer logs:\n%s'
                  % (message, self._read_logs()))

    def _wait_marker(self, name, count=1, timeout=30):
        self._wait_for(lambda: self._count(name) >= count,
                       'marker %s (count >= %d)' % (name, count), timeout)

    def _wait_exit(self, proc, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            code = proc.poll()
            if code is not None:
                return code
            time.sleep(0.05)
        self.fail('consumer process did not exit\nconsumer logs:\n%s'
                  % self._read_logs())

    def _read_logs(self):
        chunks = []
        for fname in sorted(os.listdir(self.tempdir)):
            if fname.endswith('.log'):
                with open(os.path.join(self.tempdir, fname)) as fh:
                    chunks.append('== %s ==\n%s' % (fname, fh.read()))
        return '\n'.join(chunks)

    # -- tests -------------------------------------------------------------

    def test_drain_finishes_task_and_stops_dequeueing(self):
        task_id = self._enqueue('t1')
        proc = self._start_consumer(drain=10)
        self._wait_marker('t1.started')

        # Begin the graceful drain, then enqueue another task: the draining
        # consumer must not pick it up.
        proc.send_signal(signal.SIGTERM)
        self._enqueue('t2')
        time.sleep(1.5)
        self.assertEqual(self._count('t2.started'), 0)
        self.assertIsNone(proc.poll())  # Still draining, not dead.

        # Let the in-flight task finish; the consumer then exits cleanly.
        self._touch('t1.proceed')
        self.assertEqual(self._wait_exit(proc), 0)
        self._wait_marker('t1.done')
        self._wait_marker('consumer.exited')

        # The result was persisted and the second task remains enqueued.
        self.assertEqual(self.huey.get(task_id, peek=True), 't1')
        self.assertEqual(len(self.huey), 1)

        logs = self._read_logs()
        self.assertIn('Received SIGTERM', logs)
        self.assertIn('Draining', logs)
        self.assertIn('All workers have stopped', logs)

    def test_drain_timeout_releases_unacked(self):
        task_id = self._enqueue('t1')
        proc = self._start_consumer(drain=1)
        self._wait_marker('t1.started')

        # The task never finishes, so the drain times out and the consumer
        # releases the unacknowledged task back to the queue.
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(self._wait_exit(proc), 0)
        self.assertEqual(self._count('t1.done'), 0)
        self.assertEqual(len(self.huey), 1)
        self.assertIn('Released 1 unacknowledged', self._read_logs())

        # A new consumer picks up the released task and finishes it.
        proc2 = self._start_consumer(drain=10)
        self._wait_marker('t1.started', count=2)
        self._touch('t1.proceed')
        self._wait_marker('t1.done')
        proc2.send_signal(signal.SIGTERM)
        self.assertEqual(self._wait_exit(proc2), 0)
        self.assertEqual(self.huey.get(task_id, peek=True), 't1')

    def test_repeated_sigterm_forces_shutdown(self):
        self._enqueue('t1')
        proc = self._start_consumer(drain=60)
        self._wait_marker('t1.started')

        proc.send_signal(signal.SIGTERM)
        time.sleep(1)
        self.assertIsNone(proc.poll())  # Draining, not dead.

        # A second SIGTERM forces an immediate shutdown (well before the 60s
        # drain timeout would expire).
        proc.send_signal(signal.SIGTERM)
        start = time.time()
        self.assertEqual(self._wait_exit(proc, timeout=30), 0)
        self.assertTrue(time.time() - start < 30)

        logs = self._read_logs()
        self.assertIn('forcing shutdown', logs)
        # The in-flight task was released back to the queue, not lost.
        self.assertEqual(len(self.huey), 1)

    def test_reclaim_after_consumer_crash(self):
        task_id = self._enqueue('t1')
        proc = self._start_consumer(drain=10)
        self._wait_marker('t1.started')

        # Simulate a hard crash: the task calls os._exit() mid-execution, so
        # no cleanup runs and the dequeued task remains claimed.
        self._touch('t1.crash')
        self.assertEqual(self._wait_exit(proc), 1)
        self.assertEqual(len(self.huey), 0)  # Claimed, not in the queue.
        owners = self.huey.storage.unacked_owners()
        self.assertEqual(len(owners), 1)

        # A new consumer recognizes the dead owner's claim at startup and
        # takes over the orphaned task.
        os.unlink(self._path('t1.crash'))
        proc2 = self._start_consumer(drain=10)
        self._wait_marker('t1.started', count=2)
        self.assertIn('Reclaimed 1 unacknowledged', self._read_logs())
        self._touch('t1.proceed')
        self._wait_marker('t1.done')
        proc2.send_signal(signal.SIGTERM)
        self.assertEqual(self._wait_exit(proc2), 0)
        self.assertEqual(self.huey.get(task_id, peek=True), 't1')

    def test_live_consumer_claims_are_not_reclaimed(self):
        self._enqueue('t1')
        proc_a = self._start_consumer(drain=10)
        self._wait_marker('t1.started')

        # A second consumer on the same queue must not steal the claim held
        # by the live consumer.
        proc_b = self._start_consumer(drain=10)
        time.sleep(2)
        self.assertEqual(self._count('t1.started'), 1)

        self._touch('t1.proceed')
        self._wait_marker('t1.done')
        proc_a.send_signal(signal.SIGTERM)
        proc_b.send_signal(signal.SIGTERM)
        self.assertEqual(self._wait_exit(proc_a), 0)
        self.assertEqual(self._wait_exit(proc_b), 0)
        self.assertEqual(self._count('t1.started'), 1)  # Ran exactly once.
        self.assertNotIn('Reclaimed', self._read_logs())

    def test_queue_isolation(self):
        other = SqliteHuey('otherq', filename=self.db, utc=False)
        self._enqueue('t1', queue=self.queue)
        self._enqueue('t2', queue='otherq')

        proc = self._start_consumer(queue=self.queue, drain=1)
        self._wait_marker('t1.started')
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(self._wait_exit(proc), 0)

        # The draining consumer's task was released to its own queue; the
        # other queue's task was never touched.
        self.assertEqual(len(self.huey), 1)
        self.assertEqual(len(other), 1)
        self.assertEqual(self._count('t2.started'), 0)
        self.assertEqual(other.storage.unacked_owners(), [])

    def test_drain_with_process_workers(self):
        task_id = self._enqueue('t1')
        proc = self._start_consumer(drain=10, worker_type='process')
        self._wait_marker('t1.started')

        # The main process coordinates the drain: the worker child process
        # finishes its in-flight task before the consumer exits.
        proc.send_signal(signal.SIGTERM)
        time.sleep(1)
        self.assertIsNone(proc.poll())
        self._touch('t1.proceed')
        self.assertEqual(self._wait_exit(proc), 0)
        self._wait_marker('t1.done')
        self.assertEqual(self.huey.get(task_id, peek=True), 't1')

    def test_process_worker_releases_unacked_on_exit(self):
        self._enqueue('t1')
        proc = self._start_consumer(drain=1, worker_type='process')
        self._wait_marker('t1.started')

        # The task never finishes, so the drain times out. The worker child
        # process is then terminated and releases its unacknowledged task
        # back to the queue before exiting.
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(self._wait_exit(proc), 0)
        self.assertEqual(self._count('t1.done'), 0)
        self._wait_for(lambda: len(self.huey) == 1,
                       'released task to reappear in queue')

        # A new consumer picks up the released task and finishes it.
        proc2 = self._start_consumer(drain=10)
        self._wait_marker('t1.started', count=2)
        self._touch('t1.proceed')
        self._wait_marker('t1.done')
        proc2.send_signal(signal.SIGTERM)
        self.assertEqual(self._wait_exit(proc2), 0)
