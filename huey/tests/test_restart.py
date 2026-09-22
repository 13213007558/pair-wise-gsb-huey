"""
Restart-window semantics tests.

Huey's "ack" is the atomic pop performed by dequeue(), which happens *before*
a task runs. These tests pin down the semantics of the three windows around a
worker restart, using controllable barriers (threading.Event), signal and
exception injection, and a persistent (sqlite) storage that is re-opened by a
fresh Huey instance to prove what state actually survived:

1. Side-effect done, result not yet written: the task is gone from the queue,
   no result is stored, the task is not requeued and not re-executed.
2. Result written, post-result bookkeeping incomplete: the result survives,
   the task is neither requeued nor re-executed.
3. Requeue persisted, old worker still exiting: exactly one copy of the task
   is back on the queue with retries decremented, shutdown hooks run after
   the requeue, and a fresh worker replays the task exactly once.
"""
import os
import signal
import tempfile
import threading
import time
import unittest

from huey.api import SqliteHuey
from huey.signals import SIGNAL_COMPLETE
from huey.signals import SIGNAL_ERROR
from huey.signals import SIGNAL_INTERRUPTED
from huey.signals import SIGNAL_RETRYING
from huey.tests.base import BaseTestCase
from huey.utils import Error


def wait_for(predicate, timeout=10):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if predicate():
            return True
        time.sleep(0.005)
    return False


class TestRestartWindows(BaseTestCase):
    def setUp(self):
        fd, self.db_file = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.unlink(self.db_file)  # Let sqlite create it cleanly.
        super(TestRestartWindows, self).setUp()

    def tearDown(self):
        self.huey.storage.close()
        for suffix in ('', '-wal', '-shm'):
            try:
                os.unlink(self.db_file + suffix)
            except OSError:
                pass
        super(TestRestartWindows, self).tearDown()

    def get_huey(self):
        return SqliteHuey(filename=self.db_file, utc=False)

    def fresh_view(self):
        # A brand-new Huey instance over the same database file: this is what
        # a restarted process would actually see.
        return SqliteHuey(filename=self.db_file, utc=False)

    def start_consumer(self, consumer):
        consumer.scheduler.start()
        for _, worker_t in consumer.worker_threads:
            worker_t.start()

    def alive_workers(self, consumer):
        return sum(t.is_alive() for _, t in consumer.worker_threads)

    def test_window1_side_effect_done_result_not_written(self):
        entered = threading.Event()
        release = threading.Event()
        executions = []
        signals = []
        hooks = []

        @self.huey.task()
        def win1():
            executions.append('run')
            entered.set()
            release.wait(10)
            # Simulate a hard kill landing after the side-effect but before
            # the result is written.
            raise KeyboardInterrupt

        @self.huey.signal()
        def capture(sig, task, *args, **kwargs):
            signals.append(sig)

        @self.huey.on_shutdown()
        def shutdown_hook():
            hooks.append('shutdown')

        result = win1()
        consumer = self.consumer(workers=1)
        self.start_consumer(consumer)
        self.assertTrue(entered.wait(10))

        # The ack (dequeue) already happened: the in-flight task is no
        # longer on the queue.
        self.assertEqual(len(self.huey), 0)

        # Restart signal arrives inside the window; the worker exits after
        # the current loop iteration.
        consumer.stop_flag.set()
        release.set()
        consumer.stop(graceful=True)
        self.assertEqual(self.alive_workers(consumer), 0)

        # Exactly one execution, no result, nothing requeued.
        self.assertEqual(executions, ['run'])
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.huey.result_count(), 0)
        self.assertTrue(self.huey.get(result.id, peek=True) is None)
        self.assertIn(SIGNAL_INTERRUPTED, signals)
        self.assertNotIn(SIGNAL_COMPLETE, signals)
        self.assertNotIn(SIGNAL_ERROR, signals)
        self.assertEqual(hooks, ['shutdown'])

        # The persistent view agrees: nothing to replay, no result.
        huey2 = self.fresh_view()
        try:
            self.assertEqual(len(huey2), 0)
            self.assertTrue(huey2.get(result.id, peek=True) is None)
        finally:
            huey2.storage.close()

        # A restarted consumer does not re-execute the task.
        consumer2 = self.consumer(workers=1)
        self.start_consumer(consumer2)
        time.sleep(0.1)
        consumer2.stop(graceful=True)
        self.assertEqual(executions, ['run'])

    def test_window2_result_written_bookkeeping_incomplete(self):
        executions = []
        signals = []
        hook_calls = []

        @self.huey.task()
        def win2():
            executions.append('run')
            return 42

        @self.huey.signal()
        def capture(sig, task, *args, **kwargs):
            signals.append(sig)

        @self.huey.post_execute()
        def crash_after_result(task, value, exc):
            # Injected failure in the window after the result is persisted
            # but before execution bookkeeping has finished.
            hook_calls.append('post')
            raise RuntimeError('simulated crash after result write')

        result = win2()
        consumer = self.consumer(workers=1)
        self.start_consumer(consumer)
        self.assertTrue(wait_for(lambda: len(hook_calls) == 1))
        consumer.stop(graceful=True)
        self.assertEqual(self.alive_workers(consumer), 0)

        # The hook crash is contained: the result survives, the task is not
        # requeued or re-executed, and the completion signal still fired.
        self.assertEqual(executions, ['run'])
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.huey.result_count(), 1)
        self.assertEqual(self.huey.get(result.id, peek=True), 42)
        self.assertIn(SIGNAL_COMPLETE, signals)
        self.assertNotIn(SIGNAL_RETRYING, signals)

        huey2 = self.fresh_view()
        try:
            self.assertEqual(huey2.get(result.id, peek=True), 42)
            self.assertEqual(len(huey2), 0)
        finally:
            huey2.storage.close()

    def test_window3_requeue_persisted_old_worker_exits(self):
        entered = threading.Event()
        release = threading.Event()
        executions = []
        events = []

        @self.huey.task(retries=1)
        def win3():
            executions.append('run')
            if len(executions) == 1:
                entered.set()
                release.wait(10)
                raise Exception('boom')
            return 'ok'

        @self.huey.signal(SIGNAL_RETRYING)
        def on_retrying(sig, task, *args, **kwargs):
            events.append('requeue')

        @self.huey.on_shutdown()
        def shutdown_hook():
            events.append('shutdown')

        result = win3()
        consumer = self.consumer(workers=1)
        self.start_consumer(consumer)
        self.assertTrue(entered.wait(10))

        # The stop flag is set while the task is blocked, so the worker
        # requeues the task and then exits without picking it up again.
        consumer.stop_flag.set()
        release.set()
        consumer.stop(graceful=True)
        self.assertEqual(self.alive_workers(consumer), 0)

        # The requeue was persisted before the shutdown hook ran.
        self.assertEqual(events, ['requeue', 'shutdown'])
        self.assertEqual(executions, ['run'])
        self.assertEqual(len(self.huey), 1)

        # With the default store_intermediate_errors=True, the failed
        # attempt's error is already visible under the result key.
        self.assertEqual(self.huey.result_count(), 1)
        self.assertIsInstance(self.huey.get(result.id, peek=True), Error)

        # Exactly one copy of the task, retries decremented.
        items = self.huey.storage.enqueued_items()
        self.assertEqual(len(items), 1)
        requeued = self.huey.deserialize_task(bytes(items[0]))
        self.assertEqual(requeued.id, result.id)
        self.assertEqual(requeued.retries, 0)

        # A freshly-opened instance sees the same persisted requeue.
        huey2 = self.fresh_view()
        try:
            self.assertEqual(len(huey2), 1)
        finally:
            huey2.storage.close()

        # A new worker replays the task exactly once.
        consumer2 = self.consumer(workers=1)
        self.start_consumer(consumer2)
        self.assertTrue(wait_for(lambda: len(executions) == 2))
        self.assertTrue(wait_for(lambda: self.huey.result_count() == 1))
        consumer2.stop(graceful=True)

        self.assertEqual(executions, ['run', 'run'])
        self.assertEqual(len(self.huey), 0)
        # The successful replay overwrites the intermediate error in place.
        self.assertEqual(self.huey.result_count(), 1)
        self.assertEqual(self.huey.get(result.id, peek=True), 'ok')

    def test_startup_shutdown_hook_order_and_worker_count(self):
        events = []
        lock = threading.Lock()

        def record(name):
            with lock:
                events.append((threading.current_thread().name, name))

        self.huey.on_startup('boot1')(lambda: record('boot1'))
        self.huey.on_startup('boot2')(lambda: record('boot2'))
        self.huey.on_shutdown('down1')(lambda: record('down1'))
        self.huey.on_shutdown('down2')(lambda: record('down2'))

        entered = threading.Event()
        release = threading.Event()

        @self.huey.task()
        def win4():
            entered.set()
            release.wait(10)
            record('task-done')
            return 1

        win4()
        consumer = self.consumer(workers=2)
        self.start_consumer(consumer)
        self.assertTrue(entered.wait(10))

        # Both workers are running while the task is in flight.
        self.assertEqual(self.alive_workers(consumer), 2)

        release.set()
        consumer.stop(graceful=True)
        self.assertEqual(self.alive_workers(consumer), 0)

        # Per-worker ordering: startup hooks in registration order before any
        # task work; shutdown hooks in registration order after it.
        for worker in ('Worker-1', 'Worker-2'):
            seq = [name for thread, name in events if thread == worker]
            self.assertEqual(seq[:2], ['boot1', 'boot2'])
            self.assertEqual(seq[-2:], ['down1', 'down2'])

        # Graceful shutdown: the task finished before shutdown hooks ran on
        # the worker that executed it.
        task_worker = next(thread for thread, name in events
                           if name == 'task-done')
        seq = [name for thread, name in events if thread == task_worker]
        self.assertLess(seq.index('task-done'), seq.index('down1'))

    def test_sigint_graceful_shutdown_waits_for_result(self):
        entered = threading.Event()
        release = threading.Event()
        hooks = []

        @self.huey.task()
        def win5():
            entered.set()
            release.wait(10)
            return 'done'

        @self.huey.on_shutdown()
        def shutdown_hook():
            hooks.append('shutdown')

        result = win5()
        consumer = self.consumer(workers=1)

        saved = [(sig, signal.getsignal(sig))
                 for sig in (signal.SIGINT, signal.SIGTERM)]
        if hasattr(signal, 'SIGHUP'):
            saved.append((signal.SIGHUP, signal.getsignal(signal.SIGHUP)))

        def inject_signal():
            # SIGINT lands while the task is in flight; the graceful
            # shutdown must wait for the result to be written.
            entered.wait(10)
            time.sleep(0.05)
            os.kill(os.getpid(), signal.SIGINT)
            time.sleep(0.05)
            release.set()

        try:
            threading.Thread(target=inject_signal).start()
            consumer.run()  # Blocks until the signal is handled.
        finally:
            for sig, handler in saved:
                signal.signal(sig, handler)

        self.assertEqual(self.huey.get(result.id, peek=True), 'done')
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(hooks, ['shutdown'])
        self.assertEqual(self.alive_workers(consumer), 0)
        self.assertFalse(consumer.scheduler.is_alive())

    def test_check_worker_health_replaces_dead_workers(self):
        consumer = self.consumer(workers=2)

        # Unstarted workers count as dead and are replaced.
        self.assertFalse(consumer.check_worker_health())
        self.assertEqual(self.alive_workers(consumer), 2)
        self.assertTrue(consumer.scheduler.is_alive())

        # A second pass finds everything alive and restarts nothing.
        self.assertTrue(consumer.check_worker_health())

        consumer.stop_flag.set()
        self.assertTrue(consumer._join_workers())
        self.assertEqual(self.alive_workers(consumer), 0)
