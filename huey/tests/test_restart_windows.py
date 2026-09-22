"""
Verify the semantics of a worker that exits (e.g. after receiving a restart
signal) at each of the three windows around task bookkeeping:

1. Side-effect executed, result not yet written to storage.
2. Result written, but the follow-up "ack" work (pipeline continuation)
   not yet enqueued.
3. Retry requeue written, but the old worker has not finished exiting.

The tests use controllable barriers to freeze the worker at the exact
window boundary, inject KeyboardInterrupt to simulate the restart signal,
and run everything against persistent (sqlite) storage so that state can
be inspected across a full consumer restart. Redelivery is driven by the
queue itself -- never by process/worker names.
"""
import os
import shutil
import tempfile
import threading

from huey import SqliteHuey
from huey.tests.base import BaseTestCase


class WindowBarrier(object):
    """
    Gate installed around a Huey/storage method. When the wrapped method is
    reached, the gate records the hit, blocks the worker thread, and lets
    the test inspect persistent state at that exact point. Once released,
    it optionally raises an injected exception (e.g. KeyboardInterrupt) to
    simulate the restart signal arriving at that instant.
    """
    def __init__(self, inject=None, events=None, label=None):
        self.inject = inject
        self.events = events
        self.label = label
        self.entered = threading.Event()
        self.release = threading.Event()
        self.hits = 0

    def _gate(self):
        self.hits += 1
        if self.events is not None:
            self.events.append(self.label)
        self.entered.set()
        if not self.release.wait(10):
            raise RuntimeError('barrier was not released by the test')
        if self.inject is not None:
            raise self.inject

    def wait_entered(self, timeout=10):
        return self.entered.wait(timeout)

    def open(self):
        self.release.set()

    def wrap_pre(self, fn, when=None):
        # Gate *before* calling through: the wrapped operation never lands.
        def wrapper(*args, **kwargs):
            if when is None or when(*args, **kwargs):
                self._gate()
            return fn(*args, **kwargs)
        return wrapper

    def wrap_post(self, fn, when=None):
        # Gate *after* calling through: the wrapped operation is durable.
        def wrapper(*args, **kwargs):
            ret = fn(*args, **kwargs)
            if when is None or when(*args, **kwargs):
                self._gate()
            return ret
        return wrapper


class TestWorkerRestartWindows(BaseTestCase):
    def get_huey(self):
        self._tmpdir = tempfile.mkdtemp()
        return SqliteHuey('restart-windows',
                          filename=os.path.join(self._tmpdir, 'huey.db'),
                          utc=False)

    def setUp(self):
        super(TestWorkerRestartWindows, self).setUp()
        self.events = []
        self._lock = threading.Lock()
        self._live = [0]
        self._peak = [0]

        @self.huey.on_startup('track-start')
        def _on_start():
            with self._lock:
                self._live[0] += 1
                self._peak[0] = max(self._peak[0], self._live[0])
            self.events.append('startup:%s' % threading.current_thread().name)

        @self.huey.on_shutdown('track-stop')
        def _on_stop():
            self.events.append('shutdown:%s' % threading.current_thread().name)
            with self._lock:
                self._live[0] -= 1

    def tearDown(self):
        self.huey.storage.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)
        super(TestWorkerRestartWindows, self).tearDown()

    def live_workers(self):
        with self._lock:
            return self._live[0]

    def peak_workers(self):
        with self._lock:
            return self._peak[0]

    def start_worker(self, consumer, idx=0):
        worker_impl, worker_t = consumer.worker_threads[idx]
        worker_t.start()
        return worker_impl, worker_t

    def queued_tasks(self):
        return [self.huey.deserialize_task(data)
                for data in self.huey.storage.enqueued_items()]

    def test_window1_side_effect_done_result_not_written(self):
        # Inject the restart signal after the task body ran but before the
        # result is persisted.
        barrier = WindowBarrier(inject=KeyboardInterrupt(),
                                events=self.events, label='gate:put_result')
        self.huey.put_result = barrier.wrap_pre(self.huey.put_result)

        @self.huey.task()
        def task_a(n):
            self.events.append('task-body:%s' % n)
            return n + 1

        res = task_a(1)
        self.assertEqual(len(self.huey), 1)

        consumer = self.consumer(workers=1)
        _, worker_t = self.start_worker(consumer)
        self.assertTrue(barrier.wait_entered())

        # Window 1: side-effect applied, result not yet persisted. The task
        # is already gone from the queue (dequeue is the implicit ack).
        self.assertEqual(self.live_workers(), 1)
        self.assertEqual(len(self.huey), 0)
        self.assertFalse(self.huey.storage.has_data_for_key(res.id))
        self.assertEqual(self.huey.storage.result_store_size(), 0)

        barrier.open()  # Worker receives the "signal" and exits.
        worker_t.join(10)
        self.assertFalse(worker_t.is_alive())

        # Semantics: the task is lost -- no result, no requeue -- and the
        # side-effect ran exactly once. Shutdown hooks run as the worker
        # exits, after the task body.
        self.assertEqual(self.live_workers(), 0)
        self.assertEqual(len(self.huey), 0)
        self.assertFalse(self.huey.storage.has_data_for_key(res.id))
        self.assertEqual(self.events, [
            'startup:Worker-1',
            'task-body:1',
            'gate:put_result',
            'shutdown:Worker-1',
        ])

    def test_window2_result_written_continuation_not_acked(self):
        @self.huey.task()
        def task_a(n):
            self.events.append('task-a:%s' % n)
            return n + 1

        @self.huey.task()
        def task_b(n):
            self.events.append('task-b:%s' % n)
            return n * 10

        task1 = task_a.s(1)
        task2 = task_b.s()
        task1.then(task2)
        self.huey.enqueue(task1)

        # Inject the restart signal after the result is written but before
        # the pipeline continuation (the "ack" follow-up) is enqueued.
        barrier = WindowBarrier(inject=KeyboardInterrupt(),
                                events=self.events, label='gate:enqueue-next')
        self.huey.enqueue = barrier.wrap_pre(
            self.huey.enqueue,
            when=lambda task: getattr(task, 'id', None) == task2.id)

        consumer = self.consumer(workers=1)
        _, worker_t = self.start_worker(consumer)
        self.assertTrue(barrier.wait_entered())

        # Window 2: result key exists, but the continuation is not yet in
        # the queue and task_b has not run.
        self.assertEqual(self.live_workers(), 1)
        self.assertTrue(self.huey.storage.has_data_for_key(task1.id))
        self.assertEqual(len(self.huey), 0)
        self.assertNotIn('task-b:2', self.events)

        barrier.open()  # Worker receives the "signal" and exits.
        worker_t.join(10)
        self.assertFalse(worker_t.is_alive())

        # Semantics: the result survives the restart, but the continuation
        # was never enqueued -- the pipeline stalls and task_b never runs.
        self.assertEqual(self.live_workers(), 0)
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.huey.get(task1.id), 2)  # Result is durable.
        self.assertFalse(self.huey.storage.has_data_for_key(task2.id))
        self.assertEqual(self.events, [
            'startup:Worker-1',
            'task-a:1',
            'gate:enqueue-next',
            'shutdown:Worker-1',
        ])

    def test_window3_requeue_written_old_worker_exits(self):
        self.huey.store_intermediate_errors = False
        attempts = []
        succeeded = threading.Event()

        @self.huey.task(retries=1)
        def task_flaky(n):
            attempts.append(n)
            self.events.append('execute:%d' % len(attempts))
            if len(attempts) == 1:
                raise Exception('boom')
            succeeded.set()
            return n * 10

        # Let the requeue land in storage, then inject the restart signal
        # while the old worker is still on its way out.
        barrier = WindowBarrier(inject=KeyboardInterrupt(),
                                events=self.events, label='gate:requeued')
        self.huey._requeue_task = barrier.wrap_post(self.huey._requeue_task)

        res = task_flaky(3)
        consumer = self.consumer(workers=1)
        _, worker_t = self.start_worker(consumer)
        self.assertTrue(barrier.wait_entered())

        # Window 3: the retry is durably requeued (retry budget consumed),
        # no result yet, and the old worker is still running.
        self.assertEqual(self.live_workers(), 1)
        self.assertEqual(len(self.huey), 1)
        queued = self.queued_tasks()
        self.assertEqual([t.id for t in queued], [res.id])
        self.assertEqual(queued[0].retries, 0)
        self.assertFalse(self.huey.storage.has_data_for_key(res.id))

        barrier.open()  # Old worker exits *after* the requeue landed.
        worker_t.join(10)
        self.assertFalse(worker_t.is_alive())
        self.assertEqual(self.live_workers(), 0)
        self.assertEqual(len(self.huey), 1)  # Requeue survives the exit.

        # Restart: a brand-new consumer over the same persistent storage.
        # The replacement worker is also named "Worker-1" -- redelivery is
        # driven by the queue, not by any process-name dedup.
        consumer2 = self.consumer(workers=1)
        _, worker_t2 = self.start_worker(consumer2)
        self.assertTrue(succeeded.wait(10))
        consumer2.stop_flag.set()
        worker_t2.join(10)
        self.assertFalse(worker_t2.is_alive())

        # Semantics: at-least-once delivery. The side-effect ran once per
        # delivery (twice total), the retry succeeded, and the workers
        # never overlapped.
        self.assertEqual(attempts, [3, 3])
        self.assertEqual(self.peak_workers(), 1)
        self.assertEqual(self.live_workers(), 0)
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(res.get(), 30)
        self.assertEqual(self.events, [
            'startup:Worker-1',
            'execute:1',
            'gate:requeued',
            'shutdown:Worker-1',
            'startup:Worker-1',
            'execute:2',
            'shutdown:Worker-1',
        ])

    def test_graceful_stop_orders_result_before_shutdown_hooks(self):
        started = threading.Event()
        release = threading.Event()

        @self.huey.task()
        def task_block():
            started.set()
            release.wait(10)
            self.events.append('task-done')
            return 42

        res = task_block()
        consumer = self.consumer(workers=1)
        _, worker_t = self.start_worker(consumer)
        self.assertTrue(started.wait(10))

        # The stop-flag (graceful restart signal) is set while the task is
        # mid-flight; the worker must finish the task and persist the
        # result before its shutdown hooks run.
        consumer.stop_flag.set()
        self.assertEqual(self.live_workers(), 1)
        release.set()
        worker_t.join(10)
        self.assertFalse(worker_t.is_alive())

        self.assertEqual(self.live_workers(), 0)
        self.assertEqual(res.get(), 42)
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.events, [
            'startup:Worker-1',
            'task-done',
            'shutdown:Worker-1',
        ])
