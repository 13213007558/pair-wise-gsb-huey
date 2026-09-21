import asyncio
import os
import shutil
import tempfile
import time

from huey.api import FileHuey
from huey.api import MemoryHuey
from huey.api import Result
from huey.api import SqliteHuey
from huey.api import Task
from huey.contrib.asyncio import aget_result
from huey.exceptions import ResultExpired
from huey.exceptions import ResultStoreClosed
from huey.exceptions import TaskException
from huey.tests.base import BaseTestCase
from huey.utils import Error


class Boom(Exception):
    def __init__(self, m=None):
        self._m = m
    def __repr__(self):
        return 'Boom(%s)' % self._m


class TestPipelineRetrySemantics(BaseTestCase):
    """
    A retryable failure on an intermediate pipeline node must not be handed
    off to the downstream callback as if it were a successful result.
    """
    def test_retryable_failure_then_success(self):
        calls = []

        @self.huey.task()
        def head(n):
            calls.append('head')
            return n + 1

        @self.huey.task(retries=1)
        def mid(n):
            calls.append('mid')
            if calls.count('mid') == 1:
                raise Boom('first attempt')
            return n * 10

        @self.huey.task()
        def tail(n):
            calls.append('tail')
            return n + 100

        r_head, r_mid, r_tail = self.huey.enqueue(head.s(1).then(mid).then(tail))

        self.assertEqual(self.execute_next(), 2)  # head succeeds.
        self.assertTrue(self.execute_next() is None)  # mid fails, retryable.

        # The failure was not treated as a result: the chain did not advance
        # and only the retry is queued.
        self.assertEqual(len(self.huey), 1)
        self.assertEqual(calls, ['head', 'mid'])

        # The intermediate error is visible on mid's result key, while the
        # downstream node has no result at all.
        self.assertTrue(isinstance(r_mid.get_raw_result(preserve=True), Error))
        self.assertTrue(r_tail.get_raw_result() is None)
        r_mid.reset()  # Reset the cached intermediate error.

        self.assertEqual(self.execute_next(), 20)  # mid retry succeeds.
        self.assertEqual(self.execute_next(), 120)  # tail gets success value.
        self.assertEqual(calls, ['head', 'mid', 'mid', 'tail'])
        self.assertEqual(len(self.huey), 0)

        # Every node has its own result key and the pipeline resolves to the
        # final value.
        self.assertEqual(set(self.huey.storage.result_items()),
                         {r_head.id, r_mid.id, r_tail.id})
        self.assertEqual([r() for r in (r_head, r_mid, r_tail)], [2, 20, 120])

    def test_retry_exhausted_pipeline_halts(self):
        calls = []

        @self.huey.task()
        def head(n):
            calls.append('head')
            return n + 1

        @self.huey.task(retries=1)
        def mid(n):
            calls.append('mid')
            raise Boom('always fails')

        @self.huey.task()
        def tail(n):
            calls.append('tail')
            return n + 100

        r_head, r_mid, r_tail = self.huey.enqueue(head.s(1).then(mid).then(tail))

        self.assertEqual(self.execute_next(), 2)
        self.assertTrue(self.execute_next() is None)  # 1st attempt, retries.
        self.assertEqual(len(self.huey), 1)  # Just the retry.
        self.assertTrue(self.execute_next() is None)  # Final attempt.

        # The chain never advanced: tail was not executed or enqueued.
        self.assertEqual(calls, ['head', 'mid', 'mid'])
        self.assertEqual(len(self.huey), 0)

        # The failed value is surfaced as an error on both the failed node
        # and the downstream node -- never as a successful result.
        self.assertRaises(TaskException, r_mid.get)
        self.assertTrue(isinstance(r_tail.get_raw_result(), Error))
        exc = self.trap_exception(r_tail.get)
        self.assertEqual(exc.metadata['task_id'], r_mid.id)
        self.assertEqual(r_head(), 2)

    def test_callback_raises_retryable_then_succeeds(self):
        calls = []

        @self.huey.task()
        def head(n):
            calls.append('head')
            return n + 1

        @self.huey.task(retries=1)
        def tail(n):
            calls.append('tail')
            if calls.count('tail') == 1:
                raise Boom('callback blew up')
            return n * 2

        r_head, r_tail = self.huey.enqueue(head.s(1).then(tail))
        self.assertEqual(self.execute_next(), 2)
        self.assertTrue(self.execute_next() is None)  # tail fails, retries.
        self.assertEqual(self.execute_next(), 4)  # tail retry succeeds.

        # Upstream result was not disturbed by the callback's failure.
        self.assertEqual(calls, ['head', 'tail', 'tail'])
        self.assertEqual([r_head(), r_tail()], [2, 4])

    def test_error_callback_raises(self):
        calls = []

        @self.huey.task()
        def task_a():
            calls.append('task_a')
            raise Boom('task failed')

        @self.huey.task()
        def handler(exc):
            calls.append('handler')
            raise Boom('handler failed')

        r = self.huey.enqueue(task_a.s().error(handler))
        self.assertTrue(self.execute_next() is None)  # task_a fails.
        self.assertEqual(len(self.huey), 1)  # Error handler enqueued.
        self.assertTrue(self.execute_next() is None)  # Handler also raises.

        # Both failures are recorded under their own result keys and the
        # queue is drained.
        self.assertEqual(calls, ['task_a', 'handler'])
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.huey.result_count(), 2)
        self.assertRaises(TaskException, r.get)


class DuplicateDequeueTests(object):
    """
    Re-delivery of a dequeued message (e.g. the consumer crashed after the
    result was persisted) must not re-execute the task or re-advance the
    pipeline: the chain moves forward exactly once.
    """
    def dequeue_raw(self):
        data = self.huey.storage.dequeue()
        self.assertTrue(data is not None)
        return data

    def test_duplicate_dequeue_advances_chain_once(self):
        calls = []

        @self.huey.task()
        def head(n):
            calls.append('head')
            return n + 1

        @self.huey.task()
        def tail(n):
            calls.append('tail')
            return n * 2

        r_head, r_tail = self.huey.enqueue(head.s(1).then(tail))

        # Simulate a redelivered message: two task instances deserialized
        # from the same raw message bytes.
        data = self.dequeue_raw()
        task_a = self.huey.deserialize_task(data)
        task_b = self.huey.deserialize_task(data)

        self.assertEqual(self.huey.execute(task_a), 2)
        # Result persisted and chain advanced, then the consumer "crashes"
        # and the same message is delivered again.
        self.assertEqual(self.huey.execute(task_b), 2)
        self.assertEqual(calls, ['head'])
        self.assertEqual(len(self.huey), 1)  # tail enqueued exactly once.

        self.assertEqual(self.execute_next(), 4)
        self.assertEqual(calls, ['head', 'tail'])
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(set(self.huey.storage.result_items()),
                         {r_head.id, r_tail.id})
        self.assertEqual([r_head(), r_tail()], [2, 4])

    def test_redelivery_after_terminal_failure(self):
        calls = []

        @self.huey.task()
        def boom():
            calls.append('boom')
            raise Boom('nope')

        @self.huey.task()
        def handler(exc):
            calls.append('handler')
            return -1

        r = self.huey.enqueue(boom.s().error(handler))
        data = self.dequeue_raw()
        task_a = self.huey.deserialize_task(data)
        task_b = self.huey.deserialize_task(data)

        self.assertTrue(self.huey.execute(task_a) is None)
        self.assertEqual(len(self.huey), 1)  # Error handler enqueued.

        # Redelivery after the terminal failure: the stored error is
        # returned and the error handler is not enqueued a second time.
        redelivered = self.huey.execute(task_b)
        self.assertTrue(isinstance(redelivered, Error))
        self.assertEqual(calls, ['boom'])
        self.assertEqual(len(self.huey), 1)

        self.assertEqual(self.execute_next(), -1)
        self.assertEqual(calls, ['boom', 'handler'])
        self.assertRaises(TaskException, r.get)

    def test_redelivery_competing_with_retry(self):
        calls = []

        @self.huey.task(retries=1)
        def flaky(n):
            calls.append('flaky')
            if len(calls) == 1:
                raise Boom('first attempt')
            return n * 10

        r = self.huey.enqueue(flaky.s(2))
        data = self.dequeue_raw()

        # First attempt fails; the intermediate error is stored and the task
        # is requeued for a retry.
        task_a = self.huey.deserialize_task(data)
        self.assertTrue(self.huey.execute(task_a) is None)
        self.assertEqual(len(self.huey), 1)

        # The original message is also redelivered. The stored error is not
        # terminal (retries remained), so the task executes again and
        # succeeds.
        task_b = self.huey.deserialize_task(data)
        self.assertEqual(self.huey.execute(task_b), 20)
        self.assertEqual(calls, ['flaky', 'flaky'])

        # The queued retry is then a no-op: the task already completed.
        self.assertEqual(self.execute_next(), 20)
        self.assertEqual(calls, ['flaky', 'flaky'])
        self.assertEqual(r(), 20)

    def test_redelivery_does_not_reenqueue_failed_callback(self):
        calls = []

        @self.huey.task()
        def head(n):
            calls.append('head')
            return n + 1

        @self.huey.task()
        def tail(n):
            calls.append('tail')
            raise Boom('tail failed')

        r_head, r_tail = self.huey.enqueue(head.s(1).then(tail))
        data = self.dequeue_raw()
        task_a = self.huey.deserialize_task(data)
        self.assertEqual(self.huey.execute(task_a), 2)

        # tail runs and fails terminally.
        self.assertTrue(self.execute_next() is None)
        self.assertEqual(calls, ['head', 'tail'])
        self.assertEqual(len(self.huey), 0)

        # Redelivery of the head message does not re-enqueue the failed tail.
        task_b = self.huey.deserialize_task(data)
        self.assertEqual(self.huey.execute(task_b), 2)
        self.assertEqual(calls, ['head', 'tail'])
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(r_head(), 2)
        self.assertRaises(TaskException, r_tail.get)


class TestDuplicateDequeueMemory(DuplicateDequeueTests, BaseTestCase):
    def get_huey(self):
        return MemoryHuey(utc=False)


class TempDirMixin(object):
    def setUp(self):
        self._tempdir = tempfile.mkdtemp()
        super(TempDirMixin, self).setUp()

    def tearDown(self):
        self.huey.storage.close()
        shutil.rmtree(self._tempdir)
        super(TempDirMixin, self).tearDown()


class TestDuplicateDequeueSqlite(TempDirMixin, DuplicateDequeueTests,
                                 BaseTestCase):
    def get_huey(self):
        return SqliteHuey(filename=os.path.join(self._tempdir, 'huey.db'),
                          utc=False)


class TestDuplicateDequeueFile(TempDirMixin, DuplicateDequeueTests,
                               BaseTestCase):
    def get_huey(self):
        return FileHuey('test-pipeline', path=self._tempdir, utc=False)


class ResultStoreErrorTests(object):
    """
    A closed or expired result store raises a distinguishable error instead
    of reporting the missing result as None.
    """
    def test_closed_store_raises(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        r = task_a(1)
        self.assertEqual(self.execute_next(), 2)
        self.assertEqual(r.get(preserve=True), 2)  # Sanity: result readable.

        self.huey.storage.close()
        r.reset()
        self.assertRaises(ResultStoreClosed, r.get)
        self.assertRaises(ResultStoreClosed, self.huey.get, r.id)
        # A blocking read fails immediately instead of timing out.
        self.assertRaises(ResultStoreClosed, r.get, blocking=True, timeout=5)

    def test_missing_result_is_none(self):
        # A result that simply does not exist is still reported as None.
        res = Result(self.huey, Task(id='does-not-exist'))
        self.assertTrue(res.get() is None)
        self.assertTrue(self.huey.get('does-not-exist') is None)


class TestResultStoreErrorsMemory(ResultStoreErrorTests, BaseTestCase):
    def get_huey(self):
        return MemoryHuey(utc=False)

    def test_expired_result_raises(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        r = task_a(1)
        self.assertEqual(self.execute_next(), 2)

        # Force the stored result to expire.
        self.huey.storage._expires[r.id] = time.monotonic() - 1
        self.assertRaises(ResultExpired, r.get)
        self.assertRaises(ResultExpired, self.huey.get, r.id)

    def test_expired_result_via_ttl(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        r = task_a(1)
        # Write a result with a TTL and let it expire before reading.
        self.huey.put_if_empty(r.id, 'sentinel', ttl=0.01)
        time.sleep(0.02)
        self.assertRaises(ResultExpired, r.get)


class TestResultStoreErrorsSqlite(TempDirMixin, ResultStoreErrorTests,
                                  BaseTestCase):
    def get_huey(self):
        return SqliteHuey(filename=os.path.join(self._tempdir, 'huey.db'),
                          utc=False)


class TestResultStoreErrorsFile(TempDirMixin, ResultStoreErrorTests,
                                BaseTestCase):
    def get_huey(self):
        return FileHuey('test-results', path=self._tempdir, utc=False)


class TestPipelineAsyncStyle(BaseTestCase):
    """
    The same guarantees hold when results are consumed asyncio-style and
    when tasks run in immediate (synchronous) mode.
    """
    def test_pipeline_final_value_async(self):
        calls = []

        @self.huey.task()
        def head(n):
            calls.append('head')
            return n + 1

        @self.huey.task()
        def tail(n):
            calls.append('tail')
            return n * 2

        r_head, r_tail = self.huey.enqueue(head.s(1).then(tail))

        # Redeliver the head message: chain still advances exactly once.
        data = self.huey.storage.dequeue()
        task_a = self.huey.deserialize_task(data)
        task_b = self.huey.deserialize_task(data)
        self.assertEqual(self.huey.execute(task_a), 2)
        self.assertEqual(self.huey.execute(task_b), 2)
        self.assertEqual(self.execute_next(), 4)
        self.assertEqual(calls, ['head', 'tail'])

        async def main():
            return await asyncio.gather(
                aget_result(r_head), aget_result(r_tail))
        self.assertEqual(asyncio.run(main()), [2, 4])

    def test_immediate_mode_pipeline(self):
        self.huey.immediate = True
        calls = []

        @self.huey.task()
        def head(n):
            calls.append('head')
            return n + 1

        @self.huey.task()
        def tail(n):
            calls.append('tail')
            return n * 2

        task = head.s(1).then(tail)
        r_head, r_tail = self.huey.enqueue(task)
        self.assertEqual(calls, ['head', 'tail'])
        self.assertEqual([r_head.get(preserve=True), r_tail.get(preserve=True)],
                         [2, 4])

        # Re-executing the completed head task is a no-op.
        self.assertEqual(self.huey.execute(task), 2)
        self.assertEqual(calls, ['head', 'tail'])
        self.assertEqual([r_head(), r_tail()], [2, 4])
