import asyncio

from huey.api import Result
from huey.api import Task
from huey.contrib.asyncio import aget_result
from huey.contrib.asyncio import aget_result_group
from huey.exceptions import RetryTask
from huey.exceptions import ResultTimeout
from huey.exceptions import TaskException
from huey.tests.base import BaseTestCase


class TestAsyncioHelpers(BaseTestCase):
    def setUp(self):
        super(TestAsyncioHelpers, self).setUp()
        self.huey.immediate = True

    def test_aget_result(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        async def main():
            return await aget_result(task_a(1))
        self.assertEqual(asyncio.run(main()), 2)

    def test_aget_result_error(self):
        @self.huey.task()
        def task_e():
            raise ValueError('uh-oh')

        async def main():
            return await aget_result(task_e())
        self.assertRaises(TaskException, asyncio.run, main())

    def test_aget_result_timeout(self):
        res = Result(self.huey, Task(id='missing'))

        async def main():
            return await aget_result(res, timeout=0.05)
        self.assertRaises(ResultTimeout, asyncio.run, main())

    def test_aget_result_group(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        async def main():
            return await aget_result_group(task_a.map([1, 2, 3]))
        self.assertEqual(asyncio.run(main()), [2, 3, 4])

    def test_aget_pipeline_after_retry_success(self):
        attempts = [0]

        @self.huey.task(retries=1)
        def first():
            attempts[0] += 1
            if attempts[0] == 1:
                raise RetryTask()
            return 2

        @self.huey.task()
        def second(value):
            return value + 1

        results = self.huey.enqueue(first.s().then(second))

        async def main():
            return await aget_result_group(results)

        self.assertEqual(asyncio.run(main()), [2, 3])
        self.assertEqual(attempts, [2])

    def test_aget_pipeline_after_retry_failure(self):
        attempts = [0]

        @self.huey.task(retries=1)
        def first():
            attempts[0] += 1
            raise ValueError('failed')

        @self.huey.task()
        def second(value):
            return value

        results = self.huey.enqueue(first.s().then(second))

        async def main():
            return await aget_result_group(results)

        self.assertRaises(TaskException, lambda: asyncio.run(main()))
        self.assertEqual(attempts, [2])
