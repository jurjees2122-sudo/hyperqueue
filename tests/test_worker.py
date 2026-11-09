import asyncio
import unittest
from hyperqueue.engine import QueueEngine
from hyperqueue.models import JobStatus, Task
from hyperqueue.storage.memory import MemoryStorageBackend
from hyperqueue.worker import WorkerPool


class TestWorkerPool(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.backend = MemoryStorageBackend()
        self.engine = QueueEngine(backend=self.backend)

    async def asyncTearDown(self):
        self.engine.close()

    async def test_successful_task_execution(self):
        @Task(name="add_numbers")
        def add_numbers(a: int, b: int) -> int:
            return a + b

        job = self.engine.enqueue("add_numbers", {"a": 10, "b": 25})
        pool = WorkerPool(backend=self.backend, concurrency=2, poll_interval=0.05)

        runner = asyncio.create_task(pool.start())
        for _ in range(50):
            j = self.engine.get_job(job.id)
            if j.status == JobStatus.COMPLETED:
                break
            await asyncio.sleep(0.05)

        pool.stop()
        await runner

        completed_job = self.engine.get_job(job.id)
        self.assertEqual(completed_job.status, JobStatus.COMPLETED)
        self.assertEqual(completed_job.result, 35)

    async def test_async_task_execution(self):
        @Task(name="async_multiply")
        async def async_multiply(x: int) -> int:
            await asyncio.sleep(0.01)
            return x * 3

        job = self.engine.enqueue("async_multiply", {"x": 7})
        pool = WorkerPool(backend=self.backend, concurrency=2, poll_interval=0.05)

        runner = asyncio.create_task(pool.start())
        for _ in range(50):
            j = self.engine.get_job(job.id)
            if j.status == JobStatus.COMPLETED:
                break
            await asyncio.sleep(0.05)

        pool.stop()
        await runner

        completed_job = self.engine.get_job(job.id)
        self.assertEqual(completed_job.status, JobStatus.COMPLETED)
        self.assertEqual(completed_job.result, 21)


if __name__ == "__main__":
    unittest.main()
