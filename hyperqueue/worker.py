import asyncio
import logging
import time
from typing import Optional, List
from hyperqueue.models import Job, JobStatus, Task
from hyperqueue.retry import RetryManager
from hyperqueue.storage.base import StorageBackend
from hyperqueue.telemetry import global_metrics

logger = logging.getLogger("hyperqueue.worker")


class WorkerPool:
    """Concurrent worker pool orchestrating job retrieval, execution, and fault tolerance."""

    def __init__(
        self,
        backend: StorageBackend,
        concurrency: int = 4,
        poll_interval: float = 0.5,
    ):
        self.backend = backend
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.retry_manager = RetryManager()
        self._running = False
        self._tasks: List[asyncio.Task] = []

    async def _worker_loop(self, worker_id: int) -> None:
        logger.debug("Worker-%d started", worker_id)
        while self._running:
            try:
                job = self.backend.acquire_next_job()
                if not job:
                    await asyncio.sleep(self.poll_interval)
                    continue

                await self._execute_job(worker_id, job)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Worker-%d unexpected loop error: %s", worker_id, e)
                await asyncio.sleep(self.poll_interval)

    async def _execute_job(self, worker_id: int, job: Job) -> None:
        task_fn = Task.get(job.task_name)
        start_time = time.time()

        if not task_fn:
            err = f"Task '{job.task_name}' is not registered with @Task"
            logger.error(err)
            self.retry_manager.handle_failure(job, RuntimeError(err))
            self.backend.save_job(job)
            global_metrics.record_job_failure(retried=False)
            return

        try:
            logger.info("Worker-%d executing Job %s (%s)", worker_id, job.id, job.task_name)
            if asyncio.iscoroutinefunction(task_fn):
                res = await asyncio.wait_for(task_fn(**job.payload), timeout=job.max_execution_time_sec)
            else:
                loop = asyncio.get_running_loop()
                res = await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: task_fn(**job.payload)),
                    timeout=job.max_execution_time_sec
                )

            duration = time.time() - start_time
            job.status = JobStatus.COMPLETED
            job.result = res
            job.completed_at = time.time()
            self.backend.save_job(job)
            global_metrics.record_job_completion(duration)
            logger.info("Worker-%d completed Job %s in %.3fs", worker_id, job.id, duration)

        except Exception as exc:
            duration = time.time() - start_time
            is_retrying = job.retry_count < job.retry_policy.max_retries
            self.retry_manager.handle_failure(job, exc)
            self.backend.save_job(job)
            global_metrics.record_job_failure(retried=is_retrying)
            logger.warning("Worker-%d failed Job %s: %s", worker_id, job.id, exc)

    async def start(self) -> None:
        """Launch worker pool."""
        self._running = True
        logger.info("Starting WorkerPool with %d concurrency...", self.concurrency)
        self._tasks = [
            asyncio.create_task(self._worker_loop(i + 1))
            for i in range(self.concurrency)
        ]
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def stop(self) -> None:
        """Trigger graceful shutdown."""
        logger.info("Stopping WorkerPool...")
        self._running = False
        for task in self._tasks:
            task.cancel()
