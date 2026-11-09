import time
from typing import Any, Dict, Optional
from hyperqueue.engine import QueueEngine
from hyperqueue.models import Job, JobStatus, Priority, RetryPolicy


class HyperQueueClient:
    """High-level programmatic client for submitting tasks and waiting for results."""

    def __init__(self, engine: Optional[QueueEngine] = None):
        self.engine = engine or QueueEngine()

    def submit(
        self,
        task_name: str,
        payload: Optional[Dict[str, Any]] = None,
        priority: Priority = Priority.NORMAL,
        delay_sec: float = 0.0,
        retry_policy: Optional[RetryPolicy] = None,
        max_execution_time_sec: float = 300.0,
    ) -> Job:
        """Submit a task to the queue."""
        return self.engine.enqueue(
            task_name=task_name,
            payload=payload,
            priority=priority,
            delay_sec=delay_sec,
            retry_policy=retry_policy,
            max_execution_time_sec=max_execution_time_sec,
        )

    def wait_for_job(self, job_id: str, timeout_sec: float = 30.0, poll_interval: float = 0.2) -> Job:
        """Poll until job reaches a terminal state (COMPLETED, DEAD_LETTER, FAILED, CANCELLED)."""
        deadline = time.time() + timeout_sec
        terminal_states = {
            JobStatus.COMPLETED,
            JobStatus.DEAD_LETTER,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        }

        while time.time() < deadline:
            job = self.engine.get_job(job_id)
            if job and job.status in terminal_states:
                return job
            time.sleep(poll_interval)

        raise TimeoutError(f"Job {job_id} did not finish within {timeout_sec} seconds")

    def cancel(self, job_id: str) -> bool:
        """Cancel a pending job."""
        return self.engine.cancel_job(job_id)

    def status(self, job_id: str) -> Optional[Job]:
        """Fetch latest status of a job."""
        return self.engine.get_job(job_id)
