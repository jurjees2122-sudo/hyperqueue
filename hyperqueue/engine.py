import logging
import time
from typing import Any, Dict, List, Optional
from hyperqueue.models import Job, JobStatus, Priority, RetryPolicy, Task
from hyperqueue.storage.base import StorageBackend
from hyperqueue.storage.sqlite import SQLiteStorageBackend

logger = logging.getLogger("hyperqueue.engine")


class QueueEngine:
    """Core orchestrator managing job lifecycles, prioritization, and persistence."""

    def __init__(self, backend: Optional[StorageBackend] = None):
        self.backend = backend or SQLiteStorageBackend()

    def enqueue(
        self,
        task_name: str,
        payload: Optional[Dict[str, Any]] = None,
        priority: Priority = Priority.NORMAL,
        delay_sec: float = 0.0,
        retry_policy: Optional[RetryPolicy] = None,
        max_execution_time_sec: float = 300.0,
    ) -> Job:
        """Enqueues a task for asynchronous execution."""
        now = time.time()
        job = Job(
            task_name=task_name,
            payload=payload or {},
            priority=priority,
            status=JobStatus.PENDING,
            retry_policy=retry_policy or RetryPolicy(),
            created_at=now,
            scheduled_at=now + max(0.0, delay_sec),
            max_execution_time_sec=max_execution_time_sec,
        )
        self.backend.save_job(job)
        logger.info("Enqueued job %s for task '%s' (Priority: %s)", job.id, task_name, priority.name)
        return job

    def get_job(self, job_id: str) -> Optional[Job]:
        """Fetch job details by ID."""
        return self.backend.get_job(job_id)

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending job."""
        job = self.backend.get_job(job_id)
        if not job:
            return False
        if job.status == JobStatus.PENDING:
            job.status = JobStatus.CANCELLED
            job.completed_at = time.time()
            self.backend.save_job(job)
            return True
        return False

    def replay_dead_letter(self, job_id: Optional[str] = None) -> int:
        """Re-enqueues jobs from the DEAD_LETTER queue for another execution attempt."""
        replayed = 0
        if job_id:
            job = self.backend.get_job(job_id)
            if job and job.status == JobStatus.DEAD_LETTER:
                job.status = JobStatus.PENDING
                job.retry_count = 0
                job.scheduled_at = time.time()
                job.last_error = None
                self.backend.save_job(job)
                replayed += 1
        else:
            dlq_jobs = self.backend.list_jobs(status=JobStatus.DEAD_LETTER, limit=500)
            now = time.time()
            for job in dlq_jobs:
                job.status = JobStatus.PENDING
                job.retry_count = 0
                job.scheduled_at = now
                job.last_error = None
                self.backend.save_job(job)
                replayed += 1
        return replayed

    def list_jobs(self, status: Optional[JobStatus] = None, limit: int = 100) -> List[Job]:
        """List active/completed jobs."""
        return self.backend.list_jobs(status=status, limit=limit)

    def stats(self) -> Dict[str, Any]:
        """Return system status counts."""
        return self.backend.count_by_status()

    def close(self) -> None:
        """Close storage backend."""
        self.backend.close()
