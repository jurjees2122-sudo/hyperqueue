import logging
import time
from typing import Optional, Type
from hyperqueue.models import Job, JobStatus, RetryPolicy

logger = logging.getLogger("hyperqueue.retry")


class RetryManager:
    """Manages job failures, backoff delays, and dead-letter routing."""

    def __init__(self, dlq_retries: int = 3):
        self.default_max_retries = dlq_retries

    def handle_failure(self, job: Job, error: Exception) -> Job:
        """Processes a job execution failure. Either reschedules with backoff or moves to DEAD_LETTER."""
        job.retry_count += 1
        job.last_error = f"{type(error).__name__}: {str(error)}"

        policy: RetryPolicy = job.retry_policy

        if job.retry_count <= policy.max_retries:
            delay = policy.calculate_delay(job.retry_count)
            job.status = JobStatus.PENDING
            job.scheduled_at = time.time() + delay
            logger.warning(
                "Job %s failed (attempt %d/%d). Rescheduling in %.2fs. Error: %s",
                job.id, job.retry_count, policy.max_retries, delay, job.last_error
            )
        else:
            job.status = JobStatus.DEAD_LETTER
            job.completed_at = time.time()
            logger.error(
                "Job %s exceeded max retries (%d). Routed to DEAD_LETTER queue. Final error: %s",
                job.id, policy.max_retries, job.last_error
            )

        return job
