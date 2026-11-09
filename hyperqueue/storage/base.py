from abc import ABC, abstractmethod
from typing import List, Optional
from hyperqueue.models import Job, JobStatus


class StorageBackend(ABC):
    """Abstract interface defining required storage operations for HyperQueue."""

    @abstractmethod
    def save_job(self, job: Job) -> None:
        """Persist or update a job record."""
        pass

    @abstractmethod
    def get_job(self, job_id: str) -> Optional[Job]:
        """Retrieve a job by its ID."""
        pass

    @abstractmethod
    def acquire_next_job(self, lock_duration_sec: float = 60.0) -> Optional[Job]:
        """Atomically fetch and lock the highest-priority scheduled pending job."""
        pass

    @abstractmethod
    def list_jobs(self, status: Optional[JobStatus] = None, limit: int = 100) -> List[Job]:
        """List jobs optionally filtered by status."""
        pass

    @abstractmethod
    def count_by_status(self) -> dict:
        """Return counts of jobs by status."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Release underlying connections and resources."""
        pass
