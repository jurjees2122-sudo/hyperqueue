import heapq
import threading
import time
from typing import Dict, List, Optional, Tuple
from hyperqueue.models import Job, JobStatus
from hyperqueue.storage.base import StorageBackend


class MemoryStorageBackend(StorageBackend):
    """Thread-safe in-memory storage engine backed by a priority heap."""

    def __init__(self):
        self._lock = threading.RLock()
        self._jobs: Dict[str, Job] = {}
        # Heap elements: (-priority, scheduled_at, sequence, job_id)
        self._heap: List[Tuple[int, float, int, str]] = []
        self._seq = 0

    def save_job(self, job: Job) -> None:
        with self._lock:
            is_new = job.id not in self._jobs
            self._jobs[job.id] = job
            if job.status == JobStatus.PENDING:
                self._seq += 1
                heapq.heappush(
                    self._heap,
                    (-int(job.priority), job.scheduled_at, self._seq, job.id)
                )

    def get_job(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def acquire_next_job(self, lock_duration_sec: float = 60.0) -> Optional[Job]:
        with self._lock:
            now = time.time()
            delayed_items = []
            acquired_job = None

            while self._heap:
                neg_prio, sched_at, seq, jid = heapq.heappop(self._heap)
                job = self._jobs.get(jid)
                if not job or job.status != JobStatus.PENDING:
                    continue

                if sched_at > now:
                    delayed_items.append((neg_prio, sched_at, seq, jid))
                    continue

                # Acquired
                job.status = JobStatus.RUNNING
                job.started_at = now
                acquired_job = job
                break

            for item in delayed_items:
                heapq.heappush(self._heap, item)

            return acquired_job

    def list_jobs(self, status: Optional[JobStatus] = None, limit: int = 100) -> List[Job]:
        with self._lock:
            jobs = list(self._jobs.values())
            if status:
                jobs = [j for j in jobs if j.status == status]
            jobs.sort(key=lambda j: j.created_at, reverse=True)
            return jobs[:limit]

    def count_by_status(self) -> dict:
        with self._lock:
            counts = {s.value: 0 for s in JobStatus}
            for j in self._jobs.values():
                counts[j.status.value] += 1
            return counts

    def close(self) -> None:
        with self._lock:
            self._jobs.clear()
            self._heap.clear()
