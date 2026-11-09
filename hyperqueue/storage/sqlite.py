import json
import sqlite3
import threading
import time
from typing import Dict, List, Optional
from hyperqueue.models import Job, JobStatus, Priority, RetryPolicy
from hyperqueue.storage.base import StorageBackend


class SQLiteStorageBackend(StorageBackend):
    """High-performance SQLite storage backend featuring WAL-mode and atomic job leasing."""

    def __init__(self, db_path: str = "hyperqueue.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA busy_timeout = 10000;")
            self._local.conn = conn
        return self._local.conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    task_name TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    retry_policy TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    scheduled_at REAL NOT NULL,
                    started_at REAL,
                    completed_at REAL,
                    retry_count INTEGER NOT NULL,
                    max_execution_time_sec REAL NOT NULL,
                    last_error TEXT,
                    result TEXT
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_prio ON jobs(status, scheduled_at, priority DESC);")

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        payload = json.loads(row["payload"]) if row["payload"] else {}
        rp_data = json.loads(row["retry_policy"]) if row["retry_policy"] else {}
        result = json.loads(row["result"]) if row["result"] is not None else None

        policy = RetryPolicy(
            max_retries=rp_data.get("max_retries", 3),
            initial_delay=rp_data.get("initial_delay", 1.0),
            max_delay=rp_data.get("max_delay", 60.0),
            backoff_multiplier=rp_data.get("backoff_multiplier", 2.0),
            jitter=rp_data.get("jitter", True),
        )

        return Job(
            id=row["id"],
            task_name=row["task_name"],
            payload=payload,
            priority=Priority(row["priority"]),
            status=JobStatus(row["status"]),
            retry_policy=policy,
            created_at=row["created_at"],
            scheduled_at=row["scheduled_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            retry_count=row["retry_count"],
            max_execution_time_sec=row["max_execution_time_sec"],
            last_error=row["last_error"],
            result=result,
        )

    def save_job(self, job: Job) -> None:
        conn = self._get_conn()
        with conn:
            conn.execute("""
                INSERT INTO jobs (
                    id, task_name, payload, priority, status, retry_policy,
                    created_at, scheduled_at, started_at, completed_at,
                    retry_count, max_execution_time_sec, last_error, result
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    status = excluded.status,
                    scheduled_at = excluded.scheduled_at,
                    started_at = excluded.started_at,
                    completed_at = excluded.completed_at,
                    retry_count = excluded.retry_count,
                    last_error = excluded.last_error,
                    result = excluded.result;
            """, (
                job.id,
                job.task_name,
                json.dumps(job.payload),
                int(job.priority),
                job.status.value,
                json.dumps({
                    "max_retries": job.retry_policy.max_retries,
                    "initial_delay": job.retry_policy.initial_delay,
                    "max_delay": job.retry_policy.max_delay,
                    "backoff_multiplier": job.retry_policy.backoff_multiplier,
                    "jitter": job.retry_policy.jitter,
                }),
                job.created_at,
                job.scheduled_at,
                job.started_at,
                job.completed_at,
                job.retry_count,
                job.max_execution_time_sec,
                job.last_error,
                json.dumps(job.result) if job.result is not None else None,
            ))

    def get_job(self, job_id: str) -> Optional[Job]:
        conn = self._get_conn()
        cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        return self._row_to_job(row) if row else None

    def acquire_next_job(self, lock_duration_sec: float = 60.0) -> Optional[Job]:
        conn = self._get_conn()
        now = time.time()
        with conn:
            cursor = conn.execute("""
                SELECT id FROM jobs
                WHERE status = 'PENDING' AND scheduled_at <= ?
                ORDER BY priority DESC, created_at ASC
                LIMIT 1;
            """, (now,))
            row = cursor.fetchone()
            if not row:
                return None

            job_id = row["id"]
            conn.execute("""
                UPDATE jobs
                SET status = 'RUNNING', started_at = ?
                WHERE id = ? AND status = 'PENDING';
            """, (now, job_id))

        return self.get_job(job_id)

    def list_jobs(self, status: Optional[JobStatus] = None, limit: int = 100) -> List[Job]:
        conn = self._get_conn()
        if status:
            cursor = conn.execute(
                "SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status.value, limit)
            )
        else:
            cursor = conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?",
                (limit,)
            )
        return [self._row_to_job(r) for r in cursor.fetchall()]

    def count_by_status(self) -> Dict[str, int]:
        conn = self._get_conn()
        cursor = conn.execute("SELECT status, COUNT(*) as cnt FROM jobs GROUP BY status")
        counts = {s.value: 0 for s in JobStatus}
        for row in cursor.fetchall():
            counts[row["status"]] = row["cnt"]
        return counts

    def close(self) -> None:
        if hasattr(self._local, "conn") and self._local.conn is not None:
            self._local.conn.close()
            self._local.conn = None
