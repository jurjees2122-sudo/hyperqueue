import os
import tempfile
import time
import unittest
from hyperqueue.models import Job, JobStatus, Priority
from hyperqueue.storage.memory import MemoryStorageBackend
from hyperqueue.storage.sqlite import SQLiteStorageBackend


class TestStorageBackends(unittest.TestCase):

    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
        self.sqlite = SQLiteStorageBackend(db_path=self.temp_db.name)
        self.memory = MemoryStorageBackend()

    def tearDown(self):
        self.sqlite.close()
        self.memory.close()
        if os.path.exists(self.temp_db.name):
            try:
                os.remove(self.temp_db.name)
            except OSError:
                pass

    def _test_basic_crud(self, backend):
        job = Job(task_name="test_task", payload={"key": "val"}, priority=Priority.HIGH)
        backend.save_job(job)

        fetched = backend.get_job(job.id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.task_name, "test_task")
        self.assertEqual(fetched.payload, {"key": "val"})
        self.assertEqual(fetched.priority, Priority.HIGH)
        self.assertEqual(fetched.status, JobStatus.PENDING)

    def test_memory_crud(self):
        self._test_basic_crud(self.memory)

    def test_sqlite_crud(self):
        self._test_basic_crud(self.sqlite)

    def _test_priority_ordering(self, backend):
        low = Job(task_name="low", priority=Priority.LOW)
        high = Job(task_name="high", priority=Priority.HIGH)
        critical = Job(task_name="crit", priority=Priority.CRITICAL)

        backend.save_job(low)
        backend.save_job(high)
        backend.save_job(critical)

        first = backend.acquire_next_job()
        second = backend.acquire_next_job()
        third = backend.acquire_next_job()

        self.assertEqual(first.priority, Priority.CRITICAL)
        self.assertEqual(second.priority, Priority.HIGH)
        self.assertEqual(third.priority, Priority.LOW)

    def test_memory_priority(self):
        self._test_priority_ordering(self.memory)

    def test_sqlite_priority(self):
        self._test_priority_ordering(self.sqlite)

    def _test_delayed_job(self, backend):
        future_job = Job(
            task_name="future",
            priority=Priority.CRITICAL,
            scheduled_at=time.time() + 10.0,
        )
        ready_job = Job(
            task_name="ready",
            priority=Priority.LOW,
            scheduled_at=time.time() - 1.0,
        )

        backend.save_job(future_job)
        backend.save_job(ready_job)

        acquired = backend.acquire_next_job()
        self.assertIsNotNone(acquired)
        self.assertEqual(acquired.task_name, "ready")

    def test_memory_delayed_job(self):
        self._test_delayed_job(self.memory)

    def test_sqlite_delayed_job(self):
        self._test_delayed_job(self.sqlite)


if __name__ == "__main__":
    unittest.main()
