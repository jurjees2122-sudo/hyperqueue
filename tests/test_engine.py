import unittest
from hyperqueue.engine import QueueEngine
from hyperqueue.models import JobStatus, Priority
from hyperqueue.storage.memory import MemoryStorageBackend


class TestQueueEngine(unittest.TestCase):

    def setUp(self):
        self.backend = MemoryStorageBackend()
        self.engine = QueueEngine(backend=self.backend)

    def tearDown(self):
        self.engine.close()

    def test_enqueue_and_get(self):
        job = self.engine.enqueue("send_email", {"to": "user@example.com"}, priority=Priority.HIGH)
        self.assertIsNotNone(job.id)
        fetched = self.engine.get_job(job.id)
        self.assertEqual(fetched.task_name, "send_email")
        self.assertEqual(fetched.status, JobStatus.PENDING)

    def test_cancel_pending_job(self):
        job = self.engine.enqueue("cancel_me")
        success = self.engine.cancel_job(job.id)
        self.assertTrue(success)

        fetched = self.engine.get_job(job.id)
        self.assertEqual(fetched.status, JobStatus.CANCELLED)

    def test_dlq_replay(self):
        job = self.engine.enqueue("dlq_job")
        job.status = JobStatus.DEAD_LETTER
        self.backend.save_job(job)

        replayed = self.engine.replay_dead_letter(job.id)
        self.assertEqual(replayed, 1)

        fetched = self.engine.get_job(job.id)
        self.assertEqual(fetched.status, JobStatus.PENDING)
        self.assertEqual(fetched.retry_count, 0)


if __name__ == "__main__":
    unittest.main()
