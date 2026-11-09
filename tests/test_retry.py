import unittest
from hyperqueue.models import Job, JobStatus, Priority, RetryPolicy
from hyperqueue.retry import RetryManager


class TestRetryPolicy(unittest.TestCase):

    def test_backoff_calculation(self):
        policy = RetryPolicy(initial_delay=1.0, backoff_multiplier=2.0, max_delay=10.0, jitter=False)
        self.assertEqual(policy.calculate_delay(1), 2.0)
        self.assertEqual(policy.calculate_delay(2), 4.0)
        self.assertEqual(policy.calculate_delay(3), 8.0)
        self.assertEqual(policy.calculate_delay(4), 10.0)  # Capped at max_delay

    def test_dead_letter_routing(self):
        manager = RetryManager()
        job = Job(
            task_name="failing_task",
            retry_policy=RetryPolicy(max_retries=2, initial_delay=0.1, jitter=False)
        )

        # Attempt 1 -> retry
        manager.handle_failure(job, RuntimeError("Err 1"))
        self.assertEqual(job.retry_count, 1)
        self.assertEqual(job.status, JobStatus.PENDING)

        # Attempt 2 -> retry
        manager.handle_failure(job, RuntimeError("Err 2"))
        self.assertEqual(job.retry_count, 2)
        self.assertEqual(job.status, JobStatus.PENDING)

        # Attempt 3 -> exceeded max_retries (2) -> DEAD_LETTER
        manager.handle_failure(job, RuntimeError("Err 3"))
        self.assertEqual(job.retry_count, 3)
        self.assertEqual(job.status, JobStatus.DEAD_LETTER)
        self.assertIn("Err 3", job.last_error)


if __name__ == "__main__":
    unittest.main()
