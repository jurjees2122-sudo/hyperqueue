"""HyperQueue Quickstart Example

Demonstrates:
1. Defining asynchronous tasks using @Task
2. Enqueuing tasks with custom priorities
3. Running a local worker pool to process tasks
"""

import asyncio
import time
from hyperqueue import Task, HyperQueueClient, WorkerPool, Priority
from hyperqueue.storage import SQLiteStorageBackend


# 1. Define tasks
@Task(name="send_notification")
def send_notification(user_id: str, message: str) -> dict:
    print(f"--> Sending notification to {user_id}: '{message}'")
    time.sleep(0.1)  # Simulate network I/O
    return {"status": "delivered", "user_id": user_id}


@Task(name="generate_report")
async def generate_report(report_id: str, records: int) -> dict:
    print(f"--> Generating analytical report #{report_id} with {records} items...")
    await asyncio.sleep(0.2)  # Non-blocking async processing
    return {"report_id": report_id, "items_processed": records, "status": "ready"}


async def main():
    backend = SQLiteStorageBackend("quickstart.db")
    client = HyperQueueClient()
    client.engine.backend = backend

    print("Submitting tasks to HyperQueue...")

    # Submit low and high priority jobs
    client.submit("send_notification", {"user_id": "usr_99", "message": "Low prio update"}, priority=Priority.LOW)
    urgent_job = client.submit("send_notification", {"user_id": "usr_01", "message": "Critical Alert!"}, priority=Priority.CRITICAL)
    report_job = client.submit("generate_report", {"report_id": "REP-2026", "records": 5000}, priority=Priority.HIGH)

    print(f"Urgent Job enqueued: {urgent_job.id}")
    print(f"Report Job enqueued: {report_job.id}")

    # Launch worker pool
    pool = WorkerPool(backend=backend, concurrency=2, poll_interval=0.1)
    worker_task = asyncio.create_task(pool.start())

    # Wait for completion
    await asyncio.sleep(1.0)
    pool.stop()
    await worker_task

    # Inspect results
    res_urgent = client.status(urgent_job.id)
    res_report = client.status(report_job.id)

    print(f"\nResults:")
    print(f"Urgent Job Result: {res_urgent.result}")
    print(f"Report Job Result: {res_report.result}")
    print(f"\nAggregate Stats: {client.engine.stats()}")


if __name__ == "__main__":
    asyncio.run(main())
