"""Complex Multi-Stage Batch Processing Pipeline with HyperQueue.

Demonstrates:
- Task dependency simulation
- Dead-letter queue fault handling and recovery
- High-throughput parallel ingestion
"""

import asyncio
import random
from hyperqueue import Task, HyperQueueClient, WorkerPool, Priority, RetryPolicy
from hyperqueue.storage import MemoryStorageBackend


@Task(name="ingest_raw_events")
async def ingest_raw_events(batch_id: int, event_count: int):
    await asyncio.sleep(0.05)
    return {"batch_id": batch_id, "ingested": event_count}


@Task(name="enrich_metadata")
async def enrich_metadata(batch_id: int):
    # Simulate occasional transient network failure
    if random.random() < 0.3:
        raise ConnectionResetError("Remote schema registry temporarily unreachable")
    return {"batch_id": batch_id, "enriched": True}


async def main():
    backend = MemoryStorageBackend()
    client = HyperQueueClient()
    client.engine.backend = backend

    print("=== HyperQueue Batch Ingestion Pipeline ===")
    policy = RetryPolicy(max_retries=3, initial_delay=0.1, backoff_multiplier=1.5, jitter=True)

    # Enqueue ingestion jobs
    for i in range(10):
        client.submit("ingest_raw_events", {"batch_id": i, "event_count": 500}, priority=Priority.NORMAL)
        client.submit("enrich_metadata", {"batch_id": i}, priority=Priority.HIGH, retry_policy=policy)

    print(f"Enqueued 20 multi-stage pipeline tasks.")

    pool = WorkerPool(backend=backend, concurrency=4, poll_interval=0.02)
    runner = asyncio.create_task(pool.start())

    # Let pipeline execute
    await asyncio.sleep(1.5)
    pool.stop()
    await runner

    print("\n--- Pipeline Execution Summary ---")
    stats = client.engine.stats()
    for status, count in stats.items():
        print(f"  {status:<14}: {count}")

    # Inspect Dead-Letter queue if any
    dlq_jobs = client.engine.list_jobs(status=None)
    failed = [j for j in dlq_jobs if j.status.value in ("FAILED", "DEAD_LETTER")]
    if failed:
        print(f"\n{len(failed)} tasks moved to DEAD_LETTER queue. Ready for replay.")
    else:
        print("\nAll pipeline tasks processed successfully without unrecoverable errors.")


if __name__ == "__main__":
    asyncio.run(main())
