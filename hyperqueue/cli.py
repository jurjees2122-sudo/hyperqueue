import asyncio
import json
import logging
import sys
import time
import click

from hyperqueue.engine import QueueEngine
from hyperqueue.models import Priority, JobStatus, Task
from hyperqueue.storage.sqlite import SQLiteStorageBackend
from hyperqueue.worker import WorkerPool
from hyperqueue.telemetry import global_metrics


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose debug logs.")
def main(verbose: bool):
    """HyperQueue CLI: Asynchronous distributed task queue & orchestrator."""
    setup_logging(verbose)


@main.command()
@click.option("--concurrency", "-c", default=4, help="Number of concurrent worker coroutines.")
@click.option("--db", default="hyperqueue.db", help="Path to SQLite database.")
@click.option("--poll-interval", default=0.5, help="Seconds to sleep when idle.")
def worker(concurrency: int, db: str, poll_interval: float):
    """Start the background worker pool to process pending tasks."""
    click.secho(f"Starting HyperQueue Worker Pool (concurrency={concurrency}, db='{db}')...", fg="green", bold=True)
    backend = SQLiteStorageBackend(db_path=db)
    pool = WorkerPool(backend=backend, concurrency=concurrency, poll_interval=poll_interval)

    # Register default built-in tasks if none registered
    if not Task.all_tasks():
        @Task(name="echo")
        def echo_task(message: str = "hello"):
            return f"Echoed: {message}"

        @Task(name="compute")
        def compute_task(n: int = 1000):
            return sum(i * i for i in range(n))

        @Task(name="fail_demo")
        def fail_demo_task():
            raise ValueError("Intentional failure to demonstrate retry & DLQ")

    try:
        asyncio.run(pool.start())
    except KeyboardInterrupt:
        click.secho("\nShutting down workers gracefully...", fg="yellow")
        pool.stop()


@main.command()
@click.argument("task_name")
@click.option("--payload", "-p", default="{}", help="JSON payload dictionary for the task.")
@click.option("--priority", type=click.Choice(["LOW", "NORMAL", "HIGH", "CRITICAL"]), default="NORMAL")
@click.option("--delay", default=0.0, help="Delay in seconds before execution.")
@click.option("--db", default="hyperqueue.db", help="Path to SQLite database.")
def enqueue(task_name: str, payload: str, priority: str, delay: float, db: str):
    """Submit a task to the queue."""
    try:
        data = json.loads(payload)
    except Exception as e:
        click.secho(f"Error parsing payload JSON: {e}", fg="red")
        sys.exit(1)

    backend = SQLiteStorageBackend(db_path=db)
    engine = QueueEngine(backend=backend)
    prio_enum = Priority[priority]

    job = engine.enqueue(task_name=task_name, payload=data, priority=prio_enum, delay_sec=delay)
    click.secho(f"Successfully enqueued job: {job.id}", fg="green")
    click.echo(f"  Task:     {job.task_name}")
    click.echo(f"  Priority: {job.priority.name}")
    click.echo(f"  Status:   {job.status.value}")


@main.command(name="list")
@click.option("--status", type=click.Choice([s.value for s in JobStatus]), default=None)
@click.option("--limit", default=25, help="Max jobs to display.")
@click.option("--db", default="hyperqueue.db", help="Path to SQLite database.")
def list_jobs(status: str, limit: int, db: str):
    """List jobs currently in the queue or completed."""
    backend = SQLiteStorageBackend(db_path=db)
    engine = QueueEngine(backend=backend)
    status_filter = JobStatus(status) if status else None

    jobs = engine.list_jobs(status=status_filter, limit=limit)
    if not jobs:
        click.echo("No matching jobs found.")
        return

    click.secho(f"{'JOB ID':<38} {'TASK':<18} {'STATUS':<12} {'PRIO':<10} {'RETRIES':<8}", bold=True)
    click.echo("-" * 90)
    for j in jobs:
        color = "green" if j.status == JobStatus.COMPLETED else "red" if j.status in (JobStatus.FAILED, JobStatus.DEAD_LETTER) else "yellow"
        prio_name = j.priority.name
        status_text = click.style(f"{j.status.value:<12}", fg=color)
        click.echo(f"{j.id:<38} {j.task_name:<18} {status_text} {prio_name:<10} {j.retry_count:<8}")


@main.command()
@click.argument("job_id")
@click.option("--db", default="hyperqueue.db", help="Path to SQLite database.")
def status(job_id: str, db: str):
    """Inspect detailed information of a specific job."""
    backend = SQLiteStorageBackend(db_path=db)
    engine = QueueEngine(backend=backend)
    job = engine.get_job(job_id)
    if not job:
        click.secho(f"Job '{job_id}' not found.", fg="red")
        sys.exit(1)

    click.secho(f"Job Details: {job.id}", fg="cyan", bold=True)
    click.echo(json.dumps(job.to_dict(), indent=2))


@main.group()
def dlq():
    """Manage the Dead-Letter Queue (DLQ)."""
    pass


@dlq.command(name="replay")
@click.option("--id", "job_id", default=None, help="Replay a specific job ID (or all if omitted).")
@click.option("--db", default="hyperqueue.db", help="Path to SQLite database.")
def dlq_replay(job_id: str, db: str):
    """Replay failed jobs in the dead-letter queue."""
    backend = SQLiteStorageBackend(db_path=db)
    engine = QueueEngine(backend=backend)
    count = engine.replay_dead_letter(job_id=job_id)
    click.secho(f"Replayed {count} job(s) from DEAD_LETTER queue back to PENDING.", fg="green")


@main.command()
@click.option("--db", default="hyperqueue.db", help="Path to SQLite database.")
def stats(db: str):
    """View job statistics and queue health counts."""
    backend = SQLiteStorageBackend(db_path=db)
    engine = QueueEngine(backend=backend)
    counts = engine.stats()
    click.secho("HyperQueue Aggregate Statistics", bold=True)
    for st, count in counts.items():
        click.echo(f"  {st:<15}: {count}")


@main.command()
@click.option("--count", default=200, help="Number of benchmark tasks to run.")
def benchmark(count: int):
    """Run an end-to-end local latency and throughput benchmark."""
    click.secho(f"Running benchmark with {count} tasks...", fg="cyan", bold=True)
    from hyperqueue.storage.memory import MemoryStorageBackend

    backend = MemoryStorageBackend()
    engine = QueueEngine(backend=backend)

    @Task(name="bench_task")
    def bench_task(x: int):
        return x * x

    start_enq = time.time()
    for i in range(count):
        prio = Priority.CRITICAL if i % 10 == 0 else Priority.NORMAL
        engine.enqueue("bench_task", {"x": i}, priority=prio)
    enq_duration = time.time() - start_enq
    click.echo(f"  Enqueued {count} tasks in {enq_duration:.4f}s ({count / enq_duration:.1f} enqueue/s)")

    pool = WorkerPool(backend=backend, concurrency=8, poll_interval=0.01)

    async def run_until_done():
        runner = asyncio.create_task(pool.start())
        while True:
            st = backend.count_by_status()
            if st.get(JobStatus.COMPLETED.value, 0) >= count:
                break
            await asyncio.sleep(0.01)
        pool.stop()
        await runner

    start_proc = time.time()
    asyncio.run(run_until_done())
    proc_duration = time.time() - start_proc
    click.secho(f"  Processed {count} tasks in {proc_duration:.4f}s ({count / proc_duration:.1f} tasks/sec)", fg="green", bold=True)

    metrics = global_metrics.get_summary()
    click.echo(f"  P50 Latency: {metrics['latency_p50_ms']} ms")
    click.echo(f"  P95 Latency: {metrics['latency_p95_ms']} ms")


if __name__ == "__main__":
    main()
