# HyperQueue ⚡

[![HyperQueue CI](https://github.com/jurjees2122-sudo/hyperqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/jurjees2122-sudo/hyperqueue/actions)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-throughput, fault-tolerant asynchronous task execution engine and job orchestrator. Built with zero heavy external runtime dependencies, featuring **priority scheduling**, **WAL-mode persistent storage**, **exponential backoff with full jitter**, **dead-letter queuing (DLQ)**, and a **terminal CLI**.

---

## 🏛️ System Architecture

```mermaid
flowchart TD
    Client["Client / CLI / Producer"] -->|submit(task, payload, priority)| Engine["Queue Engine"]
    Engine -->|atomic persist & order| Storage[("Storage Engine\n(SQLite WAL / In-Memory Heap)")]

    subgraph WorkerPool ["Worker Pool (Async Concurrency)"]
        W1["Worker Coroutine 1"]
        W2["Worker Coroutine 2"]
        WN["Worker Coroutine N"]
    end

    Storage -->|atomic lease acquire| WorkerPool
    WorkerPool -->|on success| Metrics["Telemetry & Metrics\n(P50/P95 Latency, Throughput)"]
    WorkerPool -->|on failure| RetryMgr["Retry Manager\n(Exponential Backoff + Jitter)"]
    RetryMgr -->|retries <= max| Storage
    RetryMgr -->|retries > max| DLQ[("Dead-Letter Queue (DLQ)")]
```

---

## ✨ Key Features

- **Multi-Level Priority Queuing**: Supports `CRITICAL`, `HIGH`, `NORMAL`, and `LOW` priorities with starvation-free scheduling.
- **ACID Persistence with Zero-Config**: Built-in SQLite storage utilizing **Write-Ahead Logging (WAL)** and atomic row leasing for local fault-tolerance.
- **In-Memory Priority Heap Backend**: High-performance backend capable of thousands of operations per second for testing and transient caching.
- **Intelligent Retry & Resilience**: Decorrelated exponential backoff with full jitter to avoid the "thundering herd" problem.
- **Dead-Letter Queue (DLQ) Management**: Isolates poison-pill jobs and provides CLI commands to inspect and replay jobs.
- **Real-Time Telemetry**: Tracks P50, P95, and P99 execution latency percentiles and task throughput.
- **Full CLI & Terminal Tooling**: Control workers, inspect jobs, enqueue tasks, and run benchmarks directly from your terminal.

---

## 🚀 Quickstart

### 1. Installation

```bash
git clone https://github.com/jurjees2122-sudo/hyperqueue.git
cd hyperqueue
pip install -r requirements.txt
```

### 2. Python API

```python
import asyncio
from hyperqueue import Task, HyperQueueClient, WorkerPool, Priority

# Register an asynchronous or synchronous task
@Task(name="send_alert")
async def send_alert(recipient: str, message: str):
    await asyncio.sleep(0.05)
    return {"status": "sent", "recipient": recipient}

async def main():
    client = HyperQueueClient()

    # Enqueue a high-priority task
    job = client.submit(
        task_name="send_alert",
        payload={"recipient": "ops-team@company.internal", "message": "High CPU alert"},
        priority=Priority.HIGH
    )
    print(f"Enqueued job: {job.id}")

    # Start worker pool with 4 concurrent coroutines
    pool = WorkerPool(backend=client.engine.backend, concurrency=4)
    worker_task = asyncio.create_task(pool.start())

    # Wait for result
    result_job = client.wait_for_job(job.id, timeout_sec=5.0)
    print(f"Job completed: {result_job.result}")

    pool.stop()
    await worker_task

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 💻 Terminal CLI Usage

HyperQueue provides a developer-friendly command-line interface for terminal-driven development and operations:

```bash
# 1. Start a background worker pool with 4 concurrent coroutines
python -m hyperqueue.cli worker --concurrency 4

# 2. Enqueue a task directly from your terminal
python -m hyperqueue.cli enqueue compute --priority CRITICAL --payload '{"n": 5000}'

# 3. List active and completed jobs
python -m hyperqueue.cli list

# 4. Inspect job details
python -m hyperqueue.cli status <job-id>

# 5. Check queue health and aggregate statistics
python -m hyperqueue.cli stats

# 6. Replay poison-pill tasks from the Dead-Letter Queue
python -m hyperqueue.cli dlq replay

# 7. Run an end-to-end performance benchmark
python -m hyperqueue.cli benchmark --count 1000
```

---

## ⚖️ Engineering Trade-offs & Design Decisions

| Decision | Chosen Architecture | Alternative Considered | Rationale |
| :--- | :--- | :--- | :--- |
| **Persistence** | SQLite with WAL Mode | Redis / PostgreSQL | Zero external service dependencies for local development. WAL mode allows concurrent readers and atomic row leases without external locks. |
| **Worker Concurrency** | Python `asyncio` Event Loop | ThreadPool / MultiProcess | Minimal memory overhead per worker coroutine. Easily handles I/O-bound jobs (network calls, external APIs, db queries). CPU-bound jobs offload cleanly via `run_in_executor`. |
| **Retry Strategy** | Exponential Backoff with Full Jitter | Constant Fixed Retries | Prevents synchronized retry spikes (thundering herd problem) against downstream services during outages. |
| **Queue Structure** | Min-Heap Priority Tuple `(-priority, scheduled_at, seq)` | FIFO LinkedList / RingBuffer | Enables strict $O(\log N)$ prioritization while guaranteeing deterministic FIFO ordering for equal priorities. |

---

## 🧪 Running the Test Suite

```bash
# Run unit and integration tests
python -m unittest discover -s tests -v
```

All 13 unit and integration tests cover:
- Memory and SQLite CRUD operations and persistence integrity
- Strict priority ordering and delayed execution
- Exponential backoff calculation and dead-letter routing
- Multi-coroutine worker pool concurrency and graceful shutdown

---

## 🗺️ Active Development Roadmap

Currently being actively developed using AI-assisted terminal pairing:

- [ ] **Redis Backend**: Add distributed Redis cluster backend using Lua scripts for atomic leasing.
- [ ] **DAG / Workflow Pipelines**: Support parent-child task chaining (e.g., Run Task B only after Task A succeeds).
- [ ] **Prometheus Exporter**: Standalone HTTP `/metrics` endpoint for Prometheus/Grafana monitoring.
- [ ] **Webhook Dispatcher**: Automatic HTTP callback notification upon task completion or DLQ failure.
- [ ] **Distributed Worker Heartbeats**: Node auto-discovery and automatic stale task recovery for dead worker nodes.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
