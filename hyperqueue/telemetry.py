import threading
import time
from collections import deque
from typing import Dict, List, Any


class MetricsCollector:
    """Collects real-time latency, throughput, and error metrics for HyperQueue."""

    def __init__(self, sample_window: int = 1000):
        self._lock = threading.Lock()
        self._sample_window = sample_window
        self._latencies: deque = deque(maxlen=sample_window)
        self._tasks_completed = 0
        self._tasks_failed = 0
        self._tasks_retried = 0
        self._start_time = time.time()

    def record_job_completion(self, duration_sec: float) -> None:
        with self._lock:
            self._tasks_completed += 1
            self._latencies.append(duration_sec)

    def record_job_failure(self, retried: bool = False) -> None:
        with self._lock:
            if retried:
                self._tasks_retried += 1
            else:
                self._tasks_failed += 1

    def get_summary(self) -> Dict[str, Any]:
        with self._lock:
            uptime = max(0.001, time.time() - self._start_time)
            sorted_latencies = sorted(self._latencies)
            n = len(sorted_latencies)

            p50 = sorted_latencies[int(n * 0.50)] if n else 0.0
            p95 = sorted_latencies[int(n * 0.95)] if n else 0.0
            p99 = sorted_latencies[int(n * 0.99)] if n else 0.0
            avg = sum(sorted_latencies) / n if n else 0.0

            throughput = self._tasks_completed / uptime

            return {
                "uptime_seconds": round(uptime, 2),
                "tasks_completed": self._tasks_completed,
                "tasks_failed": self._tasks_failed,
                "tasks_retried": self._tasks_retried,
                "throughput_per_sec": round(throughput, 2),
                "latency_avg_ms": round(avg * 1000, 2),
                "latency_p50_ms": round(p50 * 1000, 2),
                "latency_p95_ms": round(p95 * 1000, 2),
                "latency_p99_ms": round(p99 * 1000, 2),
            }


global_metrics = MetricsCollector()
