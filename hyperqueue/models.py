import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Callable, Dict, Optional


class Priority(IntEnum):
    LOW = 10
    NORMAL = 20
    HIGH = 30
    CRITICAL = 40


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    DEAD_LETTER = "DEAD_LETTER"
    CANCELLED = "CANCELLED"


@dataclass
class RetryPolicy:
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True

    def calculate_delay(self, retry_count: int) -> float:
        """Calculate exponential backoff delay with optional full jitter."""
        import random
        delay = min(self.max_delay, self.initial_delay * (self.backoff_multiplier ** retry_count))
        if self.jitter:
            delay = random.uniform(0.0, delay)
        return max(0.1, delay)


@dataclass
class Job:
    task_name: str
    payload: Dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    priority: Priority = Priority.NORMAL
    status: JobStatus = JobStatus.PENDING
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    created_at: float = field(default_factory=time.time)
    scheduled_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    retry_count: int = 0
    max_execution_time_sec: float = 300.0
    last_error: Optional[str] = None
    result: Optional[Any] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "task_name": self.task_name,
            "payload": self.payload,
            "priority": int(self.priority),
            "status": self.status.value,
            "retry_policy": {
                "max_retries": self.retry_policy.max_retries,
                "initial_delay": self.retry_policy.initial_delay,
                "max_delay": self.retry_policy.max_delay,
                "backoff_multiplier": self.retry_policy.backoff_multiplier,
                "jitter": self.retry_policy.jitter,
            },
            "created_at": self.created_at,
            "scheduled_at": self.scheduled_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "retry_count": self.retry_count,
            "max_execution_time_sec": self.max_execution_time_sec,
            "last_error": self.last_error,
            "result": self.result,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        rp_data = data.get("retry_policy", {})
        policy = RetryPolicy(
            max_retries=rp_data.get("max_retries", 3),
            initial_delay=rp_data.get("initial_delay", 1.0),
            max_delay=rp_data.get("max_delay", 60.0),
            backoff_multiplier=rp_data.get("backoff_multiplier", 2.0),
            jitter=rp_data.get("jitter", True),
        )
        job = cls(
            id=data["id"],
            task_name=data["task_name"],
            payload=data.get("payload", {}),
            priority=Priority(data.get("priority", 20)),
            status=JobStatus(data.get("status", "PENDING")),
            retry_policy=policy,
            created_at=data.get("created_at", time.time()),
            scheduled_at=data.get("scheduled_at", time.time()),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            retry_count=data.get("retry_count", 0),
            max_execution_time_sec=data.get("max_execution_time_sec", 300.0),
            last_error=data.get("last_error"),
            result=data.get("result"),
        )
        return job


class Task:
    """Registry wrapper for tasks executable by HyperQueue."""
    _registry: Dict[str, Callable] = {}

    def __init__(self, name: Optional[str] = None):
        self.name = name

    def __call__(self, fn: Callable) -> Callable:
        task_name = self.name or fn.__name__
        Task._registry[task_name] = fn
        fn.__hyperqueue_task_name__ = task_name
        return fn

    @classmethod
    def get(cls, name: str) -> Optional[Callable]:
        return cls._registry.get(name)

    @classmethod
    def all_tasks(cls) -> Dict[str, Callable]:
        return dict(cls._registry)
