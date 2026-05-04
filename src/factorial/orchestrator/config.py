from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class AgentWorkerConfig:
    workers: int = 1
    batch_size: int = 25
    max_retries: int = 5
    heartbeat_interval: int = 5
    missed_heartbeats_threshold: int = 5
    missed_heartbeats_grace_period: int = 5
    turn_timeout: int = 120


@dataclass
class TaskTTLConfig:
    """TTL configuration for finished tasks, in seconds."""

    completed_ttl: int = 3600
    failed_ttl: int = 86400
    cancelled_ttl: int = 1800


@dataclass
class MetricsTimelineConfig:
    """Configuration for metrics timeline and bucketing."""

    timeline_duration: int = 3600
    bucket_size: Literal["seconds", "minutes", "hours", "days"] = "minutes"
    retention_multiplier: float = 2.0

    def __post_init__(self) -> None:
        min_buckets = 50
        if self.bucket_size == "seconds":
            min_duration = min_buckets
        elif self.bucket_size == "minutes":
            min_duration = min_buckets * 60
        elif self.bucket_size == "hours":
            min_duration = min_buckets * 60 * 60
        elif self.bucket_size == "days":
            min_duration = min_buckets * 60 * 60 * 24
        else:
            raise ValueError(f"Invalid bucket_size: {self.bucket_size}")

        if self.timeline_duration < min_duration:
            raise ValueError(
                f"Timeline duration ({self.timeline_duration}s) is too short "
                f"for bucket size '{self.bucket_size}'. "
                f"Minimum duration required: {min_duration}s "
                f"to ensure at least {min_buckets} buckets."
            )

    @property
    def retention_duration(self) -> int:
        return int(self.timeline_duration * self.retention_multiplier)

    @property
    def bucket_duration(self) -> int:
        if self.bucket_size == "seconds":
            return 1
        if self.bucket_size == "minutes":
            return 60
        if self.bucket_size == "hours":
            return 3600
        if self.bucket_size == "days":
            return 86400
        raise ValueError(f"Invalid bucket_size: {self.bucket_size}")

    @property
    def display_name(self) -> str:
        if self.timeline_duration < 3600:
            minutes = self.timeline_duration // 60
            return f"{minutes}m"
        if self.timeline_duration < 86400:
            hours = self.timeline_duration // 3600
            return f"{hours}h"
        days = self.timeline_duration // 86400
        return f"{days}d"


@dataclass
class MaintenanceWorkerConfig:
    """Configuration for stale recovery and garbage collection."""

    interval: int = 10
    workers: int = 1
    task_ttl: TaskTTLConfig = field(default_factory=TaskTTLConfig)
    max_cleanup_batch: int = 100
    metrics_timeline: MetricsTimelineConfig = field(
        default_factory=MetricsTimelineConfig
    )


@dataclass
class ObservabilityConfig:
    """Configuration for the observability dashboard."""

    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8080
    cors_origins: list[str] = field(default_factory=lambda: ["*"])
    dashboard_name: str | None = None


__all__ = [
    "AgentWorkerConfig",
    "MaintenanceWorkerConfig",
    "MetricsTimelineConfig",
    "ObservabilityConfig",
    "TaskTTLConfig",
]
