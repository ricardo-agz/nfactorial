from __future__ import annotations
import asyncio
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as redis
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from agent.agent import BaseAgent
from agent.logging import get_logger
from agent.context import TaskStatus

logger = get_logger("agent.dashboard")

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class TTLInfo:
    """TTL configuration information for display purposes"""

    completed_ttl_seconds: int
    failed_ttl_seconds: int
    cancelled_ttl_seconds: int

    @property
    def completed_ttl_display(self) -> str:
        return self._format_duration(self.completed_ttl_seconds)

    @property
    def failed_ttl_display(self) -> str:
        return self._format_duration(self.failed_ttl_seconds)

    @property
    def cancelled_ttl_display(self) -> str:
        return self._format_duration(self.cancelled_ttl_seconds)

    @property
    def min_ttl_seconds(self) -> int:
        """Returns the minimum TTL across all task types.

        This is useful for determining the shortest time window
        for activity data collection.
        """
        return min(
            self.completed_ttl_seconds,
            self.failed_ttl_seconds,
            self.cancelled_ttl_seconds,
        )

    @property
    def max_ttl_seconds(self) -> int:
        """Returns the maximum TTL across all task types.

        This is useful for determining the longest time window
        for activity data collection.
        """
        return max(
            self.completed_ttl_seconds,
            self.failed_ttl_seconds,
            self.cancelled_ttl_seconds,
        )

    def _format_duration(self, seconds: int) -> str:
        """Format duration in seconds to human-readable string."""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            if remaining_seconds == 0:
                return f"{minutes}m"
            else:
                return f"{minutes}m {remaining_seconds}s"
        else:
            hours = seconds // 3600
            remaining_minutes = (seconds % 3600) // 60
            if remaining_minutes == 0:
                return f"{hours}h"
            else:
                return f"{hours}h {remaining_minutes}m"


@dataclass(slots=True)
class QueueMetrics:
    agent_name: str
    main_queue_size: int
    processing_count: int
    completed_count: int
    failed_count: int
    cancelled_count: int
    pending_tool_results_count: int
    stale_tasks_count: int
    last_updated: datetime


@dataclass(slots=True)
class AgentPerformanceMetrics:
    agent_name: str
    tasks_per_minute: float
    avg_processing_time_seconds: float
    success_rate_percent: float
    retry_rate_percent: float
    current_throughput: int
    last_activity: datetime | None


@dataclass(slots=True)
class SystemMetrics:
    total_agents: int
    total_workers: int
    healthy_workers: int
    total_tasks_queued: int
    total_tasks_processing: int
    total_tasks_completed_recent: int
    total_tasks_failed_recent: int
    redis_connected: bool
    uptime_seconds: int
    system_throughput_per_minute: float
    overall_success_rate_percent: float
    activity_window_display: str  # e.g., "Last 1h", "Last 30m", etc.
    last_updated: datetime


@dataclass(slots=True)
class TaskDistribution:
    queued: int
    processing: int
    completed: int
    failed: int
    cancelled: int
    pending_tool_results: int
    total: int


@dataclass(slots=True)
class ActivityBucket:
    """Represents activity in a time bucket"""

    timestamp: float  # Unix timestamp for the bucket start
    count: int  # Number of tasks in this bucket


@dataclass(slots=True)
class ActivityChartData:
    """Activity chart data for a specific task type"""

    task_type: str  # 'completed', 'failed', or 'cancelled'
    ttl_seconds: int  # TTL for this task type
    bucket_size_seconds: int  # Size of each time bucket
    buckets: list[ActivityBucket]  # Time-ordered buckets
    total_tasks: int  # Total tasks across all buckets
    task_timestamps: list[float]  # Raw task timestamps for precise frontend rendering


@dataclass(slots=True)
class SystemActivityCharts:
    """Activity charts for all task types across all agents"""

    completed: ActivityChartData
    failed: ActivityChartData
    cancelled: ActivityChartData
    last_updated: datetime


# ---------------------------------------------------------------------------
# Enhanced metric collector
# ---------------------------------------------------------------------------


class ObservabilityCollector:
    """Collects comprehensive queue & system metrics with graceful Redis failure handling."""

    def __init__(self, redis_client: redis.Redis, ttl_info: TTLInfo | None = None):
        self._r = redis_client
        self._start_ts = time.time()
        self._ttl_info = ttl_info or TTLInfo(
            completed_ttl_seconds=3600,  # 1 hour default
            failed_ttl_seconds=86400,  # 24 hours default
            cancelled_ttl_seconds=1800,  # 30 minutes default
        )

    # -------------- Redis helpers ------------------------------------------------

    async def _safe_int(self, coro: asyncio.Future) -> int:
        try:
            value = await coro
            return int(value or 0)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Redis metric fetch failed: %s", exc)
            return 0

    async def _safe_float(self, coro: asyncio.Future) -> float:
        try:
            value = await coro
            return float(value or 0.0)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Redis metric fetch failed: %s", exc)
            return 0.0

    async def _check_redis(self) -> bool:
        try:
            await self._r.ping()
            return True
        except Exception:  # noqa: BLE001
            return False

    async def _count_stale_tasks(
        self, agent: BaseAgent[Any], heartbeat_timeout: int = 30
    ) -> int:
        """Count tasks that haven't had a heartbeat in the specified timeout"""
        try:
            processing_key = f"processing:{agent.name}:heartbeats"
            cutoff_time = time.time() - heartbeat_timeout
            stale_count = await self._r.zcount(processing_key, 0, cutoff_time)
            return int(stale_count or 0)
        except Exception:
            return 0

    async def _count_pending_tool_results(self, agent: BaseAgent[Any]) -> int:
        """Count tasks waiting for tool results"""
        try:
            # This is an approximation - we'd need to scan task data to get exact count
            # For now, we'll use a simple heuristic based on tool result keys
            pattern = f"tool:{agent.name}:*:results"
            keys = await self._r.keys(pattern)
            return len(keys) if keys else 0
        except Exception:
            return 0

    async def _get_recent_activity_count(
        self, agent: BaseAgent[Any], window_seconds: int
    ) -> tuple[int, int]:
        """Get completed and failed task counts in the specified time window"""
        try:
            cutoff_time = time.time() - window_seconds

            completed_key = f"queue:{agent.name}:completed"
            failed_key = f"queue:{agent.name}:failed"

            pipe = self._r.pipeline()
            pipe.zcount(completed_key, cutoff_time, "+inf")
            pipe.zcount(failed_key, cutoff_time, "+inf")

            completed_recent, failed_recent = await pipe.execute()
            return int(completed_recent or 0), int(failed_recent or 0)
        except Exception:
            return 0, 0

    def _get_activity_window_seconds(self) -> int:
        """Get the appropriate time window for activity metrics based on TTL configuration"""
        # Use the minimum TTL as the activity window - this ensures we only show data
        # that we're confident exists across all task types
        min_ttl = self._ttl_info.min_ttl_seconds

        # Don't go above 24 hours for activity window, but respect the actual minimum TTL
        # even if it's very short (like 30 seconds)
        return min(min_ttl, 86400)

    def _get_activity_window_display(self) -> str:
        """Get a human-readable display string for the activity window"""
        window_seconds = self._get_activity_window_seconds()

        if window_seconds < 60:
            return f"Last {window_seconds}s"
        elif window_seconds < 3600:
            minutes = window_seconds // 60
            return f"Last {minutes}m"
        elif window_seconds < 86400:
            hours = window_seconds // 3600
            return f"Last {hours}h"
        else:
            return "Last 24h"

    async def _get_processing_times(self, agent: BaseAgent[Any]) -> tuple[float, float]:
        """Get average processing time and current throughput"""
        try:
            processing_key = f"processing:{agent.name}:heartbeats"
            completed_key = f"queue:{agent.name}:completed"

            # Get current processing tasks and their start times
            current_time = time.time()
            processing_tasks = await self._r.zrange(
                processing_key, 0, -1, withscores=True
            )

            if processing_tasks:
                total_processing_time = sum(
                    current_time - start_time for _, start_time in processing_tasks
                )
                avg_processing_time = total_processing_time / len(processing_tasks)
            else:
                avg_processing_time = 0.0

            # Get recent completions for throughput calculation (last 5 minutes)
            recent_cutoff = current_time - 300  # 5 minutes
            recent_completions = await self._r.zcount(
                completed_key, recent_cutoff, "+inf"
            )
            throughput = (recent_completions or 0) / 5.0  # per minute

            return avg_processing_time, throughput
        except Exception:
            return 0.0, 0.0

    async def _get_last_activity(self, agent: BaseAgent[Any]) -> datetime | None:
        """Get the timestamp of the last activity (completion or failure)"""
        try:
            completed_key = f"queue:{agent.name}:completed"
            failed_key = f"queue:{agent.name}:failed"

            pipe = self._r.pipeline()
            pipe.zrange(completed_key, -1, -1, withscores=True)
            pipe.zrange(failed_key, -1, -1, withscores=True)

            completed_latest, failed_latest = await pipe.execute()

            latest_timestamp = 0
            if completed_latest:
                latest_timestamp = max(latest_timestamp, completed_latest[0][1])
            if failed_latest:
                latest_timestamp = max(latest_timestamp, failed_latest[0][1])

            if latest_timestamp > 0:
                return datetime.fromtimestamp(latest_timestamp, tz=timezone.utc)
            return None
        except Exception:
            return None

    # -------------- Queue-level --------------------------------------------------

    async def get_queue_metrics(self, agent: BaseAgent[Any]) -> QueueMetrics:
        name = agent.name
        keys = {
            "main": f"queue:{name}:main",
            "processing": f"processing:{name}:heartbeats",
            "completed": f"queue:{name}:completed",
            "failed": f"queue:{name}:failed",
            "cancelled": f"queue:{name}:cancelled",
        }

        # Pipeline reduces RTT; tolerate failure gracefully.
        pipe = self._r.pipeline()
        pipe.llen(keys["main"])
        pipe.zcard(keys["processing"])
        pipe.zcard(keys["completed"])
        pipe.zcard(keys["failed"])
        pipe.zcard(keys["cancelled"])

        try:
            (
                main_sz,
                proc_cnt,
                comp_cnt,
                fail_cnt,
                canc_cnt,
            ) = await pipe.execute()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Queue metric pipeline failed for %s: %s", name, exc)
            main_sz = proc_cnt = comp_cnt = fail_cnt = canc_cnt = 0

        # Get additional metrics
        stale_count = await self._count_stale_tasks(agent)
        pending_tool_count = await self._count_pending_tool_results(agent)

        return QueueMetrics(
            agent_name=name,
            main_queue_size=int(main_sz),
            processing_count=int(proc_cnt),
            completed_count=int(comp_cnt),
            failed_count=int(fail_cnt),
            cancelled_count=int(canc_cnt),
            pending_tool_results_count=pending_tool_count,
            stale_tasks_count=stale_count,
            last_updated=datetime.now(timezone.utc),
        )

    async def get_agent_performance_metrics(
        self, agent: BaseAgent[Any]
    ) -> AgentPerformanceMetrics:
        """Get performance-focused metrics for an agent"""
        window_seconds = self._get_activity_window_seconds()
        completed_recent, failed_recent = await self._get_recent_activity_count(
            agent, window_seconds
        )
        avg_processing_time, current_throughput = await self._get_processing_times(
            agent
        )
        last_activity = await self._get_last_activity(agent)

        total_recent = completed_recent + failed_recent
        success_rate = (
            (completed_recent / total_recent * 100) if total_recent > 0 else 0.0
        )

        # Calculate tasks per minute over the activity window
        window_minutes = window_seconds / 60
        tasks_per_minute = total_recent / window_minutes if total_recent > 0 else 0.0

        # Get retry rate (approximate)
        queue_metrics = await self.get_queue_metrics(agent)
        total_attempts = queue_metrics.completed_count + queue_metrics.failed_count
        retry_rate = 0.0  # This would need task-level retry tracking for accuracy

        return AgentPerformanceMetrics(
            agent_name=agent.name,
            tasks_per_minute=tasks_per_minute,
            avg_processing_time_seconds=avg_processing_time,
            success_rate_percent=success_rate,
            retry_rate_percent=retry_rate,
            current_throughput=int(current_throughput),
            last_activity=last_activity,
        )

    # -------------- System-level --------------------------------------------------

    async def get_system_metrics(self, agents: list[BaseAgent[Any]]) -> SystemMetrics:
        redis_ok = await self._check_redis()
        window_seconds = self._get_activity_window_seconds()
        activity_window_display = self._get_activity_window_display()

        # Collect queue metrics concurrently for speed.
        queue_metrics_results = await asyncio.gather(
            *(self.get_queue_metrics(a) for a in agents), return_exceptions=True
        )

        # Collect performance metrics concurrently
        performance_metrics_results = await asyncio.gather(
            *(self.get_agent_performance_metrics(a) for a in agents),
            return_exceptions=True,
        )

        total_q = total_p = total_completed_recent = total_failed_recent = 0
        total_throughput = 0.0
        valid_agents = 0

        for qm, pm in zip(queue_metrics_results, performance_metrics_results):
            if isinstance(qm, Exception) or isinstance(pm, Exception):
                continue

            valid_agents += 1
            total_q += qm.main_queue_size
            total_p += qm.processing_count
            total_throughput += pm.current_throughput

            # Get recent counts using the same window as performance metrics
            completed_recent, failed_recent = await self._get_recent_activity_count(
                agents[queue_metrics_results.index(qm)], window_seconds
            )
            total_completed_recent += completed_recent
            total_failed_recent += failed_recent

        # Calculate overall success rate
        total_recent = total_completed_recent + total_failed_recent
        overall_success_rate = (
            (total_completed_recent / total_recent * 100) if total_recent > 0 else 0.0
        )

        # Estimate healthy workers (simplified - assumes 1 worker per processing task)
        healthy_workers = total_p

        return SystemMetrics(
            total_agents=len(agents),
            total_workers=total_p,  # This is approximate
            healthy_workers=healthy_workers,
            total_tasks_queued=total_q,
            total_tasks_processing=total_p,
            total_tasks_completed_recent=total_completed_recent,
            total_tasks_failed_recent=total_failed_recent,
            redis_connected=redis_ok,
            uptime_seconds=int(time.time() - self._start_ts),
            system_throughput_per_minute=total_throughput,
            overall_success_rate_percent=overall_success_rate,
            activity_window_display=activity_window_display,
            last_updated=datetime.now(timezone.utc),
        )

    async def get_task_distribution(
        self, agents: list[BaseAgent[Any]]
    ) -> TaskDistribution:
        """Get overall task distribution across all agents"""
        queue_metrics_results = await asyncio.gather(
            *(self.get_queue_metrics(a) for a in agents), return_exceptions=True
        )

        queued = processing = completed = failed = cancelled = pending_tool = 0

        for qm in queue_metrics_results:
            if isinstance(qm, Exception):
                continue

            queued += qm.main_queue_size
            processing += qm.processing_count
            completed += qm.completed_count
            failed += qm.failed_count
            cancelled += qm.cancelled_count
            pending_tool += qm.pending_tool_results_count

        total = queued + processing + completed + failed + cancelled + pending_tool

        return TaskDistribution(
            queued=queued,
            processing=processing,
            completed=completed,
            failed=failed,
            cancelled=cancelled,
            pending_tool_results=pending_tool,
            total=total,
        )

    def get_ttl_info(self) -> TTLInfo:
        """Get TTL configuration information"""
        return self._ttl_info

    def _calculate_bucket_size(self, ttl_seconds: int) -> int:
        """Calculate appropriate bucket size based on TTL duration"""
        # Aim for around 50-100 buckets for good visualization
        target_buckets = 75

        # Calculate bucket size and round to nice intervals
        raw_bucket_size = ttl_seconds / target_buckets

        if raw_bucket_size <= 60:  # Less than 1 minute
            return max(30, int(raw_bucket_size // 30) * 30)  # 30s intervals
        elif raw_bucket_size <= 300:  # Less than 5 minutes
            return int(raw_bucket_size // 60) * 60  # 1-minute intervals
        elif raw_bucket_size <= 1800:  # Less than 30 minutes
            return int(raw_bucket_size // 300) * 300  # 5-minute intervals
        elif raw_bucket_size <= 3600:  # Less than 1 hour
            return int(raw_bucket_size // 900) * 900  # 15-minute intervals
        elif raw_bucket_size <= 7200:  # Less than 2 hours
            return int(raw_bucket_size // 1800) * 1800  # 30-minute intervals
        else:
            return int(raw_bucket_size // 3600) * 3600  # 1-hour intervals

    async def _get_activity_buckets(
        self, agents: list[BaseAgent[Any]], task_type: str, ttl_seconds: int
    ) -> ActivityChartData:
        """Get time-bucketed activity data for a specific task type"""
        bucket_size = self._calculate_bucket_size(ttl_seconds)
        current_time = time.time()
        start_time = current_time - ttl_seconds

        # Create buckets
        buckets = []
        bucket_start = start_time
        while bucket_start < current_time:
            buckets.append(ActivityBucket(timestamp=bucket_start, count=0))
            bucket_start += bucket_size

        # Collect all task timestamps for precise frontend rendering
        all_task_timestamps = []

        # Query Redis for all agents
        total_tasks = 0
        for agent in agents:
            try:
                key = f"queue:{agent.name}:{task_type}"

                # Get all tasks in the time window
                tasks_in_window = await self._r.zrangebyscore(
                    key, start_time, current_time, withscores=True
                )

                # Distribute tasks into buckets and collect timestamps
                for _, task_timestamp in tasks_in_window:
                    all_task_timestamps.append(task_timestamp)

                    # Find the appropriate bucket
                    bucket_index = int((task_timestamp - start_time) // bucket_size)
                    if 0 <= bucket_index < len(buckets):
                        buckets[bucket_index].count += 1
                        total_tasks += 1

            except Exception as exc:
                logger.debug(
                    "Failed to get activity for %s %s: %s", agent.name, task_type, exc
                )
                continue

        return ActivityChartData(
            task_type=task_type,
            ttl_seconds=ttl_seconds,
            bucket_size_seconds=bucket_size,
            buckets=buckets,
            total_tasks=total_tasks,
            task_timestamps=all_task_timestamps,
        )

    async def get_system_activity_charts(
        self, agents: list[BaseAgent[Any]]
    ) -> SystemActivityCharts:
        """Get activity charts for all task types"""
        # Get activity data for each task type concurrently
        completed_task, failed_task, cancelled_task = await asyncio.gather(
            self._get_activity_buckets(
                agents, "completed", self._ttl_info.completed_ttl_seconds
            ),
            self._get_activity_buckets(
                agents, "failed", self._ttl_info.failed_ttl_seconds
            ),
            self._get_activity_buckets(
                agents, "cancelled", self._ttl_info.cancelled_ttl_seconds
            ),
            return_exceptions=True,
        )

        # Handle exceptions gracefully
        if isinstance(completed_task, Exception):
            logger.warning("Failed to get completed activity: %s", completed_task)
            completed_task = ActivityChartData(
                "completed", self._ttl_info.completed_ttl_seconds, 300, [], 0, []
            )

        if isinstance(failed_task, Exception):
            logger.warning("Failed to get failed activity: %s", failed_task)
            failed_task = ActivityChartData(
                "failed", self._ttl_info.failed_ttl_seconds, 300, [], 0, []
            )

        if isinstance(cancelled_task, Exception):
            logger.warning("Failed to get cancelled activity: %s", cancelled_task)
            cancelled_task = ActivityChartData(
                "cancelled", self._ttl_info.cancelled_ttl_seconds, 300, [], 0, []
            )

        return SystemActivityCharts(
            completed=completed_task,
            failed=failed_task,
            cancelled=cancelled_task,
            last_updated=datetime.now(timezone.utc),
        )


# ---------------------------------------------------------------------------
# Route helpers
# ---------------------------------------------------------------------------


def add_observability_routes(
    app: FastAPI,
    redis_client: redis.Redis,
    agents: list[BaseAgent[Any]],
    ttl_config: Any | None = None,  # TaskTTLConfig from manager.py
) -> None:
    """Mounts `/observability` and `/observability/api/*` endpoints on *app*."""

    # Convert TTL config to TTLInfo if provided
    ttl_info = None
    if ttl_config:
        ttl_info = TTLInfo(
            completed_ttl_seconds=ttl_config.completed_ttl,
            failed_ttl_seconds=ttl_config.failed_ttl,
            cancelled_ttl_seconds=ttl_config.cancelled_ttl,
        )

    collector = ObservabilityCollector(redis_client, ttl_info)

    # Mount static files for dashboard assets
    import os

    dashboard_dir = os.path.dirname(__file__)
    app.mount(
        "/observability/static",
        StaticFiles(directory=dashboard_dir),
        name="dashboard_static",
    )

    @app.get("/observability", response_class=HTMLResponse)
    async def _dash() -> str:  # noqa: D401, ANN001
        # Read and return the HTML file
        dashboard_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
        try:
            with open(dashboard_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            return "<html><body><h1>Dashboard not found</h1><p>The dashboard.html file is missing.</p></body></html>"

    @app.get("/observability/api/metrics", response_class=JSONResponse)
    async def _metrics():  # noqa: D401
        # Collect all metrics concurrently
        queue_metrics_task = asyncio.gather(
            *(collector.get_queue_metrics(a) for a in agents), return_exceptions=True
        )
        performance_metrics_task = asyncio.gather(
            *(collector.get_agent_performance_metrics(a) for a in agents),
            return_exceptions=True,
        )
        system_metrics_task = collector.get_system_metrics(agents)
        task_distribution_task = collector.get_task_distribution(agents)
        activity_charts_task = collector.get_system_activity_charts(agents)

        (
            queue_metrics,
            performance_metrics,
            system_metrics,
            task_distribution,
            activity_charts,
        ) = await asyncio.gather(
            queue_metrics_task,
            performance_metrics_task,
            system_metrics_task,
            task_distribution_task,
            activity_charts_task,
        )

        # Filter out exceptions
        valid_queue_metrics = [
            qm for qm in queue_metrics if not isinstance(qm, Exception)
        ]
        valid_performance_metrics = [
            pm for pm in performance_metrics if not isinstance(pm, Exception)
        ]

        return {
            "system": asdict(system_metrics),
            "queues": [asdict(qm) for qm in valid_queue_metrics],
            "performance": [asdict(pm) for pm in valid_performance_metrics],
            "task_distribution": asdict(task_distribution),
            "activity_charts": asdict(activity_charts),
            "ttl_info": {
                **asdict(collector.get_ttl_info()),
                # Add display properties that aren't included in asdict()
                "completed_ttl_display": collector.get_ttl_info().completed_ttl_display,
                "failed_ttl_display": collector.get_ttl_info().failed_ttl_display,
                "cancelled_ttl_display": collector.get_ttl_info().cancelled_ttl_display,
                "min_ttl_seconds": collector.get_ttl_info().min_ttl_seconds,
                "max_ttl_seconds": collector.get_ttl_info().max_ttl_seconds,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    @app.get("/observability/api/health", response_class=JSONResponse)
    async def _health():  # noqa: D401
        ok = await collector._check_redis()  # pylint: disable=protected-access
        return {
            "status": "healthy" if ok else "unhealthy",
            "redis": ok,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
