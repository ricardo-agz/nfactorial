from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import redis.asyncio as redis
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from agent.agent import BaseAgent
from agent.logging import get_logger
from agent.scripts import create_metrics_aggregation_script, MetricsAggregationScript

logger = get_logger(__name__)


@dataclass
class CachedMetrics:
    """Cached metrics with timestamp"""

    data: dict[str, Any]
    timestamp: float
    ttl: float

    def is_expired(self) -> bool:
        return time.time() > (self.timestamp + self.ttl)


class MetricsCollector:
    """Metrics collector using single Lua script and caching"""

    def __init__(
        self,
        redis_client: redis.Redis,
        timeline_duration: int,
        bucket_duration: int,
        retention_duration: int,
        cache_ttl: float = 5.0,
    ):
        self._r = redis_client
        self._start_ts = time.time()
        self._timeline_duration = timeline_duration
        self._bucket_duration = bucket_duration
        self._retention_duration = retention_duration
        self._cache_ttl = cache_ttl

        # Simple in-memory cache
        self._cache: Optional[CachedMetrics] = None
        self._cache_lock = asyncio.Lock()

        # Lazy-loaded script
        self._aggregation_script: Optional[MetricsAggregationScript] = None

    async def _get_aggregation_script(self) -> MetricsAggregationScript:
        """Lazy load the aggregation script"""
        if self._aggregation_script is None:
            self._aggregation_script = create_metrics_aggregation_script(self._r)
        return self._aggregation_script

    async def _check_redis(self) -> bool:
        """Check Redis connectivity"""
        try:
            await self._r.ping()
            return True
        except Exception:
            return False

    async def get_all_metrics(self, agents: list[BaseAgent[Any]]) -> dict[str, Any]:
        """Get all metrics using single Lua script call with caching"""

        # Check cache first
        async with self._cache_lock:
            if self._cache and not self._cache.is_expired():
                logger.debug("Returning cached metrics")
                return self._cache.data

        try:
            # Get agent names
            agent_names = [agent.name for agent in agents]

            # Use the aggregation script to get all data in one call
            script = await self._get_aggregation_script()
            result = await script.execute(
                timeline_duration=self._timeline_duration,
                bucket_duration=self._bucket_duration,
                agent_names=agent_names,
            )

            # Process the aggregated data
            system_totals = result.system_totals
            activity_timeline = result.activity_timeline
            agent_metrics = result.agent_metrics
            agent_activity_timelines = getattr(result, "agent_activity_timelines", {})

            # Calculate derived metrics using full timeline for success rate
            total_completed = activity_timeline["completed"]["total_tasks"]
            total_failed = activity_timeline["failed"]["total_tasks"]
            total_attempts = total_completed + total_failed
            system_success_rate = (
                (total_completed / total_attempts * 100) if total_attempts > 0 else 0.0
            )

            # Calculate current throughput using recent activity (last 5 minutes)
            # instead of the entire timeline duration to get actual current throughput
            recent_window_seconds = 300  # 5 minutes for current throughput
            current_time = time.time()
            recent_cutoff = current_time - recent_window_seconds

            # Count recent tasks from buckets
            recent_completed = 0
            recent_failed = 0

            for bucket in activity_timeline["completed"]["buckets"]:
                if bucket["timestamp"] >= recent_cutoff:
                    recent_completed += bucket["count"]

            for bucket in activity_timeline["failed"]["buckets"]:
                if bucket["timestamp"] >= recent_cutoff:
                    recent_failed += bucket["count"]

            recent_total = recent_completed + recent_failed
            recent_minutes = recent_window_seconds / 60.0

            # Calculate system throughput based on processing capacity, not just completion rate
            # If we have recent activity and average processing time, use capacity-based calculation
            system_avg_duration = (
                activity_timeline["completed"]["total_duration"]
                / activity_timeline["completed"]["total_tasks"]
                if activity_timeline["completed"]["total_tasks"] > 0
                else 0.0
            )

            if system_avg_duration > 0 and recent_total > 0:
                # Capacity-based throughput: 60 seconds / avg_duration_seconds = tasks per minute
                system_throughput = 60.0 / system_avg_duration
            else:
                # Fallback to completion rate if no processing time data
                system_throughput = (
                    recent_total / recent_minutes if recent_minutes > 0 else 0.0
                )

            # Get display name for activity window
            if self._timeline_duration < 3600:
                minutes = self._timeline_duration // 60
                activity_window_display = f"last {minutes}m"
            elif self._timeline_duration < 86400:
                hours = self._timeline_duration // 3600
                activity_window_display = f"last {hours}h"
            else:
                days = self._timeline_duration // 86400
                activity_window_display = f"last {days}d"

            # Helper function to calculate true last activity time
            def calculate_last_activity(agent_name: str, metrics: dict) -> float | None:
                """Calculate the latest activity time from heartbeats and task completions"""
                timestamps = []

                # Add last heartbeat if available
                if metrics.get("last_heartbeat") is not None:
                    timestamps.append(float(metrics["last_heartbeat"]))

                # Add latest activity from timeline buckets
                if agent_name in agent_activity_timelines:
                    agent_timeline = agent_activity_timelines[agent_name]
                    for metric_type in ["completed", "failed", "cancelled", "retried"]:
                        if metric_type in agent_timeline:
                            for bucket in agent_timeline[metric_type]["buckets"]:
                                if bucket["count"] > 0:
                                    timestamps.append(float(bucket["timestamp"]))

                return max(timestamps) if timestamps else None

            # Calculate performance data with proper last activity calculation
            performance_data = []
            for agent_name, metrics in agent_metrics.items():
                last_activity = calculate_last_activity(agent_name, metrics)

                # Calculate agent throughput using same recent window as system throughput
                agent_recent_completed = 0
                agent_recent_failed = 0

                if agent_name in agent_activity_timelines:
                    agent_timeline = agent_activity_timelines[agent_name]

                    # Count recent completed tasks
                    if "completed" in agent_timeline:
                        for bucket in agent_timeline["completed"]["buckets"]:
                            if bucket["timestamp"] >= recent_cutoff:
                                agent_recent_completed += bucket["count"]

                    # Count recent failed tasks
                    if "failed" in agent_timeline:
                        for bucket in agent_timeline["failed"]["buckets"]:
                            if bucket["timestamp"] >= recent_cutoff:
                                agent_recent_failed += bucket["count"]

                agent_recent_total = agent_recent_completed + agent_recent_failed

                # Calculate agent throughput based on processing capacity
                agent_avg_duration = (
                    metrics.get("total_duration", 0) / metrics.get("completed", 1)
                    if metrics.get("completed", 0) > 0
                    else 0.0
                )

                if agent_avg_duration > 0 and agent_recent_total > 0:
                    # Capacity-based throughput: 60 seconds / avg_duration_seconds = tasks per minute
                    agent_throughput = 60.0 / agent_avg_duration
                else:
                    # Fallback to completion rate if no processing time data
                    agent_throughput = (
                        agent_recent_total / recent_minutes
                        if recent_minutes > 0
                        else 0.0
                    )

                performance_data.append(
                    {
                        "current_throughput": agent_throughput,
                        "success_rate_percent": (
                            (
                                metrics.get("completed", 0)
                                / (
                                    metrics.get("completed", 0)
                                    + metrics.get("failed", 0)
                                )
                                * 100
                            )
                            if (metrics.get("completed", 0) + metrics.get("failed", 0))
                            > 0
                            else 0.0
                        ),
                        "avg_processing_time_seconds": (
                            metrics.get("total_duration", 0)
                            / metrics.get("completed", 1)
                            if metrics.get("completed", 0) > 0
                            else 0.0
                        ),
                        "last_activity": (
                            last_activity * 1000 if last_activity is not None else None
                        ),
                    }
                )

            # Build response
            metrics_data = {
                "system": {
                    "total_agents": len(agents),
                    "total_tasks_queued": system_totals["queued"],
                    "total_tasks_processing": system_totals["processing"],
                    "total_workers": system_totals["processing"],
                    "redis_connected": True,
                    "uptime_seconds": int(time.time() - self._start_ts),
                    "overall_success_rate_percent": system_success_rate,
                    "system_throughput_per_minute": system_throughput,
                    "activity_window_display": activity_window_display,
                },
                "task_distribution": {
                    "queued": system_totals["queued"],
                    "processing": system_totals["processing"],
                    "backoff": system_totals["backoff"],
                    "completed": activity_timeline["completed"]["total_tasks"],
                    "failed": activity_timeline["failed"]["total_tasks"],
                    "cancelled": activity_timeline["cancelled"]["total_tasks"],
                    "retried": activity_timeline["retried"]["total_tasks"],
                    "idle": system_totals["idle"],
                    "pending_tool_results": 0,
                },
                "queues": [
                    {
                        "agent_name": agent_name,
                        "main_queue_size": metrics["queued"],
                        "processing_count": metrics["processing"],
                        "backoff_count": metrics["backoff"],
                        "completed_count": metrics.get("completed", 0),
                        "failed_count": metrics.get("failed", 0),
                        "stale_tasks_count": 0,
                    }
                    for agent_name, metrics in agent_metrics.items()
                ],
                "performance": performance_data,
                "activity_charts": {
                    metric_type: {
                        "buckets": data["buckets"],
                        "total_tasks": data["total_tasks"],
                        "avg_duration": (
                            data["total_duration"] / data["total_tasks"]
                            if data["total_tasks"] > 0
                            else 0.0
                        ),
                        "success_rate": (
                            system_success_rate if metric_type == "completed" else 0.0
                        ),
                        "ttl_seconds": self._timeline_duration,
                        "task_timestamps": [
                            b["timestamp"] for b in data["buckets"] if b["count"] > 0
                        ],
                    }
                    for metric_type, data in activity_timeline.items()
                },
                "agent_activity_charts": {
                    agent_name: {
                        metric_type: {
                            "buckets": data["buckets"],
                            "total_tasks": data["total_tasks"],
                            "avg_duration": (
                                data["total_duration"] / data["total_tasks"]
                                if data["total_tasks"] > 0
                                else 0.0
                            ),
                            "ttl_seconds": self._timeline_duration,
                            "task_timestamps": [
                                b["timestamp"]
                                for b in data["buckets"]
                                if b["count"] > 0
                            ],
                        }
                        for metric_type, data in agent_timeline.items()
                    }
                    for agent_name, agent_timeline in agent_activity_timelines.items()
                },
                "ttl_info": {
                    "timeline_duration": self._timeline_duration,
                    "bucket_duration": self._bucket_duration,
                    "retention_duration": self._retention_duration,
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            # Cache the result
            async with self._cache_lock:
                self._cache = CachedMetrics(
                    data=metrics_data,
                    timestamp=time.time(),
                    ttl=self._cache_ttl,
                )

            logger.debug("Collected metrics using Lua script")
            return metrics_data

        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")
            # Fallback to basic system info
            redis_ok = await self._check_redis()
            return {
                "error": "Failed to collect metrics",
                "system": {
                    "redis_connected": redis_ok,
                    "uptime_seconds": int(time.time() - self._start_ts),
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    async def invalidate_cache(self) -> None:
        """Manually invalidate the cache"""
        async with self._cache_lock:
            self._cache = None


def add_observability_routes(
    app: FastAPI,
    redis_client: redis.Redis,
    agents: list[BaseAgent[Any]],
    metrics_config: Any,
) -> None:
    """Add observability routes with Lua scripts and caching"""

    # Extract timeline configuration
    timeline_duration = getattr(metrics_config, "timeline_duration", 3600)
    bucket_duration = getattr(metrics_config, "bucket_duration", 60)
    retention_duration = getattr(metrics_config, "retention_duration", 7200)

    collector = MetricsCollector(
        redis_client,
        timeline_duration=timeline_duration,
        bucket_duration=bucket_duration,
        retention_duration=retention_duration,
        cache_ttl=10.0,
    )

    # Mount static files
    import os

    dashboard_dir = os.path.dirname(__file__)
    app.mount(
        "/observability/static",
        StaticFiles(directory=dashboard_dir),
        name="dashboard_static",
    )

    @app.get("/observability", response_class=HTMLResponse)
    async def dashboard():
        """Serve the dashboard HTML"""
        dashboard_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
        try:
            with open(dashboard_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            return "<html><body><h1>Dashboard not found</h1></body></html>"

    @app.get("/observability/api/metrics", response_class=JSONResponse)
    async def metrics():
        """Get all metrics efficiently using single Lua script call + caching"""
        return await collector.get_all_metrics(agents)

    @app.get("/observability/api/metrics/fast", response_class=JSONResponse)
    async def metrics_fast():
        """Get metrics with reduced timeline for faster loading"""
        # Create a fast collector with shorter timeline
        fast_collector = MetricsCollector(
            redis_client,
            timeline_duration=900,  # 15 minutes instead of 1 hour
            bucket_duration=bucket_duration,
            retention_duration=retention_duration,
            cache_ttl=15.0,  # Longer cache for fast endpoint
        )
        return await fast_collector.get_all_metrics(agents)

    @app.get("/observability/api/health", response_class=JSONResponse)
    async def health():
        """Simple health check"""
        redis_ok = await collector._check_redis()
        return {
            "status": "healthy" if redis_ok else "unhealthy",
            "redis": redis_ok,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    @app.post("/observability/api/cache/invalidate", response_class=JSONResponse)
    async def invalidate_cache():
        """Manually invalidate metrics cache"""
        await collector.invalidate_cache()
        return {
            "status": "cache_invalidated",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
