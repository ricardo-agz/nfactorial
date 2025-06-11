from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as redis
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from factorial.agent import BaseAgent
from factorial.logging import get_logger

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
    """Metrics collector using Redis pipelines and caching"""

    def __init__(
        self,
        redis_client: redis.Redis,
        timeline_duration: int,
        bucket_duration: int,
        retention_duration: int,
        namespace: str,
        total_configured_workers: int,
        cache_ttl: float = 15.0,
    ):
        self._r = redis_client
        self._start_ts = time.time()
        self._timeline_duration = timeline_duration
        self._bucket_duration = bucket_duration
        self._retention_duration = retention_duration
        self._namespace = namespace
        self._total_configured_workers = total_configured_workers
        self._cache_ttl = cache_ttl

        # Simple in-memory cache
        self._cache: CachedMetrics | None = None
        self._cache_lock = asyncio.Lock()

    async def _check_redis(self) -> bool:
        """Check Redis connectivity"""
        try:
            await self._r.ping()
            return True
        except Exception:
            return False

    async def get_all_metrics(self, agents: list[BaseAgent[Any]]) -> dict[str, Any]:
        """Get all metrics using Redis pipelines and transactions"""

        # Check cache first
        async with self._cache_lock:
            if self._cache and not self._cache.is_expired():
                logger.debug("Returning cached metrics")
                return self._cache.data

        try:
            # Get agent names
            agent_names = [agent.name for agent in agents]

            # Calculate bucket timestamps
            current_time = time.time()
            start_time = current_time - self._timeline_duration
            start_bucket = (
                int(start_time / self._bucket_duration) * self._bucket_duration
            )
            end_bucket = (
                int(current_time / self._bucket_duration) * self._bucket_duration
            )
            bucket_timestamps = list(
                range(
                    start_bucket,
                    end_bucket + self._bucket_duration,
                    self._bucket_duration,
                )
            )

            # Initialize result structure
            system_totals = {"queued": 0, "processing": 0, "idle": 0, "backoff": 0}
            activity_timeline = {
                metric_type: {"buckets": [], "total_tasks": 0, "total_duration": 0}
                for metric_type in ["completed", "failed", "cancelled", "retried"]
            }
            agent_metrics = {}
            agent_activity_timelines = {}

            # Step 1: Collect current queue states using pipeline
            pipe = self._r.pipeline(transaction=False)
            for agent_name in agent_names:
                # Use exact Redis key patterns from queue.py
                queue_main_key = f"{self._namespace}:queue:{agent_name}:main"
                processing_heartbeats_key = (
                    f"{self._namespace}:processing:{agent_name}:heartbeats"
                )
                queue_backoff_key = f"{self._namespace}:queue:{agent_name}:backoff"
                agent_idle_gauge_key = (
                    f"{self._namespace}:metrics:gauge:{agent_name}:idle"
                )

                pipe.llen(queue_main_key)  # Get main queue length
                pipe.zcard(processing_heartbeats_key)  # Get processing count
                pipe.zcard(queue_backoff_key)  # Get backoff count
                pipe.get(agent_idle_gauge_key)  # Get idle count
                pipe.zrange(
                    processing_heartbeats_key, -1, -1, withscores=True
                )  # Get last heartbeat

            queue_results = await pipe.execute()

            # Process queue state results
            for i, agent_name in enumerate(agent_names):
                base_idx = i * 5
                queued = int(queue_results[base_idx] or 0)
                processing = int(queue_results[base_idx + 1] or 0)
                backoff = int(queue_results[base_idx + 2] or 0)
                idle = int(queue_results[base_idx + 3] or 0)
                last_heartbeat_data = queue_results[base_idx + 4]
                last_heartbeat = (
                    float(last_heartbeat_data[0][1]) if last_heartbeat_data else None
                )

                agent_metrics[agent_name] = {
                    "queued": queued,
                    "processing": processing,
                    "backoff": backoff,
                    "idle": idle,
                    "last_heartbeat": last_heartbeat,
                    "completed": 0,
                    "failed": 0,
                    "total_duration": 0,
                }

                agent_activity_timelines[agent_name] = {
                    metric_type: {"buckets": [], "total_tasks": 0, "total_duration": 0}
                    for metric_type in ["completed", "failed", "cancelled", "retried"]
                }

                system_totals["queued"] += queued
                system_totals["processing"] += processing
                system_totals["backoff"] += backoff
                system_totals["idle"] += idle

            # Step 2: Collect metrics for each bucket using pipeline
            for bucket_timestamp in bucket_timestamps:
                pipe = self._r.pipeline(transaction=False)

                # Get global metrics using exact key pattern from queue.py
                global_metrics_key = (
                    f"{self._namespace}:metrics:__all__:{bucket_timestamp}"
                )
                pipe.hmget(
                    global_metrics_key,
                    "completed_count",
                    "completed_total_duration",
                    "completed_total_turns",
                    "completed_total_retries",
                    "failed_count",
                    "failed_total_duration",
                    "failed_total_turns",
                    "failed_total_retries",
                    "cancelled_count",
                    "cancelled_total_duration",
                    "cancelled_total_turns",
                    "cancelled_total_retries",
                    "retried_count",
                    "retried_total_duration",
                    "retried_total_turns",
                    "retried_total_retries",
                )

                # Get per-agent metrics
                for agent_name in agent_names:
                    agent_metrics_key = (
                        f"{self._namespace}:metrics:{agent_name}:{bucket_timestamp}"
                    )
                    pipe.hmget(
                        agent_metrics_key,
                        "completed_count",
                        "completed_total_duration",
                        "completed_total_turns",
                        "completed_total_retries",
                        "failed_count",
                        "failed_total_duration",
                        "failed_total_turns",
                        "failed_total_retries",
                        "cancelled_count",
                        "cancelled_total_duration",
                        "cancelled_total_turns",
                        "cancelled_total_retries",
                        "retried_count",
                        "retried_total_duration",
                        "retried_total_turns",
                        "retried_total_retries",
                    )

                results = await pipe.execute()

                # Process global metrics
                global_metrics = results[0]
                metric_types = ["completed", "failed", "cancelled", "retried"]
                for i, metric_type in enumerate(metric_types):
                    base_idx = i * 4
                    count = int(global_metrics[base_idx] or 0)
                    total_duration = float(global_metrics[base_idx + 1] or 0)
                    total_turns = int(global_metrics[base_idx + 2] or 0)
                    total_retries = int(global_metrics[base_idx + 3] or 0)

                    activity_timeline[metric_type]["buckets"].append(
                        {
                            "timestamp": bucket_timestamp,
                            "count": count,
                            "total_duration": total_duration,
                            "total_turns": total_turns,
                            "total_retries": total_retries,
                        }
                    )
                    activity_timeline[metric_type]["total_tasks"] += count
                    activity_timeline[metric_type]["total_duration"] += total_duration

                # Process per-agent metrics
                for agent_idx, agent_name in enumerate(agent_names):
                    agent_metrics_result = results[agent_idx + 1]
                    for i, metric_type in enumerate(metric_types):
                        base_idx = i * 4
                        count = int(agent_metrics_result[base_idx] or 0)
                        total_duration = float(agent_metrics_result[base_idx + 1] or 0)
                        total_turns = int(agent_metrics_result[base_idx + 2] or 0)
                        total_retries = int(agent_metrics_result[base_idx + 3] or 0)

                        agent_activity_timelines[agent_name][metric_type][
                            "buckets"
                        ].append(
                            {
                                "timestamp": bucket_timestamp,
                                "count": count,
                                "total_duration": total_duration,
                                "total_turns": total_turns,
                                "total_retries": total_retries,
                            }
                        )
                        agent_activity_timelines[agent_name][metric_type][
                            "total_tasks"
                        ] += count
                        agent_activity_timelines[agent_name][metric_type][
                            "total_duration"
                        ] += total_duration

                        if metric_type == "completed":
                            agent_metrics[agent_name]["completed"] += count
                            agent_metrics[agent_name]["total_duration"] += (
                                total_duration
                            )
                        elif metric_type == "failed":
                            agent_metrics[agent_name]["failed"] += count

            # Calculate derived metrics using full timeline for success rate
            total_completed = activity_timeline["completed"]["total_tasks"]
            total_failed = activity_timeline["failed"]["total_tasks"]
            total_attempts = total_completed + total_failed
            system_success_rate = (
                (total_completed / total_attempts * 100) if total_attempts > 0 else 0.0
            )

            # Calculate current throughput using recent activity (last 2 minutes)
            # Use the most recent buckets for more current throughput
            recent_window_seconds = self._bucket_duration * 2  # Last 2 buckets
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

            # Calculate system throughput based on actual completion rate
            # Throughput = tasks completed per minute
            system_throughput = (
                recent_completed / recent_minutes if recent_minutes > 0 else 0.0
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

                # Calculate agent throughput based on actual completion rate
                # Throughput = tasks completed per minute
                agent_throughput = (
                    agent_recent_completed / recent_minutes
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
                    "total_workers": self._total_configured_workers,
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

            logger.debug("Collected metrics using Redis pipelines and transactions")
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
    runners: list[Any],
    metrics_config: Any,
    dashboard_name: str = "Factorial Dashboard",
    namespace: str = "factorial",
) -> None:
    """Add observability routes to the FastAPI app"""

    # Calculate total configured workers from all runners
    total_configured_workers = 0
    for runner in runners:
        # Each runner has agent workers + maintenance workers
        agent_workers = runner.agent_worker_config.workers
        maintenance_workers = runner.maintenance_worker_config.workers
        total_configured_workers += agent_workers + maintenance_workers

    # Create metrics collector with namespace
    collector = MetricsCollector(
        redis_client=redis_client,
        timeline_duration=metrics_config.timeline_duration,
        bucket_duration=metrics_config.bucket_duration,
        retention_duration=metrics_config.retention_duration,
        namespace=namespace,
        total_configured_workers=total_configured_workers,
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
                html_content = f.read()
                html_content = html_content.replace("{{APP_NAME}}", dashboard_name)
                return html_content
        except FileNotFoundError:
            return "<html><body><h1>Dashboard not found</h1></body></html>"

    @app.get("/observability/api/metrics", response_class=JSONResponse)
    async def metrics():
        """Get all metrics efficiently using single Lua script call + caching"""
        return await collector.get_all_metrics(agents)

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
