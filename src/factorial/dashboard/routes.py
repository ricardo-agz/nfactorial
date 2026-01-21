from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as redis
from fastapi import FastAPI, Query
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
        namespace: str,
        total_configured_workers: int,
        default_window: str = "1h",
        cache_ttl: float = 15.0,
    ):
        self._r = redis_client
        self._start_ts = time.time()
        self._namespace = namespace
        self._total_configured_workers = total_configured_workers
        self._default_window = default_window
        self._cache_ttl = cache_ttl

        # Simple in-memory cache
        self._cache: dict[str, CachedMetrics] = {}
        self._cache_lock = asyncio.Lock()

    async def _check_redis(self) -> bool:
        """Check Redis connectivity"""
        try:
            await self._r.ping()
            return True
        except Exception:
            return False

    async def get_all_metrics(
        self,
        agents: list[BaseAgent[Any]],
        *,
        window: str | None = None,
    ) -> dict[str, Any]:
        """Get all metrics from fixed-memory rolling bucket rings.

        Supported windows:
        - 1h  : minute-level bars (60)
        - 24h : 60 bars, each bar aggregates 24 minutes
        - 7d  : 60 bars, each bar aggregates 168 minutes (2h48m) using 6-minute buckets
        """

        window = (window or self._default_window or "1h").strip()
        if window not in {"1h", "24h", "7d"}:
            window = self._default_window

        # Check cache first (per-window)
        async with self._cache_lock:
            cached = self._cache.get(window)
            if cached and not cached.is_expired():
                logger.debug("Returning cached metrics")
                return cached.data

        try:
            agent_names = [agent.name for agent in agents]

            # ---------------------------
            # Window / ring configuration
            # ---------------------------
            chart_bars = 60
            metric_types = ["completed", "failed", "cancelled", "retried"]
            # UI only renders these; keep `retried` for distribution
            chart_metric_types = ["completed", "failed", "cancelled"]

            window_cfg: dict[str, Any] = {
                "1h": {
                    "window_seconds": 3600,
                    "ring_prefix": "m1",
                    "bucket_seconds": 60,
                    "ring_size": 1440,  # 24h of 1-minute slots
                },
                "24h": {
                    "window_seconds": 86400,
                    "ring_prefix": "m1",
                    "bucket_seconds": 60,
                    "ring_size": 1440,
                },
                "7d": {
                    "window_seconds": 7 * 86400,
                    "ring_prefix": "m6",
                    "bucket_seconds": 360,  # 6-minute slots
                    "ring_size": 1680,  # 7d / 6m
                },
            }[window]

            window_seconds = int(window_cfg["window_seconds"])
            ring_prefix = str(window_cfg["ring_prefix"])
            bucket_seconds = int(window_cfg["bucket_seconds"])
            ring_size = int(window_cfg["ring_size"])

            num_buckets = window_seconds // bucket_seconds
            if num_buckets <= 0:
                num_buckets = 1

            if num_buckets % chart_bars != 0:
                # Shouldn't happen with our configs; fall back to raw buckets.
                chart_bars = min(chart_bars, num_buckets)

            group_size = max(1, num_buckets // chart_bars)
            bar_seconds = group_size * bucket_seconds

            # Align to bucket boundaries
            now = time.time()
            end_bucket_ts = int(now // bucket_seconds) * bucket_seconds
            start_bucket_ts = end_bucket_ts - (num_buckets - 1) * bucket_seconds
            bucket_timestamps = [
                start_bucket_ts + i * bucket_seconds for i in range(num_buckets)
            ]
            slots = [
                (ts // bucket_seconds) % ring_size for ts in bucket_timestamps
            ]
            bar_timestamps = [
                bucket_timestamps[i * group_size] for i in range(chart_bars)
            ]

            # ---------------------------
            # Step 1: Queue state (fast)
            # ---------------------------
            system_totals = {"queued": 0, "processing": 0, "pending": 0, "backoff": 0}

            pipe = self._r.pipeline(transaction=False)
            for agent_name in agent_names:
                queue_main_key = f"{self._namespace}:queue:{agent_name}:main"
                processing_heartbeats_key = (
                    f"{self._namespace}:processing:{agent_name}:heartbeats"
                )
                queue_backoff_key = f"{self._namespace}:queue:{agent_name}:backoff"
                queue_pending_key = f"{self._namespace}:queue:{agent_name}:pending"

                pipe.llen(queue_main_key)
                pipe.zcard(processing_heartbeats_key)
                pipe.zcard(queue_backoff_key)
                pipe.zcard(queue_pending_key)
                pipe.zrange(processing_heartbeats_key, -1, -1, withscores=True)

            queue_results = await pipe.execute()

            agent_metrics: dict[str, dict[str, Any]] = {}
            for i, agent_name in enumerate(agent_names):
                base_idx = i * 5
                queued = int(queue_results[base_idx] or 0)
                processing = int(queue_results[base_idx + 1] or 0)
                backoff = int(queue_results[base_idx + 2] or 0)
                pending = int(queue_results[base_idx + 3] or 0)
                last_heartbeat_data = queue_results[base_idx + 4]
                last_heartbeat = (
                    float(last_heartbeat_data[0][1]) if last_heartbeat_data else None
                )

                agent_metrics[agent_name] = {
                    "queued": queued,
                    "processing": processing,
                    "backoff": backoff,
                    "pending": pending,
                    "last_heartbeat": last_heartbeat,
                    "completed": 0,
                    "failed": 0,
                    "total_duration": 0.0,  # completed duration sum
                    "last_bucket_activity": None,  # seconds
                }

                system_totals["queued"] += queued
                system_totals["processing"] += processing
                system_totals["backoff"] += backoff
                system_totals["pending"] += pending

            # ---------------------------
            # Step 2: Rolling metrics rings (bounded)
            # ---------------------------
            global_metrics_key = f"{self._namespace}:metrics:__all__"

            def build_fields(prefix: str, slot_list: list[int]) -> list[str]:
                fields: list[str] = []
                for slot in slot_list:
                    fields.append(f"{prefix}:ts:{slot}")
                    fields.append(f"{prefix}:completed_count:{slot}")
                    fields.append(f"{prefix}:completed_total_duration:{slot}")
                    fields.append(f"{prefix}:failed_count:{slot}")
                    fields.append(f"{prefix}:cancelled_count:{slot}")
                    fields.append(f"{prefix}:retried_count:{slot}")
                return fields

            fields = build_fields(ring_prefix, slots)

            pipe = self._r.pipeline(transaction=False)
            pipe.hmget(global_metrics_key, fields)
            for agent_name in agent_names:
                pipe.hmget(f"{self._namespace}:metrics:{agent_name}", fields)
            ring_results = await pipe.execute()

            def parse_ring(values: list[Any]) -> tuple[
                dict[str, list[int]],
                list[float],
                dict[str, int],
                float,
                int | None,
            ]:
                # Per-bar aggregation
                bar_counts = {m: [0] * chart_bars for m in metric_types}
                bar_completed_duration = [0.0] * chart_bars

                totals = dict.fromkeys(metric_types, 0)
                total_completed_duration = 0.0
                last_bucket_activity: int | None = None

                # Values per bucket: [ts, completed_count, completed_dur,
                #                    failed_count, cancelled_count, retried_count]
                for i, expected_ts in enumerate(bucket_timestamps):
                    off = i * 6
                    ts_raw = values[off]
                    ts = int(ts_raw) if ts_raw else 0
                    if ts != expected_ts:
                        continue

                    completed = int(values[off + 1] or 0)
                    completed_dur = float(values[off + 2] or 0.0)
                    failed = int(values[off + 3] or 0)
                    cancelled = int(values[off + 4] or 0)
                    retried = int(values[off + 5] or 0)

                    if completed or failed or cancelled or retried:
                        last_bucket_activity = expected_ts

                    bar_idx = i // group_size
                    if bar_idx >= chart_bars:
                        bar_idx = chart_bars - 1

                    bar_counts["completed"][bar_idx] += completed
                    bar_counts["failed"][bar_idx] += failed
                    bar_counts["cancelled"][bar_idx] += cancelled
                    bar_counts["retried"][bar_idx] += retried
                    bar_completed_duration[bar_idx] += completed_dur

                    totals["completed"] += completed
                    totals["failed"] += failed
                    totals["cancelled"] += cancelled
                    totals["retried"] += retried
                    total_completed_duration += completed_dur

                return (
                    bar_counts,
                    bar_completed_duration,
                    totals,
                    total_completed_duration,
                    last_bucket_activity,
                )

            # Global first, then per-agent in the same order we queued them
            (
                global_bar_counts,
                global_bar_completed_dur,
                global_totals,
                global_completed_dur_sum,
                _,
            ) = parse_ring(ring_results[0])

            # Fill per-agent timelines + totals
            agent_activity_charts: dict[str, dict[str, Any]] = {}
            for idx, agent_name in enumerate(agent_names):
                (
                    bar_counts,
                    bar_completed_dur,
                    totals,
                    completed_dur_sum,
                    last_bucket_activity,
                ) = parse_ring(ring_results[idx + 1])

                agent_metrics[agent_name]["completed"] = totals["completed"]
                agent_metrics[agent_name]["failed"] = totals["failed"]
                agent_metrics[agent_name]["total_duration"] = completed_dur_sum
                agent_metrics[agent_name]["last_bucket_activity"] = last_bucket_activity

                # Build agent charts (60 bars) for UI toggles
                agent_activity_charts[agent_name] = {}
                for metric_type in chart_metric_types:
                    total_tasks = totals[metric_type]
                    total_duration = (
                        completed_dur_sum if metric_type == "completed" else 0.0
                    )
                    buckets = [
                        {
                            "timestamp": bar_timestamps[i],
                            "count": bar_counts[metric_type][i],
                            "total_duration": (
                                bar_completed_dur[i]
                                if metric_type == "completed"
                                else 0.0
                            ),
                            "total_turns": 0,
                            "total_retries": 0,
                        }
                        for i in range(chart_bars)
                    ]
                    agent_activity_charts[agent_name][metric_type] = {
                        "buckets": buckets,
                        "total_tasks": total_tasks,
                        "avg_duration": (
                            (total_duration / total_tasks) if total_tasks > 0 else 0.0
                        ),
                        "ttl_seconds": window_seconds,
                        "task_timestamps": [
                            bar_timestamps[i]
                            for i in range(chart_bars)
                            if bar_counts[metric_type][i] > 0
                        ],
                    }

            # Global success rate for the selected window
            total_completed = global_totals["completed"]
            total_failed = global_totals["failed"]
            total_attempts = total_completed + total_failed
            system_success_rate = (
                (total_completed / total_attempts * 100) if total_attempts > 0 else 0.0
            )

            # Build global charts (60 bars)
            activity_charts: dict[str, Any] = {}
            for metric_type in chart_metric_types:
                total_tasks = global_totals[metric_type]
                total_duration = (
                    global_completed_dur_sum if metric_type == "completed" else 0.0
                )
                buckets = [
                    {
                        "timestamp": bar_timestamps[i],
                        "count": global_bar_counts[metric_type][i],
                        "total_duration": (
                            global_bar_completed_dur[i]
                            if metric_type == "completed"
                            else 0.0
                        ),
                        "total_turns": 0,
                        "total_retries": 0,
                    }
                    for i in range(chart_bars)
                ]
                activity_charts[metric_type] = {
                    "buckets": buckets,
                    "total_tasks": total_tasks,
                    "avg_duration": (
                        (total_duration / total_tasks) if total_tasks > 0 else 0.0
                    ),
                    "success_rate": (
                        system_success_rate if metric_type == "completed" else 0.0
                    ),
                    "ttl_seconds": window_seconds,
                    "task_timestamps": [
                        bar_timestamps[i]
                        for i in range(chart_bars)
                        if global_bar_counts[metric_type][i] > 0
                    ],
                }

            # ---------------------------
            # Throughput: always "recent" (last 2 minutes) from m1 ring
            # ---------------------------
            recent_bucket_seconds = 60
            recent_ring_prefix = "m1"
            recent_ring_size = 1440
            recent_end_ts = int(now // recent_bucket_seconds) * recent_bucket_seconds
            recent_bucket_timestamps = [recent_end_ts - 60, recent_end_ts]
            recent_slots = [
                (ts // recent_bucket_seconds) % recent_ring_size
                for ts in recent_bucket_timestamps
            ]

            def build_recent_fields(prefix: str, slot_list: list[int]) -> list[str]:
                f: list[str] = []
                for slot in slot_list:
                    f.append(f"{prefix}:ts:{slot}")
                    f.append(f"{prefix}:completed_count:{slot}")
                return f

            recent_fields = build_recent_fields(recent_ring_prefix, recent_slots)
            pipe = self._r.pipeline(transaction=False)
            pipe.hmget(global_metrics_key, recent_fields)
            for agent_name in agent_names:
                pipe.hmget(f"{self._namespace}:metrics:{agent_name}", recent_fields)
            recent_results = await pipe.execute()

            def parse_recent_completed(values: list[Any]) -> int:
                completed_sum = 0
                for i, expected_ts in enumerate(recent_bucket_timestamps):
                    off = i * 2
                    ts_raw = values[off]
                    ts = int(ts_raw) if ts_raw else 0
                    if ts != expected_ts:
                        continue
                    completed_sum += int(values[off + 1] or 0)
                return completed_sum

            recent_completed_global = parse_recent_completed(recent_results[0])
            recent_minutes = 2.0
            system_throughput = (
                recent_completed_global / recent_minutes if recent_minutes > 0 else 0.0
            )

            recent_completed_by_agent: dict[str, int] = {}
            for idx, agent_name in enumerate(agent_names):
                recent_completed_by_agent[agent_name] = parse_recent_completed(
                    recent_results[idx + 1]
                )

            # Display name for activity window (UI)
            if window_seconds < 3600:
                activity_window_display = f"last {window_seconds // 60}m"
            elif window_seconds < 86400:
                activity_window_display = f"last {window_seconds // 3600}h"
            else:
                activity_window_display = f"last {window_seconds // 86400}d"

            # Last activity helper
            def calculate_last_activity(agent_name: str, metrics: dict) -> float | None:
                timestamps: list[float] = []
                if metrics.get("last_heartbeat") is not None:
                    timestamps.append(float(metrics["last_heartbeat"]))
                if metrics.get("last_bucket_activity") is not None:
                    timestamps.append(float(metrics["last_bucket_activity"]))
                return max(timestamps) if timestamps else None

            performance_data = []
            for agent_name, metrics in agent_metrics.items():
                last_activity = calculate_last_activity(agent_name, metrics)
                agent_recent_completed = recent_completed_by_agent.get(agent_name, 0)
                agent_throughput = (
                    agent_recent_completed / recent_minutes
                    if recent_minutes > 0
                    else 0.0
                )

                completed = int(metrics.get("completed", 0) or 0)
                failed = int(metrics.get("failed", 0) or 0)
                attempts = completed + failed
                success_rate_percent = (
                    (completed / attempts * 100) if attempts > 0 else 0.0
                )

                total_duration = float(metrics.get("total_duration", 0.0) or 0.0)
                avg_processing_time_seconds = (
                    (total_duration / completed) if completed > 0 else 0.0
                )

                performance_data.append(
                    {
                        "current_throughput": agent_throughput,
                        "success_rate_percent": success_rate_percent,
                        "avg_processing_time_seconds": avg_processing_time_seconds,
                        "last_activity": (
                            last_activity * 1000 if last_activity is not None else None
                        ),
                    }
                )

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
                    "window": window,
                },
                "task_distribution": {
                    "queued": system_totals["queued"],
                    "processing": system_totals["processing"],
                    "backoff": system_totals["backoff"],
                    "completed": global_totals["completed"],
                    "failed": global_totals["failed"],
                    "cancelled": global_totals["cancelled"],
                    "retried": global_totals["retried"],
                    "pending": system_totals["pending"],
                    "pending_tool_results": 0,
                },
                "queues": [
                    {
                        "agent_name": agent_name,
                        "main_queue_size": metrics["queued"],
                        "processing_count": metrics["processing"],
                        "backoff_count": metrics["backoff"],
                        "pending_count": metrics["pending"],
                        "completed_count": metrics.get("completed", 0),
                        "failed_count": metrics.get("failed", 0),
                        "stale_tasks_count": 0,
                    }
                    for agent_name, metrics in agent_metrics.items()
                ],
                "performance": performance_data,
                "activity_charts": activity_charts,
                "agent_activity_charts": agent_activity_charts,
                "ttl_info": {
                    "window": window,
                    "window_seconds": window_seconds,
                    "ring_prefix": ring_prefix,
                    "bucket_seconds": bucket_seconds,
                    "ring_size": ring_size,
                    "chart_bars": chart_bars,
                    "bar_seconds": bar_seconds,
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            # Cache the result
            async with self._cache_lock:
                self._cache[window] = CachedMetrics(
                    data=metrics_data,
                    timestamp=time.time(),
                    ttl=self._cache_ttl,
                )

            logger.debug("Collected metrics using rolling rings")
            return metrics_data

        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")
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
            self._cache = {}


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

    # Pick a sensible default window from config (if provided)
    default_window = "1h"
    try:
        timeline_duration = int(getattr(metrics_config, "timeline_duration", 0) or 0)
        if timeline_duration >= 7 * 86400:
            default_window = "7d"
        elif timeline_duration >= 24 * 3600:
            default_window = "24h"
    except Exception:
        default_window = "1h"

    collector = MetricsCollector(
        redis_client=redis_client,
        namespace=namespace,
        total_configured_workers=total_configured_workers,
        default_window=default_window,
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
            with open(dashboard_path, encoding="utf-8") as f:
                html_content = f.read()
                html_content = html_content.replace("{{APP_NAME}}", dashboard_name)
                return html_content
        except FileNotFoundError:
            return "<html><body><h1>Dashboard not found</h1></body></html>"

    @app.get("/observability/api/metrics", response_class=JSONResponse)
    async def metrics(
        window: str | None = Query(
            default=None, description="Metrics window: 1h, 24h, or 7d"
        )
    ):
        """Get dashboard metrics from rolling bucket rings (fixed memory)."""
        return await collector.get_all_metrics(agents, window=window)

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
