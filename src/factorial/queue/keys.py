import time
from dataclasses import dataclass
# ***** REDIS KEY SPACE *****

# HASH: task_id -> full task data (status, agent, payload, metadata)
TASKS_DATA = "{namespace}:tasks:data:{task_id}"
# HASH: task_id -> status
TASK_STATUS = (
    "{namespace}:tasks:status"  # queued | processing | completed | failed | cancelled
)
# HASH: task_id -> agent
TASK_AGENT = "{namespace}:tasks:agent"
# HASH: task_id -> agent payload
TASK_PAYLOAD = "{namespace}:tasks:payload"
# HASH: task_id -> num pickups (int)
TASK_PICKUPS = "{namespace}:tasks:pickups"
# HASH: task_id -> num retries (int)
TASK_RETRIES = "{namespace}:tasks:retries"
# HASH: task_id -> {created_at, owner_id}
TASK_META = "{namespace}:tasks:meta"

# ===== QUEUE MANAGEMENT =====
QUEUE_BASE = "{namespace}:queue"
# LIST: task_ids waiting to be processed
QUEUE_MAIN = "{namespace}:queue:{agent}:main"
# ZSET: task_id -> timestamp of failure
QUEUE_FAILED = "{namespace}:queue:{agent}:failed"
# ZSET: task_id -> timestamp of completion
QUEUE_COMPLETIONS = "{namespace}:queue:{agent}:completed"
# ZSET: task_id -> timestamp of cancellation
QUEUE_CANCELLED = "{namespace}:queue:{agent}:cancelled"
# ZSET: task_id -> timestamp of when task is ready to be picked up again
QUEUE_BACKOFF = "{namespace}:queue:{agent}:backoff"
# ZSET: task_id -> timestamp of orphaned task
QUEUE_ORPHANED = "{namespace}:queue:{agent}:orphaned"
# ZSET: task_id -> timestamp of when task was marked as pending (tools, children)
QUEUE_PENDING = "{namespace}:queue:{agent}:pending"

# ===== ACTIVE TASK PROCESSING =====
# ZSET: task_id -> timestamp of last heartbeat
PROCESSING_HEARTBEATS = "{namespace}:processing:{agent}:heartbeats"

# ===== STEERING & CONTROL =====
# SET: task_ids marked for cancellation
TASK_CANCELLATIONS = "{namespace}:cancel:pending"
# HASH: message_id -> steering_message_json
TASK_STEERING = "{namespace}:steer:{task_id}:messages"

# ===== PENDING TASK STATE MANAGEMENT =====
# HASH: tool_call_id -> result_json or <|PENDING|>
PENDING_TOOL_RESULTS = "{namespace}:pending:{task_id}:tools"
# HASH: child_task_id -> result_json or <|PENDING|>
PENDING_CHILD_TASK_RESULTS = "{namespace}:pending:{task_id}:children"

# ===== COMMUNICATION =====
# PUBSUB: real-time updates to task owners
UPDATES_CHANNEL = "{namespace}:updates:{owner_id}"

# ===== METRICS =====
# HASH: {completed, failed, ..., completed_duration, failed_duration, ...} (bucket is a timestamp of a time bucket)
# completed, failed, cancelled, retried
AGENT_ACTIVITY_METRICS = "{namespace}:metrics:{agent}:{bucket}"
GLOBAL_ACTIVITY_METRICS = "{namespace}:metrics:__all__:{bucket}"

# ===== SENTINEL VALUES =====
PENDING_SENTINEL = "<|*PENDING*|>"


@dataclass
class RedisKeys:
    # Global keys (always present when namespace provided)
    _task_status: str
    _task_agent: str
    _task_payload: str
    _task_pickups: str
    _task_retries: str
    _task_meta: str
    _task_cancellations: str

    # Agent-scoped keys (present when agent provided)
    _queue_main: str | None = None
    _queue_completions: str | None = None
    _queue_backoff: str | None = None
    _queue_failed: str | None = None
    _queue_orphaned: str | None = None
    _queue_pending: str | None = None
    _queue_cancelled: str | None = None
    _processing_heartbeats: str | None = None

    # Metrics keys (present when agent + metrics_bucket_duration provided)
    _agent_metrics_bucket: str | None = None
    _global_metrics_bucket: str | None = None

    # Task-scoped keys (present when task_id provided)
    _task_steering: str | None = None
    _pending_tool_results: str | None = None
    _pending_child_task_results: str | None = None

    # Owner-scoped keys (present when owner_id provided)
    _updates_channel: str | None = None

    @property
    def task_status(self) -> str:
        """{namespace}:tasks:status"""
        return self._task_status

    @property
    def task_agent(self) -> str:
        """{namespace}:tasks:agent"""
        return self._task_agent

    @property
    def task_payload(self) -> str:
        """{namespace}:tasks:payload"""
        return self._task_payload

    @property
    def task_pickups(self) -> str:
        """{namespace}:tasks:pickups"""
        return self._task_pickups

    @property
    def task_retries(self) -> str:
        """{namespace}:tasks:retries"""
        return self._task_retries

    @property
    def task_meta(self) -> str:
        """{namespace}:tasks:meta"""
        return self._task_meta

    @property
    def task_cancellations(self) -> str:
        """{namespace}:cancel:pending"""
        return self._task_cancellations

    @property
    def queue_main(self) -> str:
        """{namespace}:queue:{agent}:main"""
        if self._queue_main is None:
            raise ValueError(
                "queue_main is not available - agent was not provided during RedisKeys.format()"
            )
        return self._queue_main

    @property
    def queue_completions(self) -> str:
        """{namespace}:queue:{agent}:completed"""
        if self._queue_completions is None:
            raise ValueError(
                "queue_completions is not available - agent was not provided during RedisKeys.format()"
            )
        return self._queue_completions

    @property
    def queue_backoff(self) -> str:
        """{namespace}:queue:{agent}:backoff"""
        if self._queue_backoff is None:
            raise ValueError(
                "queue_backoff is not available - agent was not provided during RedisKeys.format()"
            )
        return self._queue_backoff

    @property
    def queue_failed(self) -> str:
        """{namespace}:queue:{agent}:failed"""
        if self._queue_failed is None:
            raise ValueError(
                "queue_failed is not available - agent was not provided during RedisKeys.format()"
            )
        return self._queue_failed

    @property
    def queue_orphaned(self) -> str:
        """{namespace}:queue:{agent}:orphaned"""
        if self._queue_orphaned is None:
            raise ValueError(
                "queue_orphaned is not available - agent was not provided during RedisKeys.format()"
            )
        return self._queue_orphaned

    @property
    def queue_pending(self) -> str:
        """{namespace}:queue:{agent}:pending"""
        if self._queue_pending is None:
            raise ValueError(
                "queue_pending is not available - agent was not provided during RedisKeys.format()"
            )
        return self._queue_pending

    @property
    def queue_cancelled(self) -> str:
        """{namespace}:queue:{agent}:cancelled"""
        if self._queue_cancelled is None:
            raise ValueError(
                "queue_cancelled is not available - agent was not provided during RedisKeys.format()"
            )
        return self._queue_cancelled

    @property
    def processing_heartbeats(self) -> str:
        """{namespace}:processing:{agent}:heartbeats"""
        if self._processing_heartbeats is None:
            raise ValueError(
                "processing_heartbeats is not available - agent was not provided during RedisKeys.format()"
            )
        return self._processing_heartbeats

    @property
    def agent_metrics_bucket(self) -> str:
        """{namespace}:metrics:{agent}:{bucket}"""
        if self._agent_metrics_bucket is None:
            raise ValueError(
                "agent_metrics_bucket is not available - agent and metrics_bucket_duration were not provided during RedisKeys.format()"
            )
        return self._agent_metrics_bucket

    @property
    def global_metrics_bucket(self) -> str:
        """{namespace}:metrics:__all__:{bucket}"""
        if self._global_metrics_bucket is None:
            raise ValueError(
                "global_metrics_bucket is not available - metrics_bucket_duration was not provided during RedisKeys.format()"
            )
        return self._global_metrics_bucket

    @property
    def task_steering(self) -> str:
        """{namespace}:steer:{task_id}:messages"""
        if self._task_steering is None:
            raise ValueError(
                "task_steering is not available - task_id was not provided during RedisKeys.format()"
            )
        return self._task_steering

    @property
    def pending_tool_results(self) -> str:
        """{namespace}:pending:{task_id}:tools"""
        if self._pending_tool_results is None:
            raise ValueError(
                "pending_tool_results is not available - task_id was not provided during RedisKeys.format()"
            )
        return self._pending_tool_results

    @property
    def pending_child_task_results(self) -> str:
        """{namespace}:pending:{task_id}:children"""
        if self._pending_child_task_results is None:
            raise ValueError(
                "pending_child_task_results is not available - task_id was not provided during RedisKeys.format()"
            )
        return self._pending_child_task_results

    @property
    def updates_channel(self) -> str:
        """{namespace}:updates:{owner_id}"""
        if self._updates_channel is None:
            raise ValueError(
                "updates_channel is not available - owner_id was not provided during RedisKeys.format()"
            )
        return self._updates_channel

    @classmethod
    def for_agent(cls, namespace: str, agent: str) -> "RedisKeys":
        return cls.format(namespace=namespace, agent=agent)

    @classmethod
    def for_task(cls, namespace: str, task_id: str) -> "RedisKeys":
        return cls.format(namespace=namespace, task_id=task_id)

    @classmethod
    def for_owner(cls, namespace: str, owner_id: str) -> "RedisKeys":
        return cls.format(namespace=namespace, owner_id=owner_id)

    @classmethod
    def format(
        cls,
        namespace: str,
        agent: str | None = None,
        task_id: str | None = None,
        owner_id: str | None = None,
        metrics_bucket_duration: int | None = None,
    ) -> "RedisKeys":
        bucket_id = (
            (int(time.time() / metrics_bucket_duration) * metrics_bucket_duration)
            if metrics_bucket_duration
            else None
        )

        return cls(
            # Global keys (namespace only)
            _task_status=TASK_STATUS.format(namespace=namespace),
            _task_agent=TASK_AGENT.format(namespace=namespace),
            _task_payload=TASK_PAYLOAD.format(namespace=namespace),
            _task_pickups=TASK_PICKUPS.format(namespace=namespace),
            _task_retries=TASK_RETRIES.format(namespace=namespace),
            _task_meta=TASK_META.format(namespace=namespace),
            _task_cancellations=TASK_CANCELLATIONS.format(namespace=namespace),
            # Agent-scoped keys
            _queue_main=QUEUE_MAIN.format(namespace=namespace, agent=agent)
            if agent
            else None,
            _queue_completions=QUEUE_COMPLETIONS.format(
                namespace=namespace, agent=agent
            )
            if agent
            else None,
            _queue_backoff=QUEUE_BACKOFF.format(namespace=namespace, agent=agent)
            if agent
            else None,
            _queue_failed=QUEUE_FAILED.format(namespace=namespace, agent=agent)
            if agent
            else None,
            _queue_orphaned=QUEUE_ORPHANED.format(namespace=namespace, agent=agent)
            if agent
            else None,
            _queue_pending=QUEUE_PENDING.format(namespace=namespace, agent=agent)
            if agent
            else None,
            _queue_cancelled=QUEUE_CANCELLED.format(namespace=namespace, agent=agent)
            if agent
            else None,
            _processing_heartbeats=PROCESSING_HEARTBEATS.format(
                namespace=namespace, agent=agent
            )
            if agent
            else None,
            # Metrics keys (require bucket calculation)
            _agent_metrics_bucket=AGENT_ACTIVITY_METRICS.format(
                namespace=namespace, agent=agent, bucket=bucket_id
            )
            if agent and metrics_bucket_duration
            else None,
            _global_metrics_bucket=GLOBAL_ACTIVITY_METRICS.format(
                namespace=namespace, bucket=bucket_id
            )
            if metrics_bucket_duration
            else None,
            # Task-scoped keys
            _task_steering=TASK_STEERING.format(namespace=namespace, task_id=task_id)
            if task_id
            else None,
            _pending_tool_results=PENDING_TOOL_RESULTS.format(
                namespace=namespace, task_id=task_id
            )
            if task_id
            else None,
            _pending_child_task_results=PENDING_CHILD_TASK_RESULTS.format(
                namespace=namespace, task_id=task_id
            )
            if task_id
            else None,
            # Owner-scoped keys
            _updates_channel=UPDATES_CHANNEL.format(
                namespace=namespace, owner_id=owner_id
            )
            if owner_id
            else None,
        )
