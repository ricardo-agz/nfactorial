from dataclasses import dataclass

# ***** REDIS KEY SPACE *****

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
# HASH: task_id -> {created_at, owner_id, parent_id, resumed_from_task_id, batch_id}
TASK_META = "{namespace}:tasks:meta"
# HASH: task_id -> {current_turn: int, max_turns: int | None, progress: float}
TASK_PROGRESS = "{namespace}:tasks:progress"

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
# ZSET: task_id -> timestamp of when task was parked pending
#       external tool/hook completions or child task results
QUEUE_PENDING = "{namespace}:queue:{agent}:pending"
# ZSET: task_id -> wake timestamp for time waits (sleep/cron)
QUEUE_SCHEDULED = "{namespace}:queue:{agent}:scheduled"

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
#       used for deferred tool completions, including hook-backed flows
PENDING_TOOL_RESULTS = "{namespace}:pending:{task_id}:tools"
# HASH: child_task_id -> result_json or <|PENDING|>
PENDING_CHILD_TASK_RESULTS = "{namespace}:pending:{task_id}:children"
# SET: child_task_id values currently being awaited by the parent task
PENDING_CHILD_WAIT_IDS = "{namespace}:pending:{task_id}:children_wait_ids"
# HASH: task_id -> scheduled wait metadata JSON
SCHEDULED_WAIT_META = "{namespace}:scheduled:wait_meta"

# ===== HOOK STATE MANAGEMENT =====
# HASH: hook_id -> hook_record_json
HOOKS_INDEX = "{namespace}:hooks:index"
# SET: hook_ids for a task
HOOKS_BY_TASK = "{namespace}:hooks:by_task:{task_id}"
# ZSET: hook_id -> expiry timestamp
HOOKS_EXPIRING = "{namespace}:hooks:expiring"
# STRING: idempotency envelope for repeated hook callbacks
HOOK_IDEMPOTENCY = "{namespace}:hooks:idem:{hook_id}:{idempotency_key}"
# STRING: idempotency envelope for repeated resume_task callbacks
RESUME_IDEMPOTENCY = "{namespace}:resume:idem:{source_task_id}:{idempotency_key}"
# STRING: idempotency envelope for repeated enqueue_task requests
ENQUEUE_IDEMPOTENCY = "{namespace}:enqueue:idem:{owner_id}:{agent}:{idempotency_key}"
# STRING: idempotency envelope for repeated create_batch_and_enqueue requests
BATCH_ENQUEUE_IDEMPOTENCY = (
    "{namespace}:enqueue_batch:idem:{owner_id}:{agent}:{idempotency_key}"
)
# HASH: session_id -> hook_session_json
HOOK_SESSIONS = "{namespace}:hooks:sessions"
# HASH: tool_call_id -> session_id (task scoped)
HOOK_SESSION_BY_TOOL_CALL = "{namespace}:hooks:session_by_tool_call:{task_id}"
# SET: session ids for a task
HOOK_SESSIONS_BY_TASK = "{namespace}:hooks:sessions_by_task:{task_id}"
# HASH: tool_call_id -> session_id marked ready for worker tick
HOOK_RUNTIME_READY = "{namespace}:hooks:runtime_ready:{task_id}"

# ===== COMMUNICATION =====
# PUBSUB: real-time updates to task owners
UPDATES_CHANNEL = "{namespace}:updates:{owner_id}"

# ===== METRICS =====
# Rolling (fixed-memory) metrics ring buffers.
# We store bucketed counters inside a single Redis HASH per agent + one global HASH.
# The Lua `inc_metrics()` helper maintains a rolling window by overwriting
# slots in-place.
#
# NOTE: These keys intentionally do NOT include a `{bucket}` timestamp.
# Bucket timestamps live inside hash fields so memory stays bounded.
AGENT_ACTIVITY_METRICS = "{namespace}:metrics:{agent}"
GLOBAL_ACTIVITY_METRICS = "{namespace}:metrics:__all__"

# ===== BATCH MANAGEMENT =====
# HASH: batch_id -> {owner_id, created_at, total_tasks, max_progress,
#                    status: 'active'|'cancelled'|'completed'}
BATCH_META = "{namespace}:batches:metadata"
# HASH: batch_id -> task_ids
BATCH_TASKS = "{namespace}:batches:tasks"
# HASH: batch_id -> remaining task_ids
BATCH_REMAINING_TASKS = "{namespace}:batches:remaining"
# HASH: batch_id -> progress_sum (int)
BATCH_PROGRESS = "{namespace}:batches:progress"
# ZSET: batch_id -> timestamp of completion
BATCH_COMPLETED = "{namespace}:batches:completed"

# ===== SENTINEL VALUES =====
PENDING_SENTINEL = "<|*PENDING*|>"


@dataclass
class RedisKeys:
    # Non-default first
    _task_status: str
    _task_agent: str
    _task_payload: str
    _task_pickups: str
    _task_retries: str
    _task_meta: str
    _task_cancellations: str
    # Hook keys (namespace scoped)
    _hooks_index: str
    _hooks_expiring: str
    _hook_idempotency: str
    _resume_idempotency: str
    _enqueue_idempotency: str
    _batch_enqueue_idempotency: str
    _hook_sessions: str
    _scheduled_wait_meta: str
    # Batch keys
    _batch_tasks: str
    _batch_remaining_tasks: str
    _batch_progress: str
    _batch_completed: str
    _batch_meta: str

    # Defaults after
    _queue_main: str | None = None
    _queue_completions: str | None = None
    _queue_backoff: str | None = None
    _queue_failed: str | None = None
    _queue_orphaned: str | None = None
    _queue_pending: str | None = None
    _queue_scheduled: str | None = None
    _queue_cancelled: str | None = None
    _processing_heartbeats: str | None = None

    # Metrics keys (rolling ring buffers; agent-scoped present when agent provided)
    _agent_metrics_bucket: str | None = None
    _global_metrics_bucket: str | None = None

    # Task-scoped keys (present when task_id provided)
    _task_steering: str | None = None
    _pending_tool_results: str | None = None
    _pending_child_task_results: str | None = None
    _pending_child_wait_ids: str | None = None
    _hooks_by_task: str | None = None
    _hook_session_by_tool_call: str | None = None
    _hook_sessions_by_task: str | None = None
    _hook_runtime_ready: str | None = None

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
    def hooks_index(self) -> str:
        """{namespace}:hooks:index"""
        return self._hooks_index

    @property
    def hooks_expiring(self) -> str:
        """{namespace}:hooks:expiring"""
        return self._hooks_expiring

    def hook_resolution(self, hook_id: str, request_key: str) -> str:
        """{namespace}:hooks:idem:{hook_id}:{request_key}"""
        return self._hook_idempotency.format(
            hook_id=hook_id, idempotency_key=request_key
        )

    def hook_idempotency(self, hook_id: str, idempotency_key: str) -> str:
        """Backward-compatible alias for hook_resolution()."""
        return self.hook_resolution(hook_id=hook_id, request_key=idempotency_key)

    def resume_idempotency(self, source_task_id: str, idempotency_key: str) -> str:
        """{namespace}:resume:idem:{source_task_id}:{idempotency_key}"""
        return self._resume_idempotency.format(
            source_task_id=source_task_id,
            idempotency_key=idempotency_key,
        )

    def enqueue_idempotency(
        self,
        owner_id: str,
        agent_name: str,
        idempotency_key: str,
    ) -> str:
        """{namespace}:enqueue:idem:{owner_id}:{agent}:{idempotency_key}"""
        return self._enqueue_idempotency.format(
            owner_id=owner_id,
            agent=agent_name,
            idempotency_key=idempotency_key,
        )

    def batch_enqueue_idempotency(
        self,
        owner_id: str,
        agent_name: str,
        idempotency_key: str,
    ) -> str:
        """{namespace}:enqueue_batch:idem:{owner_id}:{agent}:{idempotency_key}"""
        return self._batch_enqueue_idempotency.format(
            owner_id=owner_id,
            agent=agent_name,
            idempotency_key=idempotency_key,
        )

    @property
    def hook_sessions(self) -> str:
        """{namespace}:hooks:sessions"""
        return self._hook_sessions

    @property
    def scheduled_wait_meta(self) -> str:
        """{namespace}:scheduled:wait_meta"""
        return self._scheduled_wait_meta

    @property
    def batch_completed(self) -> str:
        """{namespace}:batches:completed"""
        return self._batch_completed

    @property
    def batch_meta(self) -> str:
        """{namespace}:batches:metadata"""
        return self._batch_meta

    @property
    def batch_tasks(self) -> str:
        """{namespace}:batches:tasks"""
        return self._batch_tasks

    @property
    def batch_remaining_tasks(self) -> str:
        """{namespace}:batches:remaining"""
        return self._batch_remaining_tasks

    @property
    def batch_progress(self) -> str:
        """{namespace}:batches:progress"""
        return self._batch_progress

    @property
    def queue_main(self) -> str:
        """{namespace}:queue:{agent}:main"""
        if self._queue_main is None:
            raise ValueError(
                "queue_main is not available - "
                "agent was not provided during RedisKeys.format()"
            )
        return self._queue_main

    @property
    def queue_completions(self) -> str:
        """{namespace}:queue:{agent}:completed"""
        if self._queue_completions is None:
            raise ValueError(
                "queue_completions is not available - "
                "agent was not provided during RedisKeys.format()"
            )
        return self._queue_completions

    @property
    def queue_backoff(self) -> str:
        """{namespace}:queue:{agent}:backoff"""
        if self._queue_backoff is None:
            raise ValueError(
                "queue_backoff is not available - "
                "agent was not provided during RedisKeys.format()"
            )
        return self._queue_backoff

    @property
    def queue_failed(self) -> str:
        """{namespace}:queue:{agent}:failed"""
        if self._queue_failed is None:
            raise ValueError(
                "queue_failed is not available - "
                "agent was not provided during RedisKeys.format()"
            )
        return self._queue_failed

    @property
    def queue_orphaned(self) -> str:
        """{namespace}:queue:{agent}:orphaned"""
        if self._queue_orphaned is None:
            raise ValueError(
                "queue_orphaned is not available - "
                "agent was not provided during RedisKeys.format()"
            )
        return self._queue_orphaned

    @property
    def queue_pending(self) -> str:
        """{namespace}:queue:{agent}:pending"""
        if self._queue_pending is None:
            raise ValueError(
                "queue_pending is not available - "
                "agent was not provided during RedisKeys.format()"
            )
        return self._queue_pending

    @property
    def queue_scheduled(self) -> str:
        """{namespace}:queue:{agent}:scheduled"""
        if self._queue_scheduled is None:
            raise ValueError(
                "queue_scheduled is not available - "
                "agent was not provided during RedisKeys.format()"
            )
        return self._queue_scheduled

    @property
    def queue_cancelled(self) -> str:
        """{namespace}:queue:{agent}:cancelled"""
        if self._queue_cancelled is None:
            raise ValueError(
                "queue_cancelled is not available - "
                "agent was not provided during RedisKeys.format()"
            )
        return self._queue_cancelled

    @property
    def processing_heartbeats(self) -> str:
        """{namespace}:processing:{agent}:heartbeats"""
        if self._processing_heartbeats is None:
            raise ValueError(
                "processing_heartbeats is not available - "
                "agent was not provided during RedisKeys.format()"
            )
        return self._processing_heartbeats

    @property
    def agent_metrics_bucket(self) -> str:
        """{namespace}:metrics:{agent}"""
        if self._agent_metrics_bucket is None:
            raise ValueError(
                "agent_metrics_bucket is not available - "
                "agent was not provided during RedisKeys.format()"
            )
        return self._agent_metrics_bucket

    @property
    def global_metrics_bucket(self) -> str:
        """{namespace}:metrics:__all__"""
        if self._global_metrics_bucket is None:
            raise ValueError(
                "global_metrics_bucket is not available - "
                "namespace was not provided during RedisKeys.format()"
            )
        return self._global_metrics_bucket

    @property
    def task_steering(self) -> str:
        """{namespace}:steer:{task_id}:messages"""
        if self._task_steering is None:
            raise ValueError(
                "task_steering is not available - "
                "task_id was not provided during RedisKeys.format()"
            )
        return self._task_steering

    @property
    def pending_tool_results(self) -> str:
        """{namespace}:pending:{task_id}:tools"""
        if self._pending_tool_results is None:
            raise ValueError(
                "pending_tool_results is not available - "
                "task_id was not provided during RedisKeys.format()"
            )
        return self._pending_tool_results

    @property
    def pending_child_task_results(self) -> str:
        """{namespace}:pending:{task_id}:children"""
        if self._pending_child_task_results is None:
            raise ValueError(
                "pending_child_task_results is not available - "
                "task_id was not provided during RedisKeys.format()"
            )
        return self._pending_child_task_results

    @property
    def pending_child_wait_ids(self) -> str:
        """{namespace}:pending:{task_id}:children_wait_ids"""
        if self._pending_child_wait_ids is None:
            raise ValueError(
                "pending_child_wait_ids is not available - "
                "task_id was not provided during RedisKeys.format()"
            )
        return self._pending_child_wait_ids

    @property
    def hooks_by_task(self) -> str:
        """{namespace}:hooks:by_task:{task_id}"""
        if self._hooks_by_task is None:
            raise ValueError(
                "hooks_by_task is not available - "
                "task_id was not provided during RedisKeys.format()"
            )
        return self._hooks_by_task

    @property
    def hook_session_by_tool_call(self) -> str:
        """{namespace}:hooks:session_by_tool_call:{task_id}"""
        if self._hook_session_by_tool_call is None:
            raise ValueError(
                "hook_session_by_tool_call is not available - "
                "task_id was not provided during RedisKeys.format()"
            )
        return self._hook_session_by_tool_call

    @property
    def hook_sessions_by_task(self) -> str:
        """{namespace}:hooks:sessions_by_task:{task_id}"""
        if self._hook_sessions_by_task is None:
            raise ValueError(
                "hook_sessions_by_task is not available - "
                "task_id was not provided during RedisKeys.format()"
            )
        return self._hook_sessions_by_task

    @property
    def hook_runtime_ready(self) -> str:
        """{namespace}:hooks:runtime_ready:{task_id}"""
        if self._hook_runtime_ready is None:
            raise ValueError(
                "hook_runtime_ready is not available - "
                "task_id was not provided during RedisKeys.format()"
            )
        return self._hook_runtime_ready

    @property
    def updates_channel(self) -> str:
        """{namespace}:updates:{owner_id}"""
        if self._updates_channel is None:
            raise ValueError(
                "updates_channel is not available - "
                "owner_id was not provided during RedisKeys.format()"
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
    ) -> "RedisKeys":
        return cls(
            # Global keys (namespace only)
            _task_status=TASK_STATUS.format(namespace=namespace),
            _task_agent=TASK_AGENT.format(namespace=namespace),
            _task_payload=TASK_PAYLOAD.format(namespace=namespace),
            _task_pickups=TASK_PICKUPS.format(namespace=namespace),
            _task_retries=TASK_RETRIES.format(namespace=namespace),
            _task_meta=TASK_META.format(namespace=namespace),
            _task_cancellations=TASK_CANCELLATIONS.format(namespace=namespace),
            _hooks_index=HOOKS_INDEX.format(namespace=namespace),
            _hooks_expiring=HOOKS_EXPIRING.format(namespace=namespace),
            _hook_idempotency=HOOK_IDEMPOTENCY.format(
                namespace=namespace,
                hook_id="{hook_id}",
                idempotency_key="{idempotency_key}",
            ),
            _resume_idempotency=RESUME_IDEMPOTENCY.format(
                namespace=namespace,
                source_task_id="{source_task_id}",
                idempotency_key="{idempotency_key}",
            ),
            _enqueue_idempotency=ENQUEUE_IDEMPOTENCY.format(
                namespace=namespace,
                owner_id="{owner_id}",
                agent="{agent}",
                idempotency_key="{idempotency_key}",
            ),
            _batch_enqueue_idempotency=BATCH_ENQUEUE_IDEMPOTENCY.format(
                namespace=namespace,
                owner_id="{owner_id}",
                agent="{agent}",
                idempotency_key="{idempotency_key}",
            ),
            _hook_sessions=HOOK_SESSIONS.format(namespace=namespace),
            _scheduled_wait_meta=SCHEDULED_WAIT_META.format(namespace=namespace),
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
            _queue_scheduled=QUEUE_SCHEDULED.format(namespace=namespace, agent=agent)
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
            # Metrics keys (rolling ring buffers)
            _agent_metrics_bucket=AGENT_ACTIVITY_METRICS.format(
                namespace=namespace, agent=agent
            )
            if agent
            else None,
            _global_metrics_bucket=GLOBAL_ACTIVITY_METRICS.format(namespace=namespace),
            # Batch-scoped keys
            _batch_meta=BATCH_META.format(namespace=namespace),
            _batch_tasks=BATCH_TASKS.format(namespace=namespace),
            _batch_remaining_tasks=BATCH_REMAINING_TASKS.format(namespace=namespace),
            _batch_progress=BATCH_PROGRESS.format(namespace=namespace),
            _batch_completed=BATCH_COMPLETED.format(namespace=namespace),
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
            _pending_child_wait_ids=PENDING_CHILD_WAIT_IDS.format(
                namespace=namespace,
                task_id=task_id,
            )
            if task_id
            else None,
            _hooks_by_task=HOOKS_BY_TASK.format(namespace=namespace, task_id=task_id)
            if task_id
            else None,
            _hook_session_by_tool_call=HOOK_SESSION_BY_TOOL_CALL.format(
                namespace=namespace, task_id=task_id
            )
            if task_id
            else None,
            _hook_sessions_by_task=HOOK_SESSIONS_BY_TASK.format(
                namespace=namespace, task_id=task_id
            )
            if task_id
            else None,
            _hook_runtime_ready=HOOK_RUNTIME_READY.format(
                namespace=namespace,
                task_id=task_id,
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
