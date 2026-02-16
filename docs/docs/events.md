# Events

Agents run in the background so getting an agent output is not as simple as just writing `output = await Agent.run(...)`.
However, Factorial provides a way to publish and subscribe to agent events to stream progress and get the final agent output. 


To enqueue a task, you must pass in an _owner_id_, e.g.:
```python
@app.post("/api/enqueue")
async def enqueue(request: EnqueueRequest):
    task = basic_agent.create_task(
        owner_id=request.user_id,
        payload=AgentContext(
            messages=request.message_history,
            query=request.query,
        ),
    )

    await orchestrator.enqueue_task(agent=my_agent, task=task)
    return {"task_id": task.id}
```

This owner ID can then be used to subscribe to updates.

## Subscribing to Updates

The simplest way to subscribe to updates is using the orchestrator's built-in subscription method:

```python
@app.websocket("/ws/{user_id}")
async def websocket_updates(websocket: WebSocket, user_id: str):
    await websocket.accept()
    
    try:
        async for update in orchestrator.subscribe_to_updates(owner_id=user_id):
            await websocket.send_text(json.dumps(update))
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for user_id={user_id}")
```

### Filtering Updates

You can filter updates by task IDs, event types, or event patterns:

```python
# Only get completion and failure events
@app.websocket("/ws/{user_id}/final")
async def websocket_final_updates(websocket: WebSocket, user_id: str):
    await websocket.accept()
    
    try:
        async for update in orchestrator.subscribe_to_updates(
            owner_id=user_id,
            event_types=["run_completed", "run_failed", "run_cancelled"]
        ):
            await websocket.send_text(json.dumps(update))
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for user_id={user_id}")

# Only get updates for specific tasks
@app.websocket("/ws/{user_id}/tasks/{task_ids}")
async def websocket_task_updates(websocket: WebSocket, user_id: str, task_ids: str):
    await websocket.accept()
    task_id_list = task_ids.split(",")
    
    try:
        async for update in orchestrator.subscribe_to_updates(
            owner_id=user_id,
            task_ids=task_id_list
        ):
            await websocket.send_text(json.dumps(update))
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for user_id={user_id}")

# Get all progress events using regex pattern
@app.websocket("/ws/{user_id}/progress")
async def websocket_progress_updates(websocket: WebSocket, user_id: str):
    await websocket.accept()
    
    try:
        async for update in orchestrator.subscribe_to_updates(
            owner_id=user_id,
            event_pattern=r"progress_update_.*"
        ):
            await websocket.send_text(json.dumps(update))
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for user_id={user_id}")
```

**Available Filter Options:**
- `task_ids`: List of task IDs to filter for
- `event_types`: List of specific event types (exact match)
- `event_pattern`: Regex pattern to match against event types


### Manual Method

If you'd prefer to set up the redis connection yourself, you could also listen for updates like this:


```python
@app.websocket("/ws/{user_id}")
async def websocket_updates(websocket: WebSocket, user_id: str):
    await websocket.accept()
    pubsub: PubSub = redis_client.pubsub()
    channel = orchestrator.get_updates_channel(owner_id=user_id)
    print(
        f"WebSocket connection established for user_id={user_id}, subscribing to channel={channel}"
    )
    await pubsub.subscribe(channel)

    try:
        while True:
            msg: dict[str, Any] | None = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=WS_REDIS_SUB_TIMEOUT,
            )
            if msg and msg["type"] == "message":
                data = msg["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                await websocket.send_text(data)
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for user_id={user_id}")
    finally:
        await pubsub.unsubscribe(channel)  # type: ignore
        await pubsub.aclose()
        print(
            f"WebSocket cleanup completed for user_id={user_id}, unsubscribed from channel={channel}"
        )
```

<br/>
---


## Default Agent Events

Factorial automatically publishes several types of events during agent execution. All events include these base fields:

```python
{
    "event_type": str,       # e.g. 'run_started', 'run_failed', etc.
    "task_id": str,          
    "owner_id": str,         
    "timestamp": str,        # ISO timestamp
    "agent_name": str,       
    "metadata": dict | None  # Optional additional metadata
}
```

### Agent Output Events

#### `agent_output`
Published when an agent produces its final output.

```python
{
    "event_type": "agent_output",
    "task_id": "task-123",
    "owner_id": "user-456",
    "agent_name": "my_agent",
    "turn": 3,
    "timestamp": "2024-01-01T12:04:45Z",
    "data": "The weather in San Francisco is sunny with a temperature of 72°F"
}
```

### Agent Progress Events

Progress events are automatically published for key agent operations and follow the pattern `progress_update_{operation}_{status}`:

#### Completion Progress
- `progress_update_completion_started` - LLM completion request started
- `progress_update_completion_completed` - LLM completion request completed
- `progress_update_completion_failed` - LLM completion request failed

#### Tool Action Progress  
- `progress_update_tool_action_started` - Tool execution started
- `progress_update_tool_action_completed` - Tool execution completed
- `progress_update_tool_action_failed` - Tool execution failed

#### Turn Progress
- `progress_update_run_turn_started` - Agent turn started
- `progress_update_run_turn_completed` - Agent turn completed
- `progress_update_run_turn_failed` - Agent turn failed

Example progress event:
```python
{
    "event_type": "progress_update_completion_started",
    "task_id": "task-123",
    "owner_id": "user-456",
    "agent_name": "my_agent",
    "timestamp": "2024-01-01T12:02:00Z",
    "data": {
        "args": [...],
        "kwargs": {...},
        "context": {...}
    }
}
```

### Run Lifecycle Events

`run_started`

```python
{
    "event_type": "run_started",
    "task_id": "task-123",
    "owner_id": "user-456",
    "agent_name": "my_agent",
    "timestamp": "2024-01-01T12:00:00Z"
}
```

`run_completed`

```python
{
    "event_type": "run_completed",
    "task_id": "task-123",
    "owner_id": "user-456", 
    "agent_name": "my_agent",
    "timestamp": "2024-01-01T12:05:00Z"
}
```

`run_failed`

```python
{
    "event_type": "run_failed",
    "task_id": "task-123",
    "owner_id": "user-456",
    "agent_name": "my_agent",
    "timestamp": "2024-01-01T12:05:00Z",
    "error": "Agent my_agent failed to complete task task-123 due to max retries (3)"
}
```

`run_cancelled`

```python
{
    "event_type": "run_cancelled",
    "task_id": "task-123",
    "owner_id": "user-456",
    "agent_name": "my_agent",
    "timestamp": "2024-01-01T12:03:00Z"
}
```

### Queue Events

`task_failed`: Task failed to complete (but may still be retried).

```python
{
    "event_type": "task_failed",
    "task_id": "task-123",
    "owner_id": "user-456",
    "agent_name": "my_agent",
    "timestamp": "2024-01-01T12:02:00Z",
    "error": "Connection timeout"
}
```

`task_retried`: Task has been sent back to queue to be retried

```python
{
    "event_type": "task_retried",
    "task_id": "task-123",
    "owner_id": "user-456",
    "agent_name": "my_agent",
    "timestamp": "2024-01-01T12:02:00Z"
}
```

`task_pending_tool_call_results`: Task is waiting for pending tool call resolution (including hook-based approvals) and has been moved to the idle queue.

```python
{
    "event_type": "task_pending_tool_call_results",
    "task_id": "task-123",
    "owner_id": "user-456",
    "agent_name": "my_agent",
    "timestamp": "2024-01-01T12:02:30Z"
}
```

### Steering Events

#### `run_steering_applied`: Steering messages have been successfully applied to a task

```python
{
    "event_type": "run_steering_applied",
    "task_id": "task-123",
    "owner_id": "user-456",
    "agent_name": "my_agent",
    "timestamp": "2024-01-01T12:02:15Z"
}
```

#### `run_steering_failed`: Steering messages failed to be applied.

```python
{
    "event_type": "run_steering_failed",
    "task_id": "task-123",
    "owner_id": "user-456",
    "agent_name": "my_agent",
    "timestamp": "2024-01-01T12:02:15Z",
    "error": "Invalid steering message format"
}
```

<br/>
---

## Custom Agent Events

You can publish custom events from within your agent methods using the `ExecutionContext`:

### Publishing Custom Events

```python
from factorial import Agent, AgentContext, AgentEvent

class CustomAgent(Agent):
    async def run_turn(self, agent_ctx: AgentContext):
        execution_ctx = self.get_execution_context()
        
        await execution_ctx.events.publish_event(
            AgentEvent(
                event_type="my_custom_run_turn_started_event",
                task_id=execution_ctx.task_id,
                owner_id=execution_ctx.owner_id,
                agent_name=self.name,
                data={...}
            )
        )
        
        ...
```

### Using the Progress Decorator

For automatic progress tracking, you can use the `@publish_progress` decorator on your custom methods. 
This automatically publishes the following events:
* `progress_update_{func_name}_started`
* `progress_update_{func_name}_completed`
* `progress_update_{func_name}_failed`


```python
from factorial import Agent, AgentContext, AgentEvent

class CustomAgent(Agent):
    async def run_turn(self, agent_ctx: AgentContext):
        execution_ctx = self.get_execution_context()
        
        await execution_ctx.events.publish_event(
            AgentEvent(
                event_type="my_custom_run_turn_started_event",
                task_id=execution_ctx.task_id,
                owner_id=execution_ctx.owner_id,
                agent_name=self.name,
                data={...}
            )
        )
        
        ...

    @publish_progress
    async def do_some_work():
        print("doing some work"...)
```


### Custom Event Types

You can create your own event types by extending the base event classes:

```python
from dataclasses import dataclass
from factorial.events import AgentEvent

@dataclass
class CustomAnalysisEvent(AgentEvent):
    analysis_type: str
    confidence_score: float
    processing_time_ms: int

# Usage in agent
await execution_ctx.events.publish_event(
    CustomAnalysisEvent(
        event_type="sentiment_analysis_completed",
        task_id=execution_ctx.task_id,
        owner_id=execution_ctx.owner_id,
        agent_name=self.name,
        analysis_type="sentiment",
        confidence_score=0.95,
        processing_time_ms=1500
    )
)
```
