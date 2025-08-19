# Orchestrator

The Orchestrator manages agent execution, scaling, and observability. It spawns concurrent instances of each agent resgistered with a runner and handles everything from enqueing agent tasks, to retries, to streaming real-time progress events. 

## Basic Setup

```python
from factorial import Orchestrator, AgentWorkerConfig, MaintenanceWorkerConfig

orchestrator = Orchestrator(
    redis_host="localhost",
    redis_port=6379,
    openai_api_key="sk-...",
)

# Register your agent
orchestrator.register_runner(
    agent=my_agent,
    agent_worker_config=AgentWorkerConfig(workers=10),
    maintenance_worker_config=MaintenanceWorkerConfig(),
)

# Start the system
orchestrator.run()
```

## Configuration

### Redis Configuration

```python
orchestrator = Orchestrator(
    redis_host="localhost",
    redis_port=6379,
    redis_db=0,
    redis_max_connections=50,
)
```

### Worker Configuration

Control how many workers process tasks:

```python
from factorial import AgentWorkerConfig

config = AgentWorkerConfig(
    workers=10,              # Number of concurrent async workers (coroutines)
    batch_size=25,           # Tasks processed per batch
    max_retries=5,           # How many times to requeue failed tasks
    heartbeat_interval=5,    # Heartbeat frequency (seconds)
    turn_timeout=120,        # Timeout for a single agent turn (seconds)
)
```

### Maintenance Worker Configuration

Maintenance workers clean up expired tasks and recover any failed tasks that have been dropped by a crashed worker.

```python
from factorial import MaintenanceWorkerConfig, TaskTTLConfig

config = MaintenanceWorkerConfig(
    interval=10,             # Maintenance check interval
    workers=1,               # Maintenance workers
    task_ttl=TaskTTLConfig(
        completed_ttl=3600,  # Keep completed tasks for 1 hour
        failed_ttl=86400,    # Keep failed tasks for 24 hours
        cancelled_ttl=1800,  # Keep cancelled tasks for 30 minutes
    ),
)
```

## Task Management

### Enqueue Tasks


```python
import asyncio
from factorial import Agent, AgentContext, Orchestrator

agent = Agent(...)
orchestrator = Orchestrator(...)
orchestrator.register_runner(...)

async def submit_task():
    task = await orchestrator.enqueue_task(
        agent=agent,
        owner_id="user123", 
        payload=AgentContext(query="Analyze this data")
    )
    
    return task.id

task_id = asyncio.run(submit_task())
```

### Check Task Status

```python
async def check_status(task_id: str):
    status = await orchestrator.get_task_status(task_id)
    print(f"Status: {status}")  # queued, processing, completed, failed, cancelled

asyncio.run(check_status(task_id))
```

### Get Task Data

```python
async def get_task_data(task_id: str):
    task_data = await orchestrator.get_task_data(task_id)
    if task_data:
        print(f"Data: {task_data}")

asyncio.run(get_task_data(task_id))
```

### Get Task Result

```python
async def get_task_result(task_id: str):
    task_data = await orchestrator.get_task_data(task_id)
    if task_data:
        result = task_data["payload"].get("output")
        print(f"Result: {result}")

asyncio.run(get_results(task_id))
```

### Cancel Tasks

```python
async def cancel_task(task_id: str):
    await orchestrator.cancel_task(task_id)

asyncio.run(cancel_task(task_id))
```

### Steer Tasks

Inject messages into running tasks:

```python
async def steer_task(task_id: str):
    messages = [
        {"role": "user", "content": "Please focus on the financial aspects"}
    ]
    await orchestrator.steer_task(task_id, messages)

asyncio.run(steer_task(task_id))
```

## Observability

### Dashboard

The orchestrator includes a built-in web dashboard:

```python
from factorial import ObservabilityConfig

orchestrator = Orchestrator(
    observability_config=ObservabilityConfig(
        enabled=True,
        host="0.0.0.0",
        port=8080,
        dashboard_name="My AI System",
    ),
)
```

Access at: http://localhost:8080/observability

### Metrics

Configure metrics collection:

```python
from factorial import MetricsTimelineConfig

config = MetricsTimelineConfig(
    timeline_duration=3600,      # 1 hour timeline
    bucket_size="minutes",       # Bucket by minutes
    retention_multiplier=2.0,    # Keep data for 2x timeline
)
```

<br/>

---


## Run Agents Without the Orchestrator

While there are a lot of benefits of executing agents through the orchestrator, for simpler use cases, it is possible to run an agent 'in-line', without needing an orchestrator, or its underlying Redis queue. This can be useful for local development or use cases where you simply don't want the overhead of a Redis dependency. 

A practical example of such a use case is the Factorial CLI agent. You can scaffold new agents with the `nfactorial create . "<agent description>"` command, which spawns a local agent to implement the code. Given this use case only requires a single agent running locally, the orchestrator and Redis are unnecessary overhead.


Agents expose a `run_inline` method that executes the agent locally in your process using the same lifecycle as queued runs, but without Redis or workers. You pass an `AgentContext` and optionally an `event_handler` to handle real-time progress events.

```python
import asyncio
from factorial import Agent, AgentContext


agent = Agent(
    instructions="You are a helpful assistant.",
    tools=[...],
    max_turns=10,
)

async def main():
    ctx = AgentContext(query="Summarize README.md")
    # Run the agent locally (no orchestrator/Redis required)
    completion = await agent.run_inline(ctx)

    # Access final output if your agent produces one
    print(completion.output)

asyncio.run(main())
```

### Progress streaming

You can provide an `event_handler` to receive structured progress events (turn start/end, tool start/end, completion chunks, failures, etc.). Here's a self-contained example that:
- shows a simple spinner during LLM thinking
- prints nicely formatted tool messages like "read 'path'", "created 'file'", "grep 'pattern' in 'dir'"

```python

def _extract_tool_data(event_data: dict) -> tuple[str | None, dict]:
    args = (event_data.get("args") or [])
    tool_name = None
    arguments = {}
    if args:
        call = args[0]
        func = (call.get("function") or {})
        tool_name = func.get("name")
        raw_args = func.get("arguments", {})
        try:
            if isinstance(raw_args, str):
                arguments = json.loads(raw_args)
            elif isinstance(raw_args, dict):
                arguments = raw_args
        except Exception:
            arguments = {}
    return tool_name, arguments


def _format_tool_message(tool_name: str | None, args: dict, result_data: dict | None) -> str:
    name = tool_name or "tool"
    if name == "read":
        return f"read '{args.get('file_path', 'unknown')}'"
    if name == "write":
        fp = args.get("file_path", "unknown")
        return ("overwrote" if args.get("overwrite") else "created") + f" '{fp}'"
    # ... more

    return f"ran {name}"


async def event_printer(event) -> None:
    event_type: str = getattr(event, "event_type", "")

    if event_type == "progress_update_completion_started":
        print("thinking...")
    elif event_type == "progress_update_tool_action_started":
        tool_name, tool_args = extract_tool_data(event.data)
        print(_format_tool_message(tool_name, tool_args))
    elif event_type.endswith("_failed"):
        error = getattr(event, "error", None)
        print(f"error: {error or getattr(event, 'data', {})}")


agent = Agent(
    instructions="You are a helpful coding agent.",
    tools=[read, write, edit_lines, find_replace, delete, tree, ls, grep],
    max_turns=60,
)

async def main():
    ctx = AgentContext(query="Add a /health route to this FastAPI service")
    completion = await agent.run_inline(ctx, event_handler=handle_event)

    print(completion.output)

if __name__ == "__main__":
    asyncio.run(main())

```

**Notes:**

- Pending tool or child-task results are not supported in in-line mode. If you need to run a subagent, you can do so by creating a tool that calls a subagent to run in_line inside the tool execution, which effectively accomplishes the same thing.

