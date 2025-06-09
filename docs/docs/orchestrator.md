# Orchestrator

The Orchestrator manages agent execution, scaling, and observability. It handles task queuing, worker management, and provides real-time monitoring.

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
    workers=10,              # Number of worker processes
    batch_size=25,           # Tasks per batch
    max_retries=5,           # Retry failed tasks
    heartbeat_interval=5,    # Heartbeat frequency (seconds)
    turn_timeout=120,        # Timeout per agent turn
)
```

### Maintenance Configuration

Configure task cleanup and recovery:

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
from factorial import AgentContext

async def submit_task():
    context = AgentContext(query="Analyze this data")
    task = agent.create_task(owner_id="user123", payload=context)
    
    await orchestrator.enqueue_task(agent, task)
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

### Get Task Results

```python
async def get_results(task_id: str):
    task_data = await orchestrator.get_task_data(task_id)
    if task_data:
        print(f"Result: {task_data}")

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

## Scaling

### Horizontal Scaling

Run multiple orchestrator instances:

```python
# Instance 1
orchestrator1 = Orchestrator(redis_host="redis-cluster")
orchestrator1.register_runner(agent, worker_config)
orchestrator1.run()

# Instance 2
orchestrator2 = Orchestrator(redis_host="redis-cluster")
orchestrator2.register_runner(agent, worker_config)
orchestrator2.run()
```

### Load Balancing

Tasks are automatically distributed across available workers using Redis queues.

### Resource Management

```python
# High-throughput configuration
config = AgentWorkerConfig(
    workers=50,              # More workers
    batch_size=15,           # Smaller batches for faster response
    heartbeat_interval=2,    # Frequent heartbeats
    turn_timeout=60,         # Shorter timeouts
)

# Memory-optimized configuration
config = AgentWorkerConfig(
    workers=5,               # Fewer workers
    batch_size=50,           # Larger batches
    heartbeat_interval=10,   # Less frequent heartbeats
    turn_timeout=300,        # Longer timeouts
)
```

## Production Deployment

### Environment Variables

```bash
export REDIS_HOST=redis.example.com
export REDIS_PORT=6379
export OPENAI_API_KEY=sk-...
export XAI_API_KEY=xai-...
```

### Docker Deployment

```dockerfile
FROM python:3.11-slim

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "main.py"]
```

### Health Checks

The orchestrator exposes health endpoints:

- `GET /` - Basic health check
- `GET /observability` - Dashboard
- WebSocket `/observability/ws` - Real-time updates 