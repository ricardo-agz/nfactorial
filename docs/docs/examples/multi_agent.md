# Multi-Agent Research

A complex research agent that can search the web and spin up multiple independent research sub-agents.

![Dashboard](../../static/img/multi-agent-progress.png)

## 1. Create the agent

`agent.py`

```python
import os
from typing import Any
from dotenv import load_dotenv
from factorial import Agent, AgentContext, gpt_41
from exa_py import Exa


load_dotenv()
exa = Exa(api_key=os.getenv("EXA_API_KEY"))


def search(query: str) -> tuple[str, list[dict[str, Any]]]:
    """Search the web for information"""
    result = exa.search_and_contents(
        query=query, num_results=10, text={"max_characters": 500}
    )
    data = [
        {"title": r.title, "url": r.url}
        for r in result.results
    ]

    return str(result), data

basic_agent = Agent(
    instructions="You are a helpful assistant. Always start by making a plan.",
    tools=[search],
    model=gpt_41,
)
```

The agent now has the ability to search the web.

## 2. Register the runner

`orchestrator.py`

```python
from factorial import Orchestrator, AgentWorkerConfig
from agent import basic_agent

orchestrator = Orchestrator(openai_api_key=os.getenv("OPENAI_API_KEY"))

orchestrator.register_runner(
    agent=basic_agent,
    agent_worker_config=AgentWorkerConfig(workers=50, turn_timeout=120),
)

if __name__ == "__main__":
    orchestrator.run()
```

`register_runner` spins up a pool of workers that pull tasks from Redis and drive the agent.

## 3. Subagents: spawn independent child tasks

Let's say we want to give our agent the ability to spin up multiple independent research subagents and wait for their results. In v2, use `subagents.spawn(...)` and `wait.jobs(...)`.

First, create the research subagent:

```python
from factorial import BaseModel

class SubAgentOutput(BaseModel):
    findings: list[str]

search_agent = Agent(
    name="research_subagent",
    description="Research Sub-Agent",
    model=gpt_41_mini,
    instructions="You are an intelligent research assistant.",
    tools=[plan, reflect, search],
    output_type=SubAgentOutput,
)
```

Register the subagent's runner in your orchestrator:

```python
orchestrator.register_runner(
    agent=search_agent,
    agent_worker_config=AgentWorkerConfig(workers=25, turn_timeout=120),
)
```

Now create a tool that spawns child tasks and blocks on their completion:

```python
from factorial import WaitInstruction, subagents, tool, wait

@tool
async def research(
    queries: list[str],
    agent_ctx: AgentContext,
) -> WaitInstruction:
    """Spawn child search tasks for each query and wait for completion."""

    payloads = [AgentContext(query=q) for q in queries]
    jobs = await subagents.spawn(
        agent=search_agent,
        inputs=payloads,
        key="research",
    )
    return wait.jobs(jobs, message="Waiting for research subagents")
```

**Key points:**

- `subagents.spawn(...)` is imperative fan-out (non-blocking by itself)
- `wait.jobs(...)` is declarative join (blocks until child jobs complete)
- The parent task pauses until all requested child jobs complete
- Child results are automatically injected back into the parent context flow

When the agent calls this tool, it:

1. Creates child tasks for each query
2. Returns a wait instruction for those jobs
3. Pauses execution of the parent task
4. Resumes when all children complete, with results in the conversation

## 4. Expose an API & WebSocket

`server.py`

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException
from pydantic import BaseModel
import json
from starlette.websockets import WebSocket, WebSocketDisconnect
from agent import basic_agent
from orchestrator import orchestrator

app = FastAPI()

@app.websocket("/ws/{user_id}")
async def websocket_updates(websocket: WebSocket, user_id: str):
    await websocket.accept()

    try:
        async for update in orchestrator.subscribe_to_updates(owner_id=user_id):
            await websocket.send_text(json.dumps(update))
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for user_id={user_id}")


class EnqueueRequest(BaseModel):
    user_id: str
    message_history: list[dict[str, str]]
    query: str

@app.post("/api/enqueue")
async def enqueue(request: EnqueueRequest):
    payload = AgentContext(
        messages=request.message_history,
        query=request.query,
    )
    task = await orchestrator.create_agent_task(
        agent=basic_agent,
        owner_id=request.user_id,
        payload=payload,
    )
    return {"task_id": task.id}


class SteerRequest(BaseModel):
    user_id: str
    task_id: str
    messages: list[dict[str, str]]

@app.post("/api/steer")
async def steer_task_endpoint(request: SteerRequest):
    try:
        await orchestrator.steer_task(
            task_id=request.task_id,
            messages=request.messages,
        )
        return {
            "success": True,
            "message": f"Steering messages sent for task {request.task_id}",
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


class CancelRequest(BaseModel):
    user_id: str
    task_id: str

@app.post("/api/cancel")
async def cancel_task_endpoint(request: CancelRequest):
    try:
        await orchestrator.cancel_task(task_id=request.task_id)
        return {
            "success": True,
            "message": f"Task {request.task_id} marked for cancellation",
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
```

## 5. Queue your first task

```bash
curl -X POST http://localhost:8000/api/enqueue \
  -H "Content-Type: application/json" \
  -d '{
        "user_id":"demo",
        "message_history":[],
        "query":"What is the capital of France?"
      }'
```

The response contains the `task_id`. Open the WebSocket at `ws://localhost:8000/ws/demo` to watch progress in real-time.

## 6. Steering & cancellation

```bash
# append a follow-up instruction
curl -X POST http://localhost:8000/api/steer \
  -d '{"user_id":"demo","task_id":"<id>","messages":[{"role":"user","content":"make it short"}]}'

# stop the task
curl -X POST http://localhost:8000/api/cancel \
  -d '{"user_id":"demo","task_id":"<id>"}'
```

`steer` publishes `run_steering_applied` / `run_steering_failed` events.  
`cancel` publishes `run_cancelled`.

## 7. Run everything

```bash
# run orchestrator
python orchestrator.py
# run api
python server.py
```
