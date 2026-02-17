# Code Agent

An IDE-based agent that can write and edit code and *request the user* to execute code.

![Dashboard](../../static/img/code-agent.png)

## Recreating Cursor-style 'Waiting for approval...' tools

![Approval UI](/img/cursor-approve.png)

When we want the agent to do something that requires user approval we want to:

1. Prompt the user of the action we want to take
2. Put the agent in a pending idle state
3. Wait for user rejection / approval
4. Continue

## 1. Create the agent

We want to give the agent 3 tools, the ability to think/make a plan, the ability to write code, and the ability to request the user to execute code.

Note that `edit_code` is the only tool that does any real work in this example, think is simply a way to let the agent log its thoughts and
`request_code_execution` uses a hook approval flow that parks the task until the user approves or rejects via the
`/api/resolve_hook` endpoint.

`agent.py`

```python
import os
from typing import Annotated, Any
from dotenv import load_dotenv
from pydantic import BaseModel
from factorial import (
    BaseAgent,
    AgentContext,
    Hidden,
    Hook,
    HookRequestContext,
    PendingHook,
    gpt_41_mini,
    hook,
)


load_dotenv()


class IdeAgentContext(AgentContext):
    code: str


def think(thoughts: str) -> str:
    """Think deeply about the task and plan your next steps before executing"""
    return thoughts


class EditResult(BaseModel):
    summary: str
    new_code: Annotated[str, Hidden]


def edit_code(
    find: str,
    find_start_line: int,
    find_end_line: int,
    replace: str,
    agent_ctx: IdeAgentContext,
) -> EditResult:
    """
    Edit code in a file

    Arguments:
    find: The text to find and replace
    find_start_line: The start line number where the 'find' text is located
    find_end_line: The end line number where the 'find' text is located
    replace: The text to replace the 'find' text with
    """
    lines = agent_ctx.code.split("\n")
    start_idx = find_start_line - 1
    end_idx = find_end_line - 1
    if start_idx < 0 or end_idx >= len(lines) or start_idx > end_idx:
        raise ValueError(
            f"Line numbers out of range or invalid (total lines: {len(lines)})"
        )

    existing_text = "\n".join(lines[start_idx : end_idx + 1])

    # Check if the find text matches what's at those line numbers
    if find not in existing_text:
        raise ValueError(
            f"Text '{find}' not found at lines {find_start_line}-{find_end_line}. "
            f"Existing text: {existing_text}"
        )

    new_text = existing_text.replace(find, replace)
    new_lines = lines[:start_idx] + new_text.split("\n") + lines[end_idx + 1 :]

    # Update the agent context with the modified code
    agent_ctx.code = "\n".join(new_lines)

    return EditResult(
        summary=(
            f"Code successfully edited: replaced '{find}' with '{replace}' "
            f"at lines {find_start_line}-{find_end_line}"
        ),
        new_code=agent_ctx.code,
    )


class CodeExecutionApproval(Hook):
    approved: bool


def request_code_execution_approval(
    ctx: HookRequestContext,
) -> PendingHook[CodeExecutionApproval]:
    return CodeExecutionApproval.pending(
        ctx=ctx,
        title="Approve code execution",
        timeout_s=300.0,
    )


class CodeExecutionResult(BaseModel):
    summary: str
    approved: bool


async def request_code_execution(
    approval: Annotated[
        CodeExecutionApproval,
        hook.requires(request_code_execution_approval),
    ],
    agent_ctx: IdeAgentContext,
) -> CodeExecutionResult:
    """Request code execution behind an explicit approval hook."""
    if not approval.approved:
        return CodeExecutionResult(
            summary="User rejected the code execution request.",
            approved=False,
        )

    # In this example, execution can happen in your sandbox/runtime after approval.
    return CodeExecutionResult(
        summary="User approved code execution request.",
        approved=True,
    )


instructions = """
You are an IDE assistant that helps with coding tasks. You can write, read, analyze, and execute code. 
For anything non-trivial, always start by making a plan for the coding task.

The code will be displayed with line numbers in the following format. The code you write should not
contain line numbers.

[1]def hello_world():
[2]    print("Hello, world!")

In your final response, just clearly and consisely explain what you did without writing any code. 
The code changes will be shown to the user in a diff editor.
"""


class IDEAgent(BaseAgent[IdeAgentContext]):
    def __init__(self):
        super().__init__(
            context_class=IdeAgentContext,
            instructions=instructions,
            tools=[think, edit_code, request_code_execution],
            model=gpt_41_mini,
        )

    def prepare_messages(self, agent_ctx: IdeAgentContext) -> list[dict[str, Any]]:
        if agent_ctx.turn == 0:
            messages = [{"role": "system", "content": self.instructions}]
            if agent_ctx.messages:
                messages.extend(
                    [message for message in agent_ctx.messages if message["content"]]
                )

            code = "\n".join(
                [f"[{i + 1}]{line}" for i, line in enumerate(agent_ctx.code.split("\n"))]
            )
            messages.append(
                {
                    "role": "user",
                    "content": f"Code file with line numbers:\n{code}\n---\nQuery: {agent_ctx.query}",
                }
            )
        else:
            messages = agent_ctx.messages

        return messages


ide_agent = IDEAgent()
```

The agent now has the ability to think, write code, and request the user to execute the code.

## 2. Register the runner

`orchestrator.py`

```python
from factorial import Orchestrator, AgentWorkerConfig
from agent import ide_agent

orchestrator = Orchestrator(openai_api_key=os.getenv("OPENAI_API_KEY"))

orchestrator.register_runner(
    agent=ide_agent,
    agent_worker_config=AgentWorkerConfig(workers=50, turn_timeout=120),
)

if __name__ == "__main__":
    orchestrator.run()
```

`register_runner` spins up a pool of workers that pull tasks from Redis and drive the agent.

## 3. Expose an API & WebSocket

`server.py`

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException
from pydantic import BaseModel
import json
from starlette.websockets import WebSocket, WebSocketDisconnect
from agent import IdeAgentContext, ide_agent, orchestrator

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
    code: str

@app.post("/api/enqueue")
async def enqueue(request: EnqueueRequest):
    payload = IdeAgentContext(
        messages=request.message_history,
        query=request.query,
        turn=0,
        code=request.code,
    )
    task = await orchestrator.create_agent_task(
        agent=ide_agent,
        owner_id=request.user_id,
        payload=payload,
    )
    return {"task_id": task.id}


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


class ResolveHookRequest(BaseModel):
    hook_id: str
    token: str
    approved: bool
    idempotency_key: str | None = None


@app.post("/api/resolve_hook")
async def resolve_hook_endpoint(request: ResolveHookRequest):
    """Resolve a pending approval hook."""
    try:
        resolution = await orchestrator.resolve_hook(
            hook_id=request.hook_id,
            payload={"approved": request.approved},
            token=request.token,
            idempotency_key=request.idempotency_key,
        )
        return {
            "success": True,
            "status": resolution.status,
            "task_id": resolution.task_id,
            "tool_call_id": resolution.tool_call_id,
            "task_resumed": resolution.task_resumed,
        }
    except Exception as e:
        print(f"Failed to resolve hook {request.hook_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

```
