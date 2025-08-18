# Subagents

Subagents let an agent spawn one or more independent child tasks that run on other agents and report their results back to the parent. This is ideal when a subtask:

- Involves multiple steps or long running work on its own
- Produces context that is useful to the parent only as a summarized result
- Benefits from a specialized skillset/model separate from the parent

A good example of this would be a web research agent with a browsing tool that spins up a headless web browser. Since clicking and searching through the HTML contents of a website likely involves multiple steps and a lot of excess tokens from intermediate HTML content, it is useful to separate this process into a subagent of its own, whose task is narrowly defined as finding a specific answer or taking a specific action in a given website and reporting back to the main agent once completed.

Factorial provides first-class support for subagents via the `@forking_tool` decorator and the `ExecutionContext.spawn_child_task(s)` APIs. The parent task automatically pauses until all children complete, and their results are injected back into the parent conversation much like a regular tool call result.

## How it works (at a glance)

1. The parent agent calls a tool decorated with `@forking_tool(...)`.
2. Inside that tool you enqueue child tasks via `execution_ctx.spawn_child_task(...)` or `execution_ctx.spawn_child_tasks(...)`.
3. The tool returns the list of spawned child task IDs to the parent.
4. The parent task enters a `pending_child_tasks` state and pauses.
5. When each child finishes, its final output is stored and the parent resumes once all are done.
6. The parent receives an assistant message containing all child results, formatted for the LLM.

Result rendering in the parent looks like this:

```text
<sub_task_results>
<sub_task_result sub_task_id="..."> ...child output... </sub_task_result>
<sub_task_result sub_task_id="..."> ...child output... </sub_task_result>
</sub_task_results>
```

Errors from children are captured similarly using `<sub_task_error ...>` blocks.

## Createing a subagent

Define a dedicated agent for the subtask. You can give it its own tools, model, and (optionally) a structured `output_type` to ensure consistent results.

```python
from pydantic import BaseModel
from factorial import Agent, AgentContext, gpt_5


class SubAgentOutput(BaseModel):
    findings: list[str]


research_subagent = Agent(
    name="research_subagent",
    model=gpt_5,
    instructions="You are an efficient research assistant. Return concise findings.",
    tools=[...],  # e.g., search, scrape, browse, etc...
    output_type=SubAgentOutput,  # forces final_output tool usage with structured JSON
)


main_agent = Agent(
    name="main_agent",
    model=gpt_5,
    instructions="You are a helpful assistant. Use the research tool to delegate focused investigations.",
    tools=[...],  # e.g., search, research, etc...
)
```

## Implement a forking tool on the main agent

Mark a tool on the parent agent with `@forking_tool(timeout=...)` and use the `ExecutionContext` to spawn child tasks. The tool must return the child task IDs so the orchestrator knows to pause the parent until those child tasks finish.

### Batched child tasks

```python
from factorial import AgentContext, function_tool
from factorial.tools import forking_tool
from factorial.context import ExecutionContext


@forking_tool(timeout=600)
async def multi_research(
    queries: list[str],  #  list of independent research queries to be executed by concurrent subagents
    agent_ctx: AgentContext,
    execution_ctx: ExecutionContext,
) -> list[str]:
    """Spawn a one or multiple concurrent research subagents for each query and wait for results."""
    payloads = [AgentContext(query=q) for q in queries]
    batch = await execution_ctx.spawn_child_tasks(search_agent, payloads)

    # Return the list of child task IDs so the parent pauses and waits
    return batch.task_ids
```

### Single child task

```python
@forking_tool(timeout=300)
async def research(
    query: str,
    user_id: str,
    execution_ctx: ExecutionContext,
) -> list[str]:
    """Spawn a subagent to research the given query and wait for its result."""
    payload = AgentContext(query=query)
    child_id = await execution_ctx.spawn_child_task(search_agent, payload)
    return [child_id]
```

### Return shape requirements

The forking tool must return the child IDs in one of these shapes:

- A `list[str]` or `tuple[str, ...]` of task IDs
- A tuple of `(message: str, ids: list[str] | tuple[str, ...])`

If the IDs are missing or invalid, the call will raise.


## Registering runners (parent and subagent)

Register both the parent and the subagent in your orchestrator so the queue can assign work to each.

```python
from factorial import Orchestrator, AgentWorkerConfig
from agent import main_agent, search_agent

orchestrator = Orchestrator(openai_api_key=os.getenv("OPENAI_API_KEY"))

orchestrator.register_runner(
    agent=main_agent,
    agent_worker_config=AgentWorkerConfig(workers=50, turn_timeout=120),
)

orchestrator.register_runner(
    agent=research_subagent,
    agent_worker_config=AgentWorkerConfig(workers=25, turn_timeout=120),
)
```

## What the parent receives

When all child tasks complete, the parent is resumed with a new assistant message containing the formatted child results. You do not need to write any glue code—Factorial injects the results automatically via `BaseAgent.process_child_task_results()`.

Children can return either plain strings or structured outputs (if they used `output_type`). In both cases, the serialized result is embedded inside the `<sub_task_results>` block, so the LLM can read and reason over them in the next turn.

## Error handling and timeouts

- If a child task fails permanently, the parent resumes and receives an `<sub_task_error ...>` block for that child.
- The `timeout` on `@forking_tool` is the tool-level timeout. Child task processing, retries, and cancellation are still governed by the orchestrator/worker settings.
- Parent tasks remain paused only while there are outstanding child task results. If you cancel the parent, all remaining children are unlinked and the parent is cleaned up by the orchestrator.

## Tips

- Use a dedicated `output_type` on the subagent to return structured output results to the parent. 
- While it typically defeats the purpose of a subagent to inject the whole subagent trajectory context in the main agent's context, it may be helpful to incude a trajectory summary in the subagent's final output to the main agent.

## Complete example (parent + subagent)

For a full end‑to‑end demo that launches multiple research subagents and streams progress, see the [Multi‑Agent Research example](examples/multi_agent).


