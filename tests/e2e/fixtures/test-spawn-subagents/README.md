# `test-spawn-subagents`

This fixture shows the intended authoring pattern for sandbox-backed runtime tests:

- `agents.py`: local agent definitions for the fixture
- `orchestrator.py`: how the fixture registers its agents
- `probes.py`: the acceptance contract for the fixture

The agents in this fixture use `factorial.testing.MockAgent`, so the fixture:

- uses the canonical `Agent(...)` runtime path,
- executes real tools, waits, subagent spawning, and orchestration, and
- avoids real model/provider costs by scripting completions with `responses=[...]`.

The fixture is intentionally small and deterministic. It proves that:

1. a parent task can spawn real child tasks,
2. the parent surfaces pending child task refs in the run event stream,
3. the child tasks complete, and
4. the parent resumes and finishes with the child results.

## MockAgent Shape

The fixture's `agents.py` uses the v1 testing API:

```python
child_agent = MockAgent(
    name="spawn_child",
    instructions="Return a deterministic child completion string.",
    responses=["child complete"],
)

parent_agent = MockAgent(
    name="spawn_parent",
    instructions="Spawn two child tasks and wait for them to complete.",
    tools=[spawn_children],
    responses=[
        tool_call("spawn_children", labels=["alpha", "beta"]),
        "joined 2 child tasks",
    ],
)
```

## Local Usage

Start Redis separately, then run:

```bash
python -m tests.e2e.serve_fixture tests/e2e/fixtures/test-spawn-subagents --port 8000
```

In another terminal:

```bash
python -m tests.e2e.run_probes tests/e2e/fixtures/test-spawn-subagents --base-url http://127.0.0.1:8000
```

In CI, the sandbox runner would do the same thing inside an isolated sandbox:

1. start `redis-server`
2. launch `serve_fixture`
3. wait for `/__probe/health`
4. execute `run_probes`
5. fail the job if any probe exits non-zero
