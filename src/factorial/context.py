from dataclasses import dataclass
from typing import Any, TypeVar, Callable, Awaitable, TYPE_CHECKING
from contextvars import ContextVar
from pydantic import BaseModel
import asyncio
from factorial.events import EventPublisher

if TYPE_CHECKING:
    from factorial.agent import BaseAgent  # pragma: no cover

execution_context: ContextVar["ExecutionContext"] = ContextVar("execution_context")

ContextType = TypeVar("ContextType", bound="AgentContext")


@dataclass
class ExecutionContext:
    """Per-request context (not stored on agent)"""

    task_id: str
    owner_id: str
    retries: int
    iterations: int
    events: EventPublisher
    # Lightweight async callback injected by the orchestrator/worker that can be
    # used to enqueue child tasks.  It should accept the child agent instance
    # and its payload, and return the **task_id** of the newly created task.
    enqueue_child_task: (
        Callable[["BaseAgent[Any]", "ContextType"], Awaitable[str]] | None
    ) = None

    @classmethod
    def current(cls) -> "ExecutionContext":
        """Get current execution context"""
        return execution_context.get()

    async def spawn_child_task(
        self,
        agent: "BaseAgent[Any]",
        payload: "ContextType",
    ) -> str:
        """Enqueue a child task for *agent* with *payload*.

        This is a thin wrapper around the internal ``enqueue_child_task``
        callback injected by the worker.  It ensures the callback has been
        configured and forwards the call.  Returns the **task_id** of the
        created child task.
        """

        if self.enqueue_child_task is None:
            raise RuntimeError(
                "enqueue_child_task is not configured for this execution context"
            )

        return await self.enqueue_child_task(agent, payload)

    async def spawn_child_tasks(
        self,
        agent: "BaseAgent[Any]",
        payloads: list["ContextType"],
    ) -> list[str]:
        """Spawn multiple child tasks concurrently.

        A thin wrapper around *spawn_child_task* that launches all payloads in
        parallel using :pyfunc:`asyncio.gather` and returns the list of newly
        created task IDs in the same order as *payloads*.
        """

        coros = [self.spawn_child_task(agent, p) for p in payloads]
        return await asyncio.gather(*coros)


class AgentContext(BaseModel):
    """
    Agent state passed to the agent for turn execution.

    Base Fields:
    - query: str
    - messages: list[dict[str, Any]] = []
    - turn: int = 0
    - output: Any = None
    """

    query: str
    messages: list[dict[str, Any]] = []
    turn: int = 0
    output: Any = None

    class Config:
        extra = "allow"  # Users can add extra fields
        arbitrary_types_allowed = True  # For Any type flexibility

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        return cls(**data)

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str):
        return cls.model_validate_json(json_str)
