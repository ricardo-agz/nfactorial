from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from factorial.logging import get_logger

if TYPE_CHECKING:
    from factorial.orchestrator import Orchestrator

logger = get_logger(__name__)


async def run_process_supervisor(orchestrator: Orchestrator) -> None:
    """Run the long-lived process worker supervisor for registered runners."""
    if not orchestrator.runners:
        logger.warning("No runners registered. Nothing to run.")
        return
    await orchestrator.start_workers(orchestrator.shutdown_event)


def run_process_supervisor_sync(orchestrator: Orchestrator) -> None:
    """Synchronous helper for environments without an existing event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_process_supervisor(orchestrator))
    finally:
        pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
        if pending_tasks:
            for task in pending_tasks:
                task.cancel()
            loop.run_until_complete(
                asyncio.gather(*pending_tasks, return_exceptions=True)
            )
        loop.close()


__all__ = ["run_process_supervisor", "run_process_supervisor_sync"]
