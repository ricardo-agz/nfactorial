from __future__ import annotations

import importlib
from importlib.resources import files


def test_public_modules_import() -> None:
    modules = [
        "factorial",
        "factorial.agent",
        "factorial.agent.agent",
        "factorial.agent.config",
        "factorial.orchestrator",
        "factorial.orchestrator.client",
        "factorial.orchestrator.config",
        "factorial.execution",
        "factorial.resources",
        "factorial.queue",
        "factorial.testing",
    ]

    for module in modules:
        importlib.import_module(module)


def test_typed_package_marker_is_present() -> None:
    marker = files("factorial").joinpath("py.typed")

    assert marker.is_file()


def test_agent_types_public_contract_excludes_internal_results() -> None:
    import factorial.agent.types as agent_types
    from factorial._internal.agent.types import ToolExecutionResults

    assert "ToolExecutionResults" not in agent_types.__all__
    assert not hasattr(agent_types, "ToolExecutionResults")
    assert ToolExecutionResults.__module__ == "factorial._internal.agent.types"


def test_queue_public_contract_excludes_store_helpers() -> None:
    import factorial.queue as queue
    import factorial.queue.task as queue_task

    assert queue.__all__ == ["Task", "TaskStatus"]
    assert queue.Task is queue_task.Task
    assert queue.TaskStatus is queue_task.TaskStatus

    compatibility_helpers = {
        "get_batch_data",
        "get_task_agent",
        "get_task_data",
        "get_task_status",
        "get_task_steering_messages",
        "task_team_id",
    }
    assert compatibility_helpers.isdisjoint(queue_task.__all__)


def test_legacy_config_import_paths_still_work() -> None:
    from factorial.orchestrator.config import TaskTTLConfig as ConfigTaskTTLConfig
    from factorial.orchestrator.core import TaskTTLConfig as CoreTaskTTLConfig

    assert CoreTaskTTLConfig is ConfigTaskTTLConfig
