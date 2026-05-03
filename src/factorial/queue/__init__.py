from typing import Any

from factorial.queue.task import Task, TaskStatus

_LAZY_EXPORTS = {
    "worker_loop": ("factorial.queue.worker", "worker_loop"),
    "maintenance_loop": ("factorial.queue.maintenance", "maintenance_loop"),
    "enqueue_task": ("factorial.queue.operations", "enqueue_task"),
    "create_batch_and_enqueue": (
        "factorial.queue.operations",
        "create_batch_and_enqueue",
    ),
    "resume_task": ("factorial.queue.operations", "resume_task"),
    "cancel_task": ("factorial.queue.operations", "cancel_task"),
    "steer_task": ("factorial.queue.operations", "steer_task"),
    "signal_task": ("factorial.queue.operations", "signal_task"),
    "expire_pending_hooks": ("factorial.queue.operations", "expire_pending_hooks"),
    "messaging_groups_create": (
        "factorial.queue.operations",
        "messaging_groups_create",
    ),
    "messaging_groups_get": ("factorial.queue.operations", "messaging_groups_get"),
    "messaging_groups_list": ("factorial.queue.operations", "messaging_groups_list"),
    "messaging_groups_find": ("factorial.queue.operations", "messaging_groups_find"),
    "messaging_groups_history": (
        "factorial.queue.operations",
        "messaging_groups_history",
    ),
    "messaging_groups_list_threads": (
        "factorial.queue.operations",
        "messaging_groups_list_threads",
    ),
    "messaging_groups_add_members": (
        "factorial.queue.operations",
        "messaging_groups_add_members",
    ),
    "messaging_groups_remove_members": (
        "factorial.queue.operations",
        "messaging_groups_remove_members",
    ),
    "messaging_groups_leave": ("factorial.queue.operations", "messaging_groups_leave"),
    "messaging_groups_send": ("factorial.queue.operations", "messaging_groups_send"),
    "messaging_send_direct": ("factorial.queue.operations", "messaging_send_direct"),
    "messaging_direct_history": (
        "factorial.queue.operations",
        "messaging_direct_history",
    ),
    "messaging_direct_list_threads": (
        "factorial.queue.operations",
        "messaging_direct_list_threads",
    ),
    "messaging_inbox_direct_peek": (
        "factorial.queue.operations",
        "messaging_inbox_direct_peek",
    ),
    "messaging_inbox_direct_mark_read": (
        "factorial.queue.operations",
        "messaging_inbox_direct_mark_read",
    ),
    "messaging_inbox_group_peek": (
        "factorial.queue.operations",
        "messaging_inbox_group_peek",
    ),
    "messaging_inbox_group_mark_read": (
        "factorial.queue.operations",
        "messaging_inbox_group_mark_read",
    ),
    "messaging_inbox_receipts_peek": (
        "factorial.queue.operations",
        "messaging_inbox_receipts_peek",
    ),
    "messaging_inbox_receipts_mark_read": (
        "factorial.queue.operations",
        "messaging_inbox_receipts_mark_read",
    ),
    "messaging_human_send_direct": (
        "factorial.queue.operations",
        "messaging_human_send_direct",
    ),
    "messaging_human_send_group": (
        "factorial.queue.operations",
        "messaging_human_send_group",
    ),
    "register_pending_hook": ("factorial.queue.operations", "register_pending_hook"),
    "resolve_hook": ("factorial.queue.operations", "resolve_hook"),
    "rotate_hook_token": ("factorial.queue.operations", "rotate_hook_token"),
    "get_task_status": ("factorial.queue.task", "get_task_status"),
    "get_task_data": ("factorial.queue.task", "get_task_data"),
    "get_task_agent": ("factorial.queue.task", "get_task_agent"),
    "get_task_steering_messages": (
        "factorial.queue.task",
        "get_task_steering_messages",
    ),
}


def __getattr__(name: str) -> Any:
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module 'factorial.queue' has no attribute {name!r}")

    from importlib import import_module

    module_name, attr_name = _LAZY_EXPORTS[name]
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value

__all__ = [
    "worker_loop",
    "maintenance_loop",
    "Task",
    "TaskStatus",
    "enqueue_task",
    "create_batch_and_enqueue",
    "resume_task",
    "cancel_task",
    "steer_task",
    "signal_task",
    "expire_pending_hooks",
    "messaging_groups_create",
    "messaging_groups_get",
    "messaging_groups_list",
    "messaging_groups_find",
    "messaging_groups_history",
    "messaging_groups_list_threads",
    "messaging_groups_add_members",
    "messaging_groups_remove_members",
    "messaging_groups_leave",
    "messaging_groups_send",
    "messaging_send_direct",
    "messaging_direct_history",
    "messaging_direct_list_threads",
    "messaging_inbox_direct_peek",
    "messaging_inbox_direct_mark_read",
    "messaging_inbox_group_peek",
    "messaging_inbox_group_mark_read",
    "messaging_inbox_receipts_peek",
    "messaging_inbox_receipts_mark_read",
    "messaging_human_send_direct",
    "messaging_human_send_group",
    "register_pending_hook",
    "resolve_hook",
    "rotate_hook_token",
    "get_task_status",
    "get_task_data",
    "get_task_agent",
    "get_task_steering_messages",
]
