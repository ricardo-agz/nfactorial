from dataclasses import dataclass, field
from typing import Literal

import redis.asyncio as redis
from redis.commands.core import AsyncScript

from factorial.core.utils import decode

from ._core import (
    LuaScriptContract,
    _decode_json_string_list,
    _execute_contract,
    get_cached_script,
)


class HookWakeScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="HookWakeScript.execute",
        key_fields=(
            "queue_main_key",
            "queue_pending_key",
            "queue_orphaned_key",
            "task_statuses_key",
            "task_agents_key",
            "task_payloads_key",
            "task_pickups_key",
            "task_retries_key",
            "task_metas_key",
            "hook_runtime_ready_key",
        ),
        arg_fields=("task_id", "tool_call_id", "session_id"),
    )

    async def execute(
        self,
        *,
        queue_main_key: str,
        queue_pending_key: str,
        queue_orphaned_key: str,
        task_statuses_key: str,
        task_agents_key: str,
        task_payloads_key: str,
        task_pickups_key: str,
        task_retries_key: str,
        task_metas_key: str,
        hook_runtime_ready_key: str,
        task_id: str,
        tool_call_id: str,
        session_id: str,
    ) -> tuple[bool, str]:
        result: tuple[int, str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return bool(result[0]), decode(result[1])


async def create_hook_wake_script(redis_client: redis.Redis) -> HookWakeScript:
    return get_cached_script(redis_client, "hook_wake", HookWakeScript)


@dataclass
class HookResolveScriptResult:
    decision: Literal["claimed", "pending", "replay", "conflict", "resolved"]
    task_resumed: bool | None
    request_hash: str


class HookResolveScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="HookResolveScript._execute",
        key_fields=("hook_resolution_key",),
        arg_fields=(
            "mode",
            "request_hash",
            "ttl_seconds",
            "hook_id",
            "task_id",
            "tool_call_id",
            "task_resumed_flag",
        ),
    )

    @staticmethod
    def _decode_task_resumed_flag(flag: str) -> bool | None:
        if flag == "1":
            return True
        if flag == "0":
            return False
        return None

    @staticmethod
    def _encode_task_resumed_flag(task_resumed: bool | None) -> str:
        if task_resumed is True:
            return "1"
        if task_resumed is False:
            return "0"
        return ""

    async def _execute(
        self,
        *,
        mode: Literal["claim", "finalize"],
        hook_resolution_key: str,
        request_hash: str,
        ttl_seconds: int,
        hook_id: str,
        task_id: str,
        tool_call_id: str,
        task_resumed: bool | None = None,
    ) -> HookResolveScriptResult:
        task_resumed_flag = self._encode_task_resumed_flag(task_resumed)
        execution_values = dict(locals())
        execution_values.pop("task_resumed", None)
        result: tuple[str | bytes, str | bytes, str | bytes] = await _execute_contract(
            self, self._CONTRACT, execution_values
        )
        decision = decode(result[0])
        if decision not in {"claimed", "pending", "replay", "conflict", "resolved"}:
            raise ValueError(
                "hook_resolve script returned unexpected decision "
                f"'{decision}'"
            )
        return HookResolveScriptResult(
            decision=decision,  # type: ignore[arg-type]
            task_resumed=self._decode_task_resumed_flag(decode(result[1])),
            request_hash=decode(result[2]),
        )

    async def claim(
        self,
        *,
        hook_resolution_key: str,
        request_hash: str,
        ttl_seconds: int,
        hook_id: str,
        task_id: str,
        tool_call_id: str,
    ) -> HookResolveScriptResult:
        return await self._execute(
            mode="claim",
            hook_resolution_key=hook_resolution_key,
            request_hash=request_hash,
            ttl_seconds=ttl_seconds,
            hook_id=hook_id,
            task_id=task_id,
            tool_call_id=tool_call_id,
        )

    async def finalize(
        self,
        *,
        hook_resolution_key: str,
        request_hash: str,
        ttl_seconds: int,
        hook_id: str,
        task_id: str,
        tool_call_id: str,
        task_resumed: bool,
    ) -> HookResolveScriptResult:
        return await self._execute(
            mode="finalize",
            hook_resolution_key=hook_resolution_key,
            request_hash=request_hash,
            ttl_seconds=ttl_seconds,
            hook_id=hook_id,
            task_id=task_id,
            tool_call_id=tool_call_id,
            task_resumed=task_resumed,
        )


async def create_hook_resolve_script(redis_client: redis.Redis) -> HookResolveScript:
    return get_cached_script(redis_client, "hook_resolve", HookResolveScript)


@dataclass
class MessagingGroupMutationScriptResult:
    decision: str
    member_task_ids: list[str] = field(default_factory=list)
    detail: str | None = None
    extra: str | None = None


def _parse_group_mutation_result(
    result: list[str | bytes],
) -> MessagingGroupMutationScriptResult:
    decision = decode(result[0]) if result else "error"
    member_task_ids: list[str] = []
    detail: str | None = None
    extra: str | None = None
    if len(result) > 1:
        second = decode(result[1])
        if second.startswith("["):
            parsed = _decode_json_string_list(second)
            if parsed:
                member_task_ids = parsed
            else:
                detail = second
        else:
            detail = second
    if len(result) > 2:
        extra = decode(result[2])
    return MessagingGroupMutationScriptResult(
        decision=decision,
        member_task_ids=member_task_ids,
        detail=detail,
        extra=extra,
    )


@dataclass
class MessagingSendScriptResult:
    decision: str
    thread_message_id: str | None = None
    global_message_id: str | None = None
    delivered_task_ids: list[str] = field(default_factory=list)
    skipped_inactive_task_ids: list[str] = field(default_factory=list)
    failed_task_ids: list[str] = field(default_factory=list)
    detail: str | None = None
    extra: str | None = None


def _parse_messaging_send_result(
    result: list[str | bytes],
) -> MessagingSendScriptResult:
    decision = decode(result[0]) if result else "error"
    if decision == "sent":
        return MessagingSendScriptResult(
            decision=decision,
            thread_message_id=decode(result[1]) if len(result) > 1 else None,
            global_message_id=decode(result[2]) if len(result) > 2 else None,
            delivered_task_ids=_decode_json_string_list(
                result[3] if len(result) > 3 else None
            ),
            skipped_inactive_task_ids=_decode_json_string_list(
                result[4] if len(result) > 4 else None
            ),
            failed_task_ids=_decode_json_string_list(
                result[5] if len(result) > 5 else None
            ),
        )
    return MessagingSendScriptResult(
        decision=decision,
        detail=decode(result[1]) if len(result) > 1 else None,
        extra=decode(result[2]) if len(result) > 2 else None,
    )


class MessagingGroupCreateScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="MessagingGroupCreateScript.execute",
        key_fields=(
            "task_metas_key",
            "group_meta_key",
            "group_members_key",
            "team_tasks_key",
        ),
        arg_fields=(
            "sender_task_id",
            "team_id",
            "group_name",
            "group_meta_json",
            "member_task_ids_json",
            "groups_by_task_key_template",
        ),
    )

    async def execute(
        self,
        *,
        task_metas_key: str,
        group_meta_key: str,
        group_members_key: str,
        team_tasks_key: str,
        sender_task_id: str,
        team_id: str,
        group_name: str,
        group_meta_json: str,
        member_task_ids_json: str,
        groups_by_task_key_template: str,
    ) -> MessagingGroupMutationScriptResult:
        result: list[str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return _parse_group_mutation_result(result)


async def create_messaging_group_create_script(
    redis_client: redis.Redis,
) -> MessagingGroupCreateScript:
    return get_cached_script(
        redis_client, "messaging_group_create", MessagingGroupCreateScript
    )


class MessagingGroupAddMembersScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="MessagingGroupAddMembersScript.execute",
        key_fields=(
            "task_metas_key",
            "group_meta_key",
            "group_members_key",
            "team_tasks_key",
        ),
        arg_fields=(
            "sender_task_id",
            "team_id",
            "group_name",
            "member_task_ids_json",
            "groups_by_task_key_template",
        ),
    )

    async def execute(
        self,
        *,
        task_metas_key: str,
        group_meta_key: str,
        group_members_key: str,
        team_tasks_key: str,
        sender_task_id: str,
        team_id: str,
        group_name: str,
        member_task_ids_json: str,
        groups_by_task_key_template: str,
    ) -> MessagingGroupMutationScriptResult:
        result: list[str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return _parse_group_mutation_result(result)


async def create_messaging_group_add_members_script(
    redis_client: redis.Redis,
) -> MessagingGroupAddMembersScript:
    return get_cached_script(
        redis_client,
        "messaging_group_add_members",
        MessagingGroupAddMembersScript,
    )


class MessagingGroupRemoveMembersScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="MessagingGroupRemoveMembersScript.execute",
        key_fields=(
            "task_metas_key",
            "group_meta_key",
            "group_members_key",
        ),
        arg_fields=(
            "sender_task_id",
            "team_id",
            "group_name",
            "member_task_ids_json",
            "groups_by_task_key_template",
        ),
    )

    async def execute(
        self,
        *,
        task_metas_key: str,
        group_meta_key: str,
        group_members_key: str,
        sender_task_id: str,
        team_id: str,
        group_name: str,
        member_task_ids_json: str,
        groups_by_task_key_template: str,
    ) -> MessagingGroupMutationScriptResult:
        result: list[str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return _parse_group_mutation_result(result)


async def create_messaging_group_remove_members_script(
    redis_client: redis.Redis,
) -> MessagingGroupRemoveMembersScript:
    return get_cached_script(
        redis_client,
        "messaging_group_remove_members",
        MessagingGroupRemoveMembersScript,
    )


class MessagingGroupSendScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="MessagingGroupSendScript.execute",
        key_fields=(
            "task_statuses_key",
            "task_agents_key",
            "task_metas_key",
            "group_meta_key",
            "group_members_key",
            "thread_history_key",
            "global_history_key",
            "message_seq_key",
            "activity_wait_meta_key",
            "scheduled_wait_meta_key",
            "team_tasks_key",
        ),
        arg_fields=(
            "sender_task_id",
            "team_id",
            "group_name",
            "content",
            "data_json",
            "metadata_json",
            "steering_key_template",
            "history_maxlen",
            "queue_main_key_template",
            "queue_pending_key_template",
            "queue_scheduled_key_template",
            "groups_by_task_key_template",
        ),
    )

    async def execute(
        self,
        *,
        task_statuses_key: str,
        task_agents_key: str,
        task_metas_key: str,
        group_meta_key: str,
        group_members_key: str,
        thread_history_key: str,
        global_history_key: str,
        message_seq_key: str,
        activity_wait_meta_key: str,
        scheduled_wait_meta_key: str,
        team_tasks_key: str,
        sender_task_id: str,
        team_id: str,
        group_name: str,
        content: str,
        data_json: str,
        metadata_json: str,
        steering_key_template: str,
        history_maxlen: int,
        queue_main_key_template: str,
        queue_pending_key_template: str,
        queue_scheduled_key_template: str,
        groups_by_task_key_template: str,
    ) -> MessagingSendScriptResult:
        result: list[str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return _parse_messaging_send_result(result)


async def create_messaging_group_send_script(
    redis_client: redis.Redis,
) -> MessagingGroupSendScript:
    return get_cached_script(
        redis_client, "messaging_group_send", MessagingGroupSendScript
    )


class MessagingHumanGroupSendScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="MessagingHumanGroupSendScript.execute",
        key_fields=(
            "task_statuses_key",
            "task_agents_key",
            "task_metas_key",
            "group_meta_key",
            "group_members_key",
            "thread_history_key",
            "global_history_key",
            "message_seq_key",
            "activity_wait_meta_key",
            "scheduled_wait_meta_key",
            "team_tasks_key",
        ),
        arg_fields=(
            "team_id",
            "group_name",
            "content",
            "data_json",
            "metadata_json",
            "steering_key_template",
            "history_maxlen",
            "queue_main_key_template",
            "queue_pending_key_template",
            "queue_scheduled_key_template",
            "groups_by_task_key_template",
            "from_owner_id",
            "from_task_id",
        ),
        optional_arg_fields=frozenset({"from_task_id"}),
    )

    async def execute(
        self,
        *,
        task_statuses_key: str,
        task_agents_key: str,
        task_metas_key: str,
        group_meta_key: str,
        group_members_key: str,
        thread_history_key: str,
        global_history_key: str,
        message_seq_key: str,
        activity_wait_meta_key: str,
        scheduled_wait_meta_key: str,
        team_tasks_key: str,
        team_id: str,
        group_name: str,
        content: str,
        data_json: str,
        metadata_json: str,
        steering_key_template: str,
        history_maxlen: int,
        queue_main_key_template: str,
        queue_pending_key_template: str,
        queue_scheduled_key_template: str,
        groups_by_task_key_template: str,
        from_owner_id: str,
        from_task_id: str | None = None,
    ) -> MessagingSendScriptResult:
        result: list[str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return _parse_messaging_send_result(result)


async def create_messaging_human_group_send_script(
    redis_client: redis.Redis,
) -> MessagingHumanGroupSendScript:
    return get_cached_script(
        redis_client,
        "messaging_human_group_send",
        MessagingHumanGroupSendScript,
    )


class MessagingDirectSendScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="MessagingDirectSendScript.execute",
        key_fields=(
            "task_statuses_key",
            "task_agents_key",
            "task_metas_key",
            "thread_history_key",
            "global_history_key",
            "message_seq_key",
            "activity_wait_meta_key",
            "scheduled_wait_meta_key",
        ),
        arg_fields=(
            "sender_task_id",
            "to_task_id",
            "team_id",
            "content",
            "data_json",
            "metadata_json",
            "steering_key_template",
            "history_maxlen",
            "queue_main_key_template",
            "queue_pending_key_template",
            "queue_scheduled_key_template",
        ),
    )

    async def execute(
        self,
        *,
        task_statuses_key: str,
        task_agents_key: str,
        task_metas_key: str,
        thread_history_key: str,
        global_history_key: str,
        message_seq_key: str,
        activity_wait_meta_key: str,
        scheduled_wait_meta_key: str,
        sender_task_id: str,
        to_task_id: str,
        team_id: str,
        content: str,
        data_json: str,
        metadata_json: str,
        steering_key_template: str,
        history_maxlen: int,
        queue_main_key_template: str,
        queue_pending_key_template: str,
        queue_scheduled_key_template: str,
    ) -> MessagingSendScriptResult:
        result: list[str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return _parse_messaging_send_result(result)


async def create_messaging_direct_send_script(
    redis_client: redis.Redis,
) -> MessagingDirectSendScript:
    return get_cached_script(
        redis_client, "messaging_direct_send", MessagingDirectSendScript
    )


class MessagingHumanDirectSendScript(AsyncScript):
    _CONTRACT = LuaScriptContract(
        script_name="MessagingHumanDirectSendScript.execute",
        key_fields=(
            "task_statuses_key",
            "task_agents_key",
            "task_metas_key",
            "thread_history_key",
            "global_history_key",
            "message_seq_key",
            "activity_wait_meta_key",
            "scheduled_wait_meta_key",
        ),
        arg_fields=(
            "to_task_id",
            "team_id",
            "content",
            "data_json",
            "metadata_json",
            "steering_key_template",
            "history_maxlen",
            "queue_main_key_template",
            "queue_pending_key_template",
            "queue_scheduled_key_template",
            "from_owner_id",
            "from_task_id",
        ),
        optional_arg_fields=frozenset({"from_task_id"}),
    )

    async def execute(
        self,
        *,
        task_statuses_key: str,
        task_agents_key: str,
        task_metas_key: str,
        thread_history_key: str,
        global_history_key: str,
        message_seq_key: str,
        activity_wait_meta_key: str,
        scheduled_wait_meta_key: str,
        to_task_id: str,
        team_id: str,
        content: str,
        data_json: str,
        metadata_json: str,
        steering_key_template: str,
        history_maxlen: int,
        queue_main_key_template: str,
        queue_pending_key_template: str,
        queue_scheduled_key_template: str,
        from_owner_id: str,
        from_task_id: str | None = None,
    ) -> MessagingSendScriptResult:
        result: list[str | bytes] = await _execute_contract(
            self, self._CONTRACT, locals()
        )
        return _parse_messaging_send_result(result)


async def create_messaging_human_direct_send_script(
    redis_client: redis.Redis,
) -> MessagingHumanDirectSendScript:
    return get_cached_script(
        redis_client,
        "messaging_human_direct_send",
        MessagingHumanDirectSendScript,
    )
