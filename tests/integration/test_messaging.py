import json
from typing import Any

import pytest
import redis.asyncio as redis

from factorial.agent.context import AgentContext
from factorial.core.exceptions import (
    MessagingGroupAlreadyExistsError,
    MessagingPermissionError,
    MessagingScopeError,
)
from factorial.queue.keys import RedisKeys
from factorial.queue.operations import (
    enqueue_task,
    messaging_direct_history,
    messaging_direct_list_threads,
    messaging_groups_add_members,
    messaging_groups_create,
    messaging_groups_get,
    messaging_groups_history,
    messaging_groups_leave,
    messaging_groups_list,
    messaging_groups_list_threads,
    messaging_groups_remove_members,
    messaging_groups_send,
    messaging_human_send_direct,
    messaging_human_send_group,
    messaging_inbox_direct_mark_read,
    messaging_inbox_direct_peek,
    messaging_inbox_group_mark_read,
    messaging_inbox_group_peek,
    messaging_inbox_receipts_mark_read,
    messaging_inbox_receipts_peek,
    messaging_send_direct,
)
from factorial.queue.task import Task, TaskStatus

from .conftest import SimpleTestAgent


async def _enqueue_root_task(
    *,
    redis_client: redis.Redis,
    namespace: str,
    agent: SimpleTestAgent,
    owner_id: str,
    query: str,
) -> Task[AgentContext]:
    task = Task.create(
        owner_id=owner_id,
        agent=agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": query}]),
    )
    await enqueue_task(
        redis_client=redis_client,
        namespace=namespace,
        agent=agent,
        task=task,
    )
    return task


@pytest.mark.asyncio
async def test_group_create_get_and_list_are_team_scoped(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    team_id = sender.metadata.team_id or sender.id

    teammate = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "teammate"}]),
    )
    teammate.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=teammate,
    )

    created = await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        member_task_ids=[teammate.id],
    )
    assert created["group_name"] == "research"
    assert created["team_id"] == team_id
    assert set(created["member_task_ids"]) == {sender.id, teammate.id}

    got = await messaging_groups_get(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
    )
    assert got["group_name"] == "research"
    assert got["team_id"] == team_id
    assert set(got["member_task_ids"]) == {sender.id, teammate.id}

    sender_groups = await messaging_groups_list(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
    )
    teammate_groups = await messaging_groups_list(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=teammate.id,
    )
    assert [entry["group_name"] for entry in sender_groups] == ["research"]
    assert [entry["group_name"] for entry in teammate_groups] == ["research"]


@pytest.mark.asyncio
async def test_group_name_conflicts_within_team_only(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender_a = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender-a",
    )
    sender_b = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender-b",
    )

    await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender_a.id,
        group_name="research",
    )
    with pytest.raises(MessagingGroupAlreadyExistsError):
        await messaging_groups_create(
            redis_client=redis_client,
            namespace=test_namespace,
            sender_task_id=sender_a.id,
            group_name="research",
        )

    # Same name in a different team is valid.
    created_b = await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender_b.id,
        group_name="research",
    )
    assert created_b["team_id"] == (sender_b.metadata.team_id or sender_b.id)


@pytest.mark.asyncio
async def test_group_send_fanout_and_history_persistence(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    team_id = sender.metadata.team_id or sender.id

    member_ids: list[str] = []
    for idx in range(2):
        teammate = Task.create(
            owner_id=test_owner_id,
            agent=test_agent.name,
            payload=AgentContext(
                messages=[{"role": "user", "content": f"teammate-{idx}"}]
            ),
        )
        teammate.metadata.team_id = team_id
        await enqueue_task(
            redis_client=redis_client,
            namespace=test_namespace,
            agent=test_agent,
            task=teammate,
        )
        member_ids.append(teammate.id)

    await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        member_task_ids=member_ids,
    )
    report = await messaging_groups_send(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        content="share findings",
        metadata={"stage": "kickoff"},
    )

    assert set(report["delivered_task_ids"]) == set(member_ids)
    assert report["skipped_inactive_task_ids"] == []
    assert report["failed_task_ids"] == []

    for member_id in member_ids:
        member_keys = RedisKeys.format(namespace=test_namespace, task_id=member_id)
        steering = await redis_client.hgetall(member_keys.task_steering)  # type: ignore[misc]
        assert len(steering) == 1
        payload = json.loads(next(iter(steering.values())))
        assert "<peer_message kind='group'" in payload["content"]
        assert "share findings" in payload["content"]

    sender_keys = RedisKeys.format(namespace=test_namespace, task_id=sender.id)
    sender_steering = await redis_client.hgetall(sender_keys.task_steering)  # type: ignore[misc]
    assert sender_steering == {}

    keys = RedisKeys.format(namespace=test_namespace)
    thread_key = keys.messaging_thread_history(f"group:{team_id}:research")
    history_entries = await redis_client.xrange(thread_key, "-", "+")  # type: ignore[misc]
    assert len(history_entries) == 1
    history_payload = json.loads(history_entries[0][1]["payload"])
    assert history_payload["team_id"] == team_id
    assert history_payload["group_name"] == "research"
    assert set(history_payload["to_task_ids"]) == set(member_ids)


@pytest.mark.asyncio
async def test_direct_send_rejects_cross_team_messages(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    outsider = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="outsider",
    )
    assert (sender.metadata.team_id or sender.id) != (
        outsider.metadata.team_id or outsider.id
    )

    with pytest.raises(MessagingScopeError):
        await messaging_send_direct(
            redis_client=redis_client,
            namespace=test_namespace,
            sender_task_id=sender.id,
            to_task_id=outsider.id,
            content="hello",
            metadata=None,
        )


@pytest.mark.asyncio
async def test_group_send_skips_inactive_members(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    team_id = sender.metadata.team_id or sender.id

    teammate = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "teammate"}]),
    )
    teammate.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=teammate,
    )

    await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        member_task_ids=[teammate.id],
    )

    keys = RedisKeys.format(namespace=test_namespace)
    await redis_client.hset(keys.task_status, teammate.id, TaskStatus.COMPLETED.value)  # type: ignore[misc]

    report = await messaging_groups_send(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        content="late ping",
    )

    assert report["delivered_task_ids"] == []
    assert report["failed_task_ids"] == []
    assert report["skipped_inactive_task_ids"] == [teammate.id]


@pytest.mark.asyncio
async def test_group_add_members_requires_sender_membership(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    team_id = sender.metadata.team_id or sender.id

    non_member = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "non-member"}]),
    )
    non_member.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=non_member,
    )

    to_add = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "to-add"}]),
    )
    to_add.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=to_add,
    )

    await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
    )
    with pytest.raises(MessagingPermissionError):
        await messaging_groups_add_members(
            redis_client=redis_client,
            namespace=test_namespace,
            sender_task_id=non_member.id,
            group_name="research",
            member_task_ids=[to_add.id],
        )

    added = await messaging_groups_add_members(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        member_task_ids=[to_add.id],
    )
    assert added == [to_add.id]
    got = await messaging_groups_get(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
    )
    assert to_add.id in got["member_task_ids"]


@pytest.mark.asyncio
async def test_group_remove_members_requires_sender_membership(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    team_id = sender.metadata.team_id or sender.id

    target = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "target"}]),
    )
    target.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=target,
    )

    non_member = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "non-member"}]),
    )
    non_member.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=non_member,
    )

    await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        member_task_ids=[target.id],
    )

    with pytest.raises(MessagingPermissionError):
        await messaging_groups_remove_members(
            redis_client=redis_client,
            namespace=test_namespace,
            sender_task_id=non_member.id,
            group_name="research",
            member_task_ids=[target.id],
        )


@pytest.mark.asyncio
async def test_group_remove_members_and_leave_update_membership(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    team_id = sender.metadata.team_id or sender.id

    teammate = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "teammate"}]),
    )
    teammate.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=teammate,
    )

    await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        member_task_ids=[teammate.id],
    )

    removed = await messaging_groups_remove_members(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        member_task_ids=[teammate.id, teammate.id],
    )
    assert removed == [teammate.id]

    sender_view = await messaging_groups_get(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
    )
    assert sender_view["member_task_ids"] == [sender.id]
    teammate_groups = await messaging_groups_list(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=teammate.id,
    )
    assert teammate_groups == []

    left = await messaging_groups_leave(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
    )
    assert left is True
    assert (
        await messaging_groups_list(
            redis_client=redis_client,
            namespace=test_namespace,
            sender_task_id=sender.id,
        )
    ) == []

    left_again = await messaging_groups_leave(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
    )
    assert left_again is False


@pytest.mark.asyncio
async def test_group_send_prunes_missing_members(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    team_id = sender.metadata.team_id or sender.id

    stale_member = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "stale-member"}]),
    )
    stale_member.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=stale_member,
    )

    await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        member_task_ids=[stale_member.id],
    )

    keys = RedisKeys.format(namespace=test_namespace)
    await redis_client.hdel(keys.task_status, stale_member.id)  # type: ignore[misc]
    await redis_client.hdel(keys.task_agent, stale_member.id)  # type: ignore[misc]
    await redis_client.hdel(keys.task_payload, stale_member.id)  # type: ignore[misc]
    await redis_client.hdel(keys.task_pickups, stale_member.id)  # type: ignore[misc]
    await redis_client.hdel(keys.task_retries, stale_member.id)  # type: ignore[misc]
    await redis_client.hdel(keys.task_meta, stale_member.id)  # type: ignore[misc]

    first_report = await messaging_groups_send(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        content="first send",
    )
    assert first_report["failed_task_ids"] == [stale_member.id]

    group_members_key = keys.messaging_group_members(team_id, "research")
    assert not await redis_client.sismember(  # type: ignore[misc]
        group_members_key,
        stale_member.id,
    )

    second_report = await messaging_groups_send(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        content="second send",
    )
    assert second_report["failed_task_ids"] == []


@pytest.mark.asyncio
async def test_human_direct_send_persists_history_and_steering(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    recipient = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="recipient",
    )

    report = await messaging_human_send_direct(
        redis_client=redis_client,
        namespace=test_namespace,
        owner_id=test_owner_id,
        to_task_id=recipient.id,
        content="please continue",
        metadata={"source": "dashboard"},
    )

    assert report["thread_id"] == f"human:{test_owner_id}:{recipient.id}"
    assert report["delivered_task_ids"] == [recipient.id]
    recipient_keys = RedisKeys.format(namespace=test_namespace, task_id=recipient.id)
    steering = await redis_client.hgetall(recipient_keys.task_steering)  # type: ignore[misc]
    assert len(steering) == 1
    steering_payload = json.loads(next(iter(steering.values())))
    assert "<peer_message kind='human_direct'" in steering_payload["content"]
    assert "please continue" in steering_payload["content"]

    keys = RedisKeys.format(namespace=test_namespace)
    history_entries = await redis_client.xrange(  # type: ignore[misc]
        keys.messaging_thread_history(report["thread_id"]),
        "-",
        "+",
    )
    assert len(history_entries) == 1
    history_payload = json.loads(history_entries[0][1]["payload"])
    assert history_payload["kind"] == "human_direct"
    assert history_payload["from_owner_id"] == test_owner_id
    assert history_payload["to_task_ids"] == [recipient.id]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target_mode"),
    ["group_id", "task_name", "team_name"],
)
async def test_human_group_send_supports_all_target_selectors(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
    target_mode: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    team_id = sender.metadata.team_id or sender.id

    teammate = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "teammate"}]),
    )
    teammate.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=teammate,
    )

    created = await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="research",
        member_task_ids=[teammate.id],
    )

    target_kwargs: dict[str, Any] = {}
    if target_mode == "group_id":
        target_kwargs["group_id"] = created["group_id"]
    elif target_mode == "task_name":
        target_kwargs["task_id"] = sender.id
        target_kwargs["group_name"] = "research"
    elif target_mode == "team_name":
        target_kwargs["team_id"] = team_id
        target_kwargs["group_name"] = "research"
    else:  # pragma: no cover - defensive for future parameter edits
        raise AssertionError(f"Unhandled target mode: {target_mode}")

    report = await messaging_human_send_group(
        redis_client=redis_client,
        namespace=test_namespace,
        owner_id=test_owner_id,
        content="team update",
        metadata={"source": "operator"},
        **target_kwargs,
    )

    assert report["group_id"] == created["group_id"]
    assert set(report["delivered_task_ids"]) == {sender.id, teammate.id}
    assert report["skipped_inactive_task_ids"] == []
    assert report["failed_task_ids"] == []

    for member_id in (sender.id, teammate.id):
        member_keys = RedisKeys.format(namespace=test_namespace, task_id=member_id)
        steering = await redis_client.hgetall(member_keys.task_steering)  # type: ignore[misc]
        assert len(steering) == 1
        payload = json.loads(next(iter(steering.values())))
        assert "<peer_message kind='human_group'" in payload["content"]
        assert "team update" in payload["content"]

    keys = RedisKeys.format(namespace=test_namespace)
    history_entries = await redis_client.xrange(  # type: ignore[misc]
        keys.messaging_thread_history(report["thread_id"]),
        "-",
        "+",
    )
    assert len(history_entries) == 1
    history_payload = json.loads(history_entries[0][1]["payload"])
    assert history_payload["kind"] == "human_group"
    assert history_payload["from_owner_id"] == test_owner_id
    assert set(history_payload["to_task_ids"]) == {sender.id, teammate.id}


@pytest.mark.asyncio
async def test_group_history_and_thread_list_support_pagination(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    team_id = sender.metadata.team_id or sender.id

    teammate = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "teammate"}]),
    )
    teammate.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=teammate,
    )

    created = await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="village",
        member_task_ids=[teammate.id],
    )
    await messaging_groups_send(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="village",
        content="first",
    )
    await messaging_groups_send(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="village",
        content="second",
    )

    latest_page = await messaging_groups_history(
        redis_client=redis_client,
        namespace=test_namespace,
        team_id=team_id,
        group_name="village",
        limit=1,
        order="desc",
    )
    assert latest_page["group_id"] == created["group_id"]
    assert latest_page["has_more"] is True
    assert latest_page["messages"][0]["content"] == "second"
    assert isinstance(latest_page["next_before"], str)

    older_page = await messaging_groups_history(
        redis_client=redis_client,
        namespace=test_namespace,
        group_id=created["group_id"],
        limit=1,
        before=latest_page["next_before"],
        order="desc",
    )
    assert older_page["messages"][0]["content"] == "first"

    thread_list = await messaging_groups_list_threads(
        redis_client=redis_client,
        namespace=test_namespace,
        team_id=team_id,
        limit=10,
    )
    assert thread_list["has_more"] is False
    assert len(thread_list["conversations"]) == 1
    conversation = thread_list["conversations"][0]
    assert conversation["team_id"] == team_id
    assert conversation["group_id"] == created["group_id"]
    assert conversation["group_name"] == "village"
    assert conversation["thread_id"] == f"group:{team_id}:village"
    assert conversation["last_message_preview"] == "second"
    assert isinstance(conversation["last_message_id"], str)
    assert isinstance(conversation["last_message_at"], float)


@pytest.mark.asyncio
async def test_direct_history_and_thread_list_are_symmetric(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    task_a = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="task-a",
    )
    team_id = task_a.metadata.team_id or task_a.id

    task_b = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "task-b"}]),
    )
    task_b.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=task_b,
    )

    task_c = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "task-c"}]),
    )
    task_c.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=task_c,
    )

    await messaging_send_direct(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=task_a.id,
        to_task_id=task_b.id,
        content="alpha",
    )
    await messaging_send_direct(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=task_b.id,
        to_task_id=task_a.id,
        content="beta",
    )
    await messaging_send_direct(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=task_a.id,
        to_task_id=task_c.id,
        content="gamma",
    )

    history_ab = await messaging_direct_history(
        redis_client=redis_client,
        namespace=test_namespace,
        task_a_id=task_a.id,
        task_b_id=task_b.id,
        order="desc",
    )
    history_ba = await messaging_direct_history(
        redis_client=redis_client,
        namespace=test_namespace,
        task_a_id=task_b.id,
        task_b_id=task_a.id,
        order="desc",
    )
    assert history_ab["thread_id"] == history_ba["thread_id"]
    assert history_ab["task_a_id"] == history_ba["task_a_id"]
    assert history_ab["task_b_id"] == history_ba["task_b_id"]
    assert [entry["content"] for entry in history_ab["messages"][:2]] == [
        "beta",
        "alpha",
    ]

    first_page = await messaging_direct_list_threads(
        redis_client=redis_client,
        namespace=test_namespace,
        team_id=team_id,
        limit=1,
    )
    assert first_page["has_more"] is True
    assert isinstance(first_page["next_cursor"], str)
    second_page = await messaging_direct_list_threads(
        redis_client=redis_client,
        namespace=test_namespace,
        team_id=team_id,
        limit=1,
        cursor=first_page["next_cursor"],
    )
    assert second_page["conversations"]
    first_thread_id = first_page["conversations"][0]["thread_id"]
    second_thread_id = second_page["conversations"][0]["thread_id"]
    assert first_thread_id != second_thread_id


@pytest.mark.asyncio
async def test_direct_inbox_peek_mark_read_and_receipts_flow(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    team_id = sender.metadata.team_id or sender.id

    recipient = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "recipient"}]),
    )
    recipient.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=recipient,
    )

    await messaging_send_direct(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        to_task_id=recipient.id,
        content="day vote",
        data={"target": "task-z"},
        metadata={"round": 1},
    )

    recipient_page = await messaging_inbox_direct_peek(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=recipient.id,
        unread_only=True,
        limit=10,
    )
    assert len(recipient_page["messages"]) == 1
    recipient_message = recipient_page["messages"][0]
    assert recipient_message["content"] == "day vote"
    assert recipient_message["data"] == {"target": "task-z"}
    assert recipient_message["is_read"] is False

    mark_result = await messaging_inbox_direct_mark_read(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=recipient.id,
        message_ids=[recipient_message["message_id"]],
        notify_sender=True,
        data={"accepted": True},
    )
    assert mark_result["marked_message_ids"] == [recipient_message["message_id"]]
    assert mark_result["receipt_ids"]

    recipient_unread_after = await messaging_inbox_direct_peek(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=recipient.id,
        unread_only=True,
        limit=10,
    )
    assert recipient_unread_after["messages"] == []

    sender_receipts = await messaging_inbox_receipts_peek(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=sender.id,
        unread_only=True,
        limit=10,
    )
    assert len(sender_receipts["messages"]) == 1
    receipt = sender_receipts["messages"][0]
    assert receipt["source_message_id"] == recipient_message["message_id"]
    assert receipt["reader_task_id"] == recipient.id
    assert receipt["data"] == {"accepted": True}

    receipts_marked = await messaging_inbox_receipts_mark_read(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=sender.id,
        receipt_ids=[receipt["receipt_id"]],
    )
    assert receipts_marked["marked_receipt_ids"] == [receipt["receipt_id"]]


@pytest.mark.asyncio
async def test_group_inbox_peek_mark_read_and_data_roundtrip(
    redis_client: redis.Redis,
    test_namespace: str,
    test_agent: SimpleTestAgent,
    test_owner_id: str,
) -> None:
    sender = await _enqueue_root_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        owner_id=test_owner_id,
        query="sender",
    )
    team_id = sender.metadata.team_id or sender.id

    teammate = Task.create(
        owner_id=test_owner_id,
        agent=test_agent.name,
        payload=AgentContext(messages=[{"role": "user", "content": "teammate"}]),
    )
    teammate.metadata.team_id = team_id
    await enqueue_task(
        redis_client=redis_client,
        namespace=test_namespace,
        agent=test_agent,
        task=teammate,
    )

    await messaging_groups_create(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="village",
        member_task_ids=[teammate.id],
    )
    await messaging_groups_send(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=sender.id,
        group_name="village",
        content="lynch vote",
        data={"target": "task-w"},
        metadata={"round": 2},
    )

    unread_group = await messaging_inbox_group_peek(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=teammate.id,
        group_name="village",
        unread_only=True,
        limit=10,
    )
    assert len(unread_group["messages"]) == 1
    group_message = unread_group["messages"][0]
    assert group_message["content"] == "lynch vote"
    assert group_message["data"] == {"target": "task-w"}

    mark_result = await messaging_inbox_group_mark_read(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=teammate.id,
        group_name="village",
        message_ids=[group_message["message_id"]],
    )
    assert mark_result["marked_message_ids"] == [group_message["message_id"]]

    unread_after = await messaging_inbox_group_peek(
        redis_client=redis_client,
        namespace=test_namespace,
        task_id=teammate.id,
        group_name="village",
        unread_only=True,
        limit=10,
    )
    assert unread_after["messages"] == []
