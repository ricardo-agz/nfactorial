import json

import pytest
import redis.asyncio as redis

from factorial.context import AgentContext
from factorial.exceptions import (
    MessagingGroupAlreadyExistsError,
    MessagingPermissionError,
    MessagingScopeError,
)
from factorial.queue.keys import RedisKeys
from factorial.queue.operations import (
    enqueue_task,
    messaging_groups_add_members,
    messaging_groups_create,
    messaging_groups_get,
    messaging_groups_list,
    messaging_groups_send,
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
        payload=AgentContext(query=query),
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
        payload=AgentContext(query="teammate"),
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
            payload=AgentContext(query=f"teammate-{idx}"),
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
        payload=AgentContext(query="teammate"),
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
        payload=AgentContext(query="non-member"),
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
        payload=AgentContext(query="to-add"),
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
        payload=AgentContext(query="stale-member"),
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
