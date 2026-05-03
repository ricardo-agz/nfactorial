from __future__ import annotations

import pytest
import redis.asyncio as redis

from factorial.agent.context import AgentContext
from factorial.orchestrator import Orchestrator
from factorial.orchestrator.messaging import (
    DirectConversationListPage,
    DirectMessageHistoryPage,
    GroupConversationListPage,
    GroupMessageHistoryPage,
)
from factorial.queue.operations import (
    enqueue_task,
    messaging_groups_create,
    messaging_groups_send,
    messaging_send_direct,
)
from factorial.queue.task import Task

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
async def test_orchestrator_group_messaging_namespace_lists_and_reads_history(
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
        content="day one",
    )

    orchestrator = Orchestrator(
        redis_pool=redis_client.connection_pool,
        namespace=test_namespace,
    )
    conversation_page = await orchestrator.messaging.groups.list(
        team_id=team_id,
        limit=10,
    )
    assert isinstance(conversation_page, GroupConversationListPage)
    assert len(conversation_page.conversations) == 1
    summary = conversation_page.conversations[0]
    assert summary.group_id == created["group_id"]
    assert summary.group_name == "village"

    history_page = await orchestrator.messaging.groups.history(
        group_id=created["group_id"],
        limit=10,
    )
    assert isinstance(history_page, GroupMessageHistoryPage)
    assert history_page.group_name == "village"
    assert history_page.messages[0].content == "day one"


@pytest.mark.asyncio
async def test_orchestrator_direct_messaging_namespace_history_is_symmetric(
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

    await messaging_send_direct(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=task_a.id,
        to_task_id=task_b.id,
        content="hello",
    )
    await messaging_send_direct(
        redis_client=redis_client,
        namespace=test_namespace,
        sender_task_id=task_b.id,
        to_task_id=task_a.id,
        content="hi",
    )

    orchestrator = Orchestrator(
        redis_pool=redis_client.connection_pool,
        namespace=test_namespace,
    )
    history_ab = await orchestrator.messaging.direct.history(
        task_a_id=task_a.id,
        task_b_id=task_b.id,
        order="desc",
    )
    history_ba = await orchestrator.messaging.direct.history(
        task_a_id=task_b.id,
        task_b_id=task_a.id,
        order="desc",
    )
    assert isinstance(history_ab, DirectMessageHistoryPage)
    assert history_ab.thread_id == history_ba.thread_id
    assert [message.content for message in history_ab.messages[:2]] == ["hi", "hello"]

    direct_page = await orchestrator.messaging.direct.list(team_id=team_id, limit=10)
    assert isinstance(direct_page, DirectConversationListPage)
    assert len(direct_page.conversations) == 1
    assert direct_page.conversations[0].thread_id == history_ab.thread_id
