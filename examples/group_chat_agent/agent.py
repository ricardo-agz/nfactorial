import os
from dataclasses import dataclass, field
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from factorial import (
    Agent,
    ExecutionContext,
    WaitInstruction,
    ai_gateway,
    any_of,
    gpt_41_mini,
    messaging,
    no_tool_calls,
    subagents,
    tool,
    turn_count_is,
    verify,
    wait,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env")
load_dotenv(env_path, override=True)

TEAM_GROUP_NAME = "team_room"


@dataclass
class DemoState:
    role_name: str = "parent"
    phase: str = "init"
    topic: str = ""
    group_name: str = TEAM_GROUP_NAME
    roster: dict[str, str] = field(default_factory=dict)
    group_member_task_ids: list[str] = field(default_factory=list)
    child_jobs: list[dict[str, Any]] = field(default_factory=list)
    dm_target_task_id: str | None = None
    wait_count: int = 0
    children_wait_requested: bool = False


class SpawnTeamResult(BaseModel):
    summary: str
    group_name: str
    roster: dict[str, str]
    child_task_ids: list[str]


class RosterResult(BaseModel):
    summary: str
    roster: dict[str, str]


class GroupPostResult(BaseModel):
    summary: str
    group_name: str
    message: str
    delivered_task_ids: list[str]
    skipped_inactive_task_ids: list[str]
    failed_task_ids: list[str]


class DirectPostResult(BaseModel):
    summary: str
    resolved_to_task_id: str
    message: str
    delivered_task_ids: list[str]
    skipped_inactive_task_ids: list[str]
    failed_task_ids: list[str]


class ParentFollowupResult(BaseModel):
    summary: str
    action: str
    resolved_to_task_id: str | None = None
    message: str | None = None
    delivered_task_ids: list[str] = Field(default_factory=list)
    skipped_inactive_task_ids: list[str] = Field(default_factory=list)
    failed_task_ids: list[str] = Field(default_factory=list)


class FinalOutput(BaseModel):
    final_output: str


def verify_parent_output(output: FinalOutput | str, *, agent_ctx):
    if isinstance(output, str):
        output = FinalOutput(final_output=output)
    text = output.final_output.strip()
    if not text:
        return verify.fail(
            message="Final output cannot be empty.",
            code="empty_output",
        )

    if agent_ctx.state.role_name != "parent":
        return verify.accept()

    if not agent_ctx.state.children_wait_requested:
        return verify.fail(
            message=(
                "Parent must wait for all subagents to complete before finalizing."
            ),
            code="children_wait_required",
        )

    expected_roles = sorted(role for role in agent_ctx.state.roster.keys() if role != "parent")

    lowered = text.lower()
    if "credit" not in lowered:
        return verify.fail(
            message="Include an explicit credits section in the final deliverable.",
            code="credits_required",
        )

    missing_mentions = [role for role in expected_roles if role not in lowered]
    if missing_mentions:
        return verify.fail(
            message=(
                "Final deliverable must credit every subagent by role name. "
                f"Missing mentions: {', '.join(missing_mentions)}."
            ),
            code="role_credit_missing",
            metadata={"missing_roles": missing_mentions},
        )

    return verify.accept()


async def _sync_group_membership(agent_ctx) -> dict[str, str]:
    group_state = await ExecutionContext.current().messaging.groups.get(
        agent_ctx.state.group_name
    )
    raw_members = group_state.get("member_task_ids", [])
    if not isinstance(raw_members, list):
        raise ValueError("Group member_task_ids must be a list")
    member_task_ids = sorted(
        {
            task_id.strip()
            for task_id in raw_members
            if isinstance(task_id, str) and task_id.strip()
        }
    )
    if not member_task_ids:
        raise ValueError("Group has no member task ids yet")

    parent_task_id = group_state.get("created_by_task_id")
    if not isinstance(parent_task_id, str) or not parent_task_id.strip():
        raise ValueError("Group created_by_task_id is missing")

    current_task_id = ExecutionContext.current().task_id
    if current_task_id not in member_task_ids:
        member_task_ids.append(current_task_id)
        member_task_ids.sort()

    roster = dict(agent_ctx.state.roster)
    roster["parent"] = parent_task_id.strip()
    roster[agent_ctx.state.role_name] = current_task_id
    agent_ctx.state.roster = roster
    agent_ctx.state.group_member_task_ids = member_task_ids

    if agent_ctx.state.role_name != "parent":
        candidate_child_ids = [
            task_id
            for task_id in member_task_ids
            if task_id not in {roster["parent"], current_task_id}
        ]
        if candidate_child_ids:
            candidate_child_ids.sort()
            ordered_child_ids = sorted(
                task_id
                for task_id in member_task_ids
                if task_id != roster["parent"]
            )
            if current_task_id in ordered_child_ids and len(ordered_child_ids) > 1:
                current_index = ordered_child_ids.index(current_task_id)
                target_index = (current_index + 1) % len(ordered_child_ids)
                target_task_id = ordered_child_ids[target_index]
                if target_task_id == current_task_id:
                    target_task_id = candidate_child_ids[0]
                agent_ctx.state.dm_target_task_id = target_task_id
            else:
                agent_ctx.state.dm_target_task_id = candidate_child_ids[0]
        else:
            agent_ctx.state.dm_target_task_id = None

    return agent_ctx.state.roster


def _resolve_dm_target(raw_target: str, roster: dict[str, str]) -> str:
    candidate = raw_target.strip()
    if not candidate:
        raise ValueError("to_task_id must be a non-empty string")
    if candidate in roster.values():
        return candidate
    if candidate in roster:
        return roster[candidate]

    lower_map = {role.lower(): task_id for role, task_id in roster.items()}
    lowered = candidate.lower()
    if lowered in lower_map:
        return lower_map[lowered]

    raise ValueError(
        f"Unknown recipient '{raw_target}'. Use a task_id or one of: "
        f"{', '.join(sorted(roster.keys()))}"
    )


@tool
async def spawn_team(topic: str, agent_ctx) -> SpawnTeamResult:
    """
    Parent-only: spawn researcher/skeptic/synthesizer subagents and create a team chat group.
    """
    if agent_ctx.state.role_name != "parent":
        raise ValueError("Only the parent coordinator can spawn the team.")
    if agent_ctx.state.roster:
        return SpawnTeamResult(
            summary="Team already exists in context.",
            group_name=agent_ctx.state.group_name,
            roster=agent_ctx.state.roster,
            child_task_ids=[
                task_id for role, task_id in agent_ctx.state.roster.items() if role != "parent"
            ],
        )

    normalized_topic = topic.strip() if isinstance(topic, str) else ""
    if not normalized_topic:
        normalized_topic = agent_ctx.query.strip() or "multi-agent coordination demo"

    researcher_state = DemoState(
        role_name="researcher",
        phase="start",
        topic=normalized_topic,
        group_name=agent_ctx.state.group_name,
    )
    skeptic_state = DemoState(
        role_name="skeptic",
        phase="start",
        topic=normalized_topic,
        group_name=agent_ctx.state.group_name,
    )
    synthesizer_state = DemoState(
        role_name="synthesizer",
        phase="start",
        topic=normalized_topic,
        group_name=agent_ctx.state.group_name,
    )

    researcher_payload = researcher_agent.build_context(
        input=f"Topic: {normalized_topic}",
        state=researcher_state,
    )
    skeptic_payload = skeptic_agent.build_context(
        input=f"Topic: {normalized_topic}",
        state=skeptic_state,
    )
    synthesizer_payload = synthesizer_agent.build_context(
        input=f"Topic: {normalized_topic}",
        state=synthesizer_state,
    )

    researcher_job = (
        await subagents.spawn(
            agent=researcher_agent,
            inputs=[researcher_payload],
            key=f"{normalized_topic}:researcher",
        )
    )[0]
    skeptic_job = (
        await subagents.spawn(
            agent=skeptic_agent,
            inputs=[skeptic_payload],
            key=f"{normalized_topic}:skeptic",
        )
    )[0]
    synthesizer_job = (
        await subagents.spawn(
            agent=synthesizer_agent,
            inputs=[synthesizer_payload],
            key=f"{normalized_topic}:synthesizer",
        )
    )[0]

    jobs = [researcher_job, skeptic_job, synthesizer_job]
    await messaging.groups.create(agent_ctx.state.group_name, members=jobs)

    parent_task_id = ExecutionContext.current().task_id
    roster = {
        "parent": parent_task_id,
        "researcher": researcher_job.task_id,
        "skeptic": skeptic_job.task_id,
        "synthesizer": synthesizer_job.task_id,
    }

    agent_ctx.state.topic = normalized_topic
    agent_ctx.state.roster = roster
    agent_ctx.state.group_member_task_ids = sorted(roster.values())
    agent_ctx.state.child_jobs = [j.to_dict() for j in jobs]
    agent_ctx.state.children_wait_requested = False
    agent_ctx.state.phase = "kickoff_group"

    return SpawnTeamResult(
        summary=(
            "Spawned 3 subagents, created a shared group channel, and stored "
            "the team roster."
        ),
        group_name=agent_ctx.state.group_name,
        roster=roster,
        child_task_ids=[
            researcher_job.task_id,
            skeptic_job.task_id,
            synthesizer_job.task_id,
        ],
    )


@tool
async def get_roster(agent_ctx) -> RosterResult | WaitInstruction:
    """
    Load parent/member information from messaging group state.
    """
    if agent_ctx.state.roster and agent_ctx.state.group_member_task_ids:
        if agent_ctx.state.phase == "start":
            agent_ctx.state.phase = "share_intro"
        return RosterResult(
            summary="Roster loaded from context.",
            roster=agent_ctx.state.roster,
        )

    try:
        roster = await _sync_group_membership(agent_ctx)
    except Exception:
        # Children may begin before the group exists; wait for parent kickoff activity.
        return wait.activity(
            data={
                "reason": "waiting_for_group_membership",
                "role_name": agent_ctx.state.role_name,
            }
        )

    if agent_ctx.state.phase == "start":
        agent_ctx.state.phase = "share_intro"

    return RosterResult(
        summary="Roster derived from messaging group membership.",
        roster=roster,
    )


@tool
async def post_group(message: str, agent_ctx) -> GroupPostResult:
    """
    Send a group message to the shared team room.
    """
    if not isinstance(message, str) or not message.strip():
        raise ValueError("message must be a non-empty string")

    final_message = message.strip()

    report = await messaging.groups.send(
        agent_ctx.state.group_name,
        final_message,
        metadata={
            "role_name": agent_ctx.state.role_name,
            "phase": agent_ctx.state.phase,
            "topic": agent_ctx.state.topic,
        },
    )

    if agent_ctx.state.role_name == "parent":
        if agent_ctx.state.phase == "kickoff_group":
            agent_ctx.state.phase = "wait_one"
        elif agent_ctx.state.phase in {"engage_optional_dm", "followup_group"}:
            agent_ctx.state.phase = "wait_two"
    else:
        if agent_ctx.state.phase == "share_intro":
            agent_ctx.state.phase = "send_dm"
        elif agent_ctx.state.phase in {"optional_parent_reply", "share_after_wake"}:
            agent_ctx.state.phase = "finalize"

    return GroupPostResult(
        summary=f"Posted message to #{agent_ctx.state.group_name}.",
        group_name=agent_ctx.state.group_name,
        message=final_message,
        delivered_task_ids=report.delivered_task_ids,
        skipped_inactive_task_ids=report.skipped_inactive_task_ids,
        failed_task_ids=report.failed_task_ids,
    )


@tool
async def post_dm(to_task_id: str, message: str, agent_ctx) -> DirectPostResult:
    """
    Send a direct message to exactly one teammate.
    """
    if not isinstance(message, str) or not message.strip():
        raise ValueError("message must be a non-empty string")

    if agent_ctx.state.role_name != "parent" and agent_ctx.state.phase == "send_dm":
        if not agent_ctx.state.group_member_task_ids:
            await _sync_group_membership(agent_ctx)
        if not agent_ctx.state.dm_target_task_id:
            raise ValueError(
                "No eligible teammate target found in group membership for child DM."
            )
        resolved_target = agent_ctx.state.dm_target_task_id
    else:
        if not agent_ctx.state.roster:
            await _sync_group_membership(agent_ctx)
        if not agent_ctx.state.roster:
            raise ValueError("No roster available yet. Call get_roster first.")
        resolved_target = _resolve_dm_target(to_task_id, agent_ctx.state.roster)
    report = await messaging.send(
        resolved_target,
        message.strip(),
        metadata={
            "role_name": agent_ctx.state.role_name,
            "phase": agent_ctx.state.phase,
            "topic": agent_ctx.state.topic,
        },
    )

    if agent_ctx.state.role_name == "parent":
        if agent_ctx.state.phase == "engage_optional_dm":
            agent_ctx.state.phase = "wait_two"
    elif agent_ctx.state.phase == "send_dm":
        agent_ctx.state.phase = "awaiting_activity"
    elif agent_ctx.state.phase == "optional_parent_reply":
        agent_ctx.state.phase = "share_after_wake"

    return DirectPostResult(
        summary=f"Direct message sent to {resolved_target}.",
        resolved_to_task_id=resolved_target,
        message=message.strip(),
        delivered_task_ids=report.delivered_task_ids,
        skipped_inactive_task_ids=report.skipped_inactive_task_ids,
        failed_task_ids=report.failed_task_ids,
    )


@tool
async def parent_followup_decision(
    should_send_dm: bool,
    agent_ctx,
    to_role: str | None = None,
    message: str | None = None,
) -> ParentFollowupResult:
    """
    Parent-only: optionally send one DM after first wake, then advance workflow.
    """
    if agent_ctx.state.role_name != "parent":
        raise ValueError("Only the parent coordinator can make this decision.")

    if not should_send_dm:
        agent_ctx.state.phase = "await_children_completion"
        return ParentFollowupResult(
            summary="Parent skipped optional DM and moved to child-completion wait.",
            action="skipped_dm",
        )

    if not agent_ctx.state.roster:
        raise ValueError("Roster is missing; spawn team and kickoff first.")

    raw_target = to_role.strip() if isinstance(to_role, str) else "researcher"
    if not raw_target:
        raw_target = "researcher"
    resolved_target = _resolve_dm_target(raw_target, agent_ctx.state.roster)

    dm_message = message.strip() if isinstance(message, str) else ""
    if not dm_message:
        dm_message = (
            "Please share your latest findings and what changed after peer feedback."
        )

    report = await messaging.send(
        resolved_target,
        dm_message,
        metadata={
            "role_name": agent_ctx.state.role_name,
            "phase": agent_ctx.state.phase,
            "topic": agent_ctx.state.topic,
            "followup_dm": True,
        },
    )
    agent_ctx.state.phase = "wait_two"

    return ParentFollowupResult(
        summary=f"Parent sent optional DM to {resolved_target}.",
        action="sent_dm",
        resolved_to_task_id=resolved_target,
        message=dm_message,
        delivered_task_ids=report.delivered_task_ids,
        skipped_inactive_task_ids=report.skipped_inactive_task_ids,
        failed_task_ids=report.failed_task_ids,
    )


@tool
def wait_for_activity(reason: str, agent_ctx) -> WaitInstruction:
    """
    Parent uses wait.activity; children use a short bounded pause to avoid deadlocks.
    """
    normalized_reason = reason.strip() if isinstance(reason, str) else ""
    agent_ctx.state.wait_count += 1

    if agent_ctx.state.role_name == "parent":
        if not normalized_reason:
            normalized_reason = "waiting_for_peer_activity"
        if agent_ctx.state.phase == "wait_one":
            agent_ctx.state.phase = "engage_optional_dm"
        elif agent_ctx.state.phase == "wait_two":
            agent_ctx.state.phase = "await_children_completion"
        return wait.activity(
            data={
                "reason": normalized_reason,
                "role_name": agent_ctx.state.role_name,
                "wait_count": agent_ctx.state.wait_count,
            }
        )

    # Children should never block indefinitely while parent is waiting on wait.jobs.
    if agent_ctx.state.phase == "awaiting_activity":
        agent_ctx.state.phase = "optional_parent_reply"
    if not normalized_reason:
        normalized_reason = "bounded_pause_before_optional_parent_reply"
    return wait.sleep(
        3,
        data={
            "reason": normalized_reason,
            "role_name": agent_ctx.state.role_name,
            "wait_count": agent_ctx.state.wait_count,
        },
    )


@tool
def wait_for_children_completion(agent_ctx) -> WaitInstruction:
    """
    Parent-only: block completion until all spawned children have completed.
    """
    if agent_ctx.state.role_name != "parent":
        raise ValueError("Only the parent coordinator can wait on child completion.")
    if not agent_ctx.state.child_jobs:
        parent_task_id = ExecutionContext.current().task_id
        rebuilt_jobs = [
            {
                "task_id": task_id,
                "agent_name": f"{role}_agent",
                "parent_task_id": parent_task_id,
                "key": f"{agent_ctx.state.topic}:{role}",
            }
            for role, task_id in agent_ctx.state.roster.items()
            if role != "parent"
        ]
        if not rebuilt_jobs:
            raise ValueError("No child jobs found in context. Call spawn_team first.")
        agent_ctx.state.child_jobs = rebuilt_jobs

    agent_ctx.state.children_wait_requested = True
    agent_ctx.state.phase = "finalize"
    return wait.jobs(
        agent_ctx.state.child_jobs,
        data={
            "reason": "waiting_for_all_children_to_complete",
            "role_name": agent_ctx.state.role_name,
            "expected_children": len(agent_ctx.state.child_jobs),
        },
    )


def _parent_tool_choice(agent_ctx) -> str | dict[str, Any]:
    phase = str(agent_ctx.state.phase)
    if phase == "init":
        return {"type": "function", "function": {"name": "spawn_team"}}
    if phase == "kickoff_group":
        return {"type": "function", "function": {"name": "post_group"}}
    if phase in {"wait_one", "wait_two"}:
        return {"type": "function", "function": {"name": "wait_for_activity"}}
    if phase == "engage_optional_dm":
        return {
            "type": "function",
            "function": {"name": "parent_followup_decision"},
        }
    if phase == "await_children_completion":
        return {
            "type": "function",
            "function": {"name": "wait_for_children_completion"},
        }
    return "auto"


def _child_tool_choice(agent_ctx) -> str | dict[str, Any]:
    phase = str(agent_ctx.state.phase)
    if phase == "start":
        return {"type": "function", "function": {"name": "get_roster"}}
    if phase == "share_intro":
        return {"type": "function", "function": {"name": "post_group"}}
    if phase == "send_dm":
        return {"type": "function", "function": {"name": "post_dm"}}
    if phase == "awaiting_activity":
        return {"type": "function", "function": {"name": "wait_for_activity"}}
    if phase == "optional_parent_reply":
        return "auto"
    if phase == "share_after_wake":
        return {"type": "function", "function": {"name": "post_group"}}
    return "auto"


PARENT_INSTRUCTIONS = """
You are the parent coordinator in a multi-agent communication demo.

The goal is to visibly demonstrate:
1) parent spawning subagents,
2) group chat messages,
3) direct messages between subagents,
4) direct message exchanges between parent and subagents,
5) wait.activity pause + wake behavior.

Behavior contract:
- In kickoff, send a clear group message with assignments.
- After your first wake, optionally send a direct message to one subagent if
  clarification is useful, or explicitly skip the DM if not needed.
- If you send a DM, wait once more so the subagent can respond.
- You must wait until all subagents complete.
- Final response should be a polished collaborative deliverable with a clear
  "credits by subagent" section.
- Format your final output in markdown with sections:
  1) "## Final Deliverable"
  2) "## Credits by Subagent"

Keep messages concise and practical.
"""


def _child_instructions(role_name: str) -> str:
    return f"""
You are the {role_name} subagent in a multi-agent messaging demo.

Behavior contract:
- Read team roster first.
- Send one concise group update from your role perspective.
- Send one direct message to a teammate selected from group membership.
- Pause briefly, then review your inbox/context messages.
- If parent asked you a direct question, reply via post_dm to "parent".
- Then send one more concise group update.
- Final response should mention what you contributed.

When calling post_dm, you may pass either a role name or task_id.
Keep all content brief and concrete.
"""


def _parent_prepare_turn(turn, agent_ctx):
    turn.tool_choice = _parent_tool_choice(agent_ctx)
    turn.parallel_tool_calls = False
    turn.temperature = 0.1


def _child_prepare_turn(turn, agent_ctx):
    turn.tool_choice = _child_tool_choice(agent_ctx)
    turn.parallel_tool_calls = False
    turn.temperature = 0.1


parent_agent = Agent[DemoState](
    name="parent_coordinator",
    description="Parent coordinator agent",
    model=ai_gateway(gpt_41_mini),
    instructions=PARENT_INSTRUCTIONS,
    tools=[
        spawn_team,
        post_group,
        wait_for_activity,
        parent_followup_decision,
        wait_for_children_completion,
    ],
    prepare_turn=_parent_prepare_turn,
    verifier=verify_parent_output,
    stop_when=any_of(no_tool_calls(), turn_count_is(16)),
)

researcher_agent = Agent[DemoState](
    name="researcher_agent",
    description="Researcher subagent",
    model=ai_gateway(gpt_41_mini),
    instructions=_child_instructions("researcher"),
    tools=[
        get_roster,
        post_group,
        post_dm,
        wait_for_activity,
    ],
    prepare_turn=_child_prepare_turn,
    stop_when=any_of(no_tool_calls(), turn_count_is(12)),
)

skeptic_agent = Agent[DemoState](
    name="skeptic_agent",
    description="Skeptic subagent",
    model=ai_gateway(gpt_41_mini),
    instructions=_child_instructions("skeptic"),
    tools=[
        get_roster,
        post_group,
        post_dm,
        wait_for_activity,
    ],
    prepare_turn=_child_prepare_turn,
    stop_when=any_of(no_tool_calls(), turn_count_is(12)),
)

synthesizer_agent = Agent[DemoState](
    name="synthesizer_agent",
    description="Synthesizer subagent",
    model=ai_gateway(gpt_41_mini),
    instructions=_child_instructions("synthesizer"),
    tools=[
        get_roster,
        post_group,
        post_dm,
        wait_for_activity,
    ],
    prepare_turn=_child_prepare_turn,
    stop_when=any_of(no_tool_calls(), turn_count_is(12)),
)
