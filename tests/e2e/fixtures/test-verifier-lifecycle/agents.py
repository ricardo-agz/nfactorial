from __future__ import annotations

import json
from typing import Any

from factorial import AgentContext, ExecutionContext, verify
from factorial.testing import MockAgent


def _json_output(summary: str, score: int) -> str:
    return json.dumps(
        {"summary": summary, "score": score},
        separators=(",", ":"),
        sort_keys=True,
    )


async def _retry_until_score_passes(
    output: Any,
    *,
    agent_ctx: AgentContext[Any, Any],
    execution_ctx: ExecutionContext,
) -> Any:
    payload = json.loads(str(output))
    if payload["score"] < 5:
        return verify.retry(
            "Need stronger evidence.",
            code="score_low",
            metadata={
                "score": payload["score"],
                "turn_number": agent_ctx.turn_number,
            },
        )
    return verify.accept(
        metadata={
            "verified": True,
            "summary": payload["summary"],
            "owner_id": execution_ctx.owner_id,
            "turn_number": agent_ctx.turn_number,
        }
    )


async def _fail_after_one_retry(
    _output: Any,
    *,
    agent_ctx: AgentContext[Any, Any],
) -> Any:
    if agent_ctx.verification.attempts_used >= 1:
        return verify.fail(
            "verification retry limit reached",
            code="tests_failed",
            metadata={"attempts_used": agent_ctx.verification.attempts_used},
        )
    return verify.retry(
        "not acceptable",
        code="tests_failed",
        metadata={"attempts_used": agent_ctx.verification.attempts_used},
    )


verification_retry_agent = MockAgent(
    name="verification_retry_agent",
    instructions="Produce a weak first answer, then a stronger revision.",
    responses=[
        _json_output("first attempt", 1),
        _json_output("second attempt", 10),
    ],
    verifier=_retry_until_score_passes,
)


verification_failure_agent = MockAgent(
    name="verification_failure_agent",
    instructions="Keep producing unacceptable answers until verification fails.",
    responses=[
        _json_output("bad", 0),
        _json_output("still bad", 0),
    ],
    verifier=_fail_after_one_retry,
)
