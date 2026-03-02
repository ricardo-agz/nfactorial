from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo

from factorial.execution.context import ExecutionContext


@dataclass(frozen=True)
class WaitInstruction:
    """Serializable wait intent used by the namespace-style API."""

    kind: Literal["sleep", "cron", "jobs", "activity", "signal"]
    data: Any = None
    sleep_s: float | None = None
    cron: str | None = None
    timezone: str | None = None
    child_task_ids: list[str] | None = None
    job_refs: list[dict[str, Any]] | None = None
    signal_id: str | None = None
    activity_timeout_kind: Literal["sleep", "cron"] | None = None
    activity_timeout_s: float | None = None
    activity_timeout_cron: str | None = None
    activity_timeout_timezone: str | None = None
    signal_timeout_kind: Literal["sleep", "cron"] | None = None
    signal_timeout_s: float | None = None
    signal_timeout_cron: str | None = None
    signal_timeout_timezone: str | None = None


def _parse_cron_field(field: str, minimum: int, maximum: int) -> tuple[set[int], bool]:
    """Parse one cron field into a value set and wildcard flag."""
    values: set[int] = set()
    is_wildcard = False

    if not field:
        raise ValueError("Cron field cannot be empty")

    for token in field.split(","):
        part = token.strip()
        if not part:
            raise ValueError(f"Invalid cron token '{token}'")

        if part == "*":
            is_wildcard = True
            values.update(range(minimum, maximum + 1))
            continue

        step = 1
        base = part
        if "/" in part:
            base, step_str = part.split("/", 1)
            if not step_str.isdigit() or int(step_str) <= 0:
                raise ValueError(f"Invalid cron step '{step_str}' in '{part}'")
            step = int(step_str)

        if base == "*":
            start = minimum
            end = maximum
        elif "-" in base:
            start_str, end_str = base.split("-", 1)
            if not start_str.isdigit() or not end_str.isdigit():
                raise ValueError(f"Invalid cron range '{base}'")
            start = int(start_str)
            end = int(end_str)
            if start > end:
                raise ValueError(f"Invalid cron range '{base}'")
        else:
            if not base.isdigit():
                raise ValueError(f"Invalid cron value '{base}'")
            start = int(base)
            end = int(base)

        if start < minimum or end > maximum:
            raise ValueError(
                f"Cron value '{part}' out of bounds [{minimum}, {maximum}]"
            )

        values.update(range(start, end + 1, step))

    return values, is_wildcard


def next_cron_wake_timestamp(
    expression: str,
    timezone: str = "UTC",
    *,
    now_ts: float | None = None,
) -> float:
    """Return the next wake timestamp for a 5-field cron expression."""
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError(
            f"Invalid cron expression '{expression}': expected 5 fields"
        )

    minute_vals, minute_any = _parse_cron_field(fields[0], 0, 59)
    hour_vals, hour_any = _parse_cron_field(fields[1], 0, 23)
    day_vals, day_any = _parse_cron_field(fields[2], 1, 31)
    month_vals, month_any = _parse_cron_field(fields[3], 1, 12)
    dow_vals_raw, dow_any = _parse_cron_field(fields[4], 0, 7)

    # Cron allows both 0 and 7 for Sunday.
    dow_vals = {0 if value == 7 else value for value in dow_vals_raw}
    if dow_any:
        dow_vals = set(range(0, 7))

    tzinfo = ZoneInfo(timezone)
    now = (
        datetime.fromtimestamp(now_ts, tz=tzinfo)
        if now_ts is not None
        else datetime.now(tz=tzinfo)
    )
    probe = now.replace(second=0, microsecond=0) + timedelta(minutes=1)

    # Bound search to one year to avoid infinite loops on invalid expressions.
    max_minutes = 366 * 24 * 60
    for _ in range(max_minutes):
        month_ok = month_any or probe.month in month_vals
        hour_ok = hour_any or probe.hour in hour_vals
        minute_ok = minute_any or probe.minute in minute_vals
        if month_ok and hour_ok and minute_ok:
            dom_match = day_any or probe.day in day_vals
            cron_dow = (probe.weekday() + 1) % 7  # Sunday=0
            dow_match = dow_any or cron_dow in dow_vals

            # Standard cron semantics: if both DOM and DOW are restricted,
            # either can match; otherwise the restricted side controls.
            if day_any and dow_any:
                day_ok = True
            elif day_any:
                day_ok = dow_match
            elif dow_any:
                day_ok = dom_match
            else:
                day_ok = dom_match or dow_match

            if day_ok:
                return probe.timestamp()

        probe += timedelta(minutes=1)

    raise ValueError(
        f"Could not find next cron occurrence for '{expression}' within one year"
    )


class WaitNamespace:
    """Namespace-style wait API (`wait.sleep`, `wait.cron`, `wait.jobs`)."""

    def sleep(self, seconds: float, *, data: Any = None) -> WaitInstruction:
        return WaitInstruction(kind="sleep", sleep_s=float(seconds), data=data)

    def cron(
        self,
        expression: str,
        *,
        timezone: str = "UTC",
        tz: str | None = None,
        data: Any = None,
    ) -> WaitInstruction:
        if tz and timezone != "UTC" and tz != timezone:
            raise ValueError("Provide either 'timezone' or 'tz', not both")
        resolved_timezone = tz or timezone
        return WaitInstruction(
            kind="cron",
            cron=expression,
            timezone=resolved_timezone,
            data=data,
        )

    def jobs(
        self,
        jobs: list[Any],
        *,
        data: Any = None,
    ) -> WaitInstruction:
        if not jobs:
            raise ValueError("wait.jobs requires at least one job reference")

        current_task_id: str | None = None
        try:
            current_task_id = ExecutionContext.current().task_id
        except LookupError:
            current_task_id = None

        task_ids: list[str] = []
        serialized_refs: list[dict[str, Any]] = []
        for job in jobs:
            if isinstance(job, dict):
                job_ref = dict(job)
            elif is_dataclass(job) and not isinstance(job, type):
                job_ref = asdict(job)
            elif hasattr(job, "model_dump") and callable(job.model_dump):
                dumped = job.model_dump()
                if not isinstance(dumped, dict):
                    raise TypeError(
                        "wait.jobs expects job refs with dict-like model_dump() output"
                    )
                job_ref = dumped
            else:
                raise TypeError(
                    "wait.jobs expects JobRef-like objects (dict/dataclass/pydantic)"
                )

            task_id = job_ref.get("task_id")
            if not isinstance(task_id, str) or not task_id:
                raise ValueError("Each job ref must include a non-empty task_id")

            parent_task_id = job_ref.get("parent_task_id")
            if current_task_id is not None:
                if not isinstance(parent_task_id, str) or not parent_task_id:
                    raise ValueError(
                        "wait.jobs requires each job ref to include a non-empty "
                        "parent_task_id for the current parent task."
                    )
                if parent_task_id != current_task_id:
                    raise ValueError(
                        "wait.jobs received a job ref that belongs to a different "
                        f"parent task. Expected '{current_task_id}', got "
                        f"'{parent_task_id}'."
                    )

            task_ids.append(task_id)
            serialized_refs.append(job_ref)

        # Stable order + de-duplication.
        deduped_ids = list(dict.fromkeys(task_ids))
        return WaitInstruction(
            kind="jobs",
            child_task_ids=deduped_ids,
            job_refs=serialized_refs,
            data=data,
        )

    def activity(
        self,
        *,
        timeout: WaitInstruction | None = None,
        data: Any = None,
    ) -> WaitInstruction:
        if timeout is None:
            return WaitInstruction(kind="activity", data=data)

        if not isinstance(timeout, WaitInstruction):
            raise TypeError(
                "wait.activity(timeout=...) expects wait.sleep(...) or wait.cron(...)"
            )
        if timeout.kind not in {"sleep", "cron"}:
            raise ValueError(
                "wait.activity(timeout=...) only supports wait.sleep(...) or "
                "wait.cron(...)"
            )

        if timeout.kind == "sleep":
            if timeout.sleep_s is None:
                raise ValueError(
                    "wait.activity(timeout=wait.sleep(...)) requires a sleep duration"
                )
            return WaitInstruction(
                kind="activity",
                data=data,
                activity_timeout_kind="sleep",
                activity_timeout_s=float(timeout.sleep_s),
            )

        if not timeout.cron:
            raise ValueError(
                "wait.activity(timeout=wait.cron(...)) requires a non-empty cron "
                "expression"
            )
        return WaitInstruction(
            kind="activity",
            data=data,
            activity_timeout_kind="cron",
            activity_timeout_cron=timeout.cron,
            activity_timeout_timezone=timeout.timezone or "UTC",
        )

    def until_signal(
        self,
        signal_id: str,
        *,
        timeout: WaitInstruction | None = None,
        data: Any = None,
    ) -> WaitInstruction:
        if not isinstance(signal_id, str) or not signal_id.strip():
            raise ValueError("wait.until_signal requires a non-empty signal_id")
        normalized_signal_id = signal_id.strip()
        if timeout is None:
            return WaitInstruction(
                kind="signal",
                signal_id=normalized_signal_id,
                data=data,
            )

        if not isinstance(timeout, WaitInstruction):
            raise TypeError(
                "wait.until_signal(timeout=...) expects wait.sleep(...) or "
                "wait.cron(...)"
            )
        if timeout.kind not in {"sleep", "cron"}:
            raise ValueError(
                "wait.until_signal(timeout=...) only supports wait.sleep(...) "
                "or wait.cron(...)"
            )

        if timeout.kind == "sleep":
            if timeout.sleep_s is None:
                raise ValueError(
                    "wait.until_signal(timeout=wait.sleep(...)) requires a sleep "
                    "duration"
                )
            return WaitInstruction(
                kind="signal",
                signal_id=normalized_signal_id,
                data=data,
                signal_timeout_kind="sleep",
                signal_timeout_s=float(timeout.sleep_s),
            )

        if not timeout.cron:
            raise ValueError(
                "wait.until_signal(timeout=wait.cron(...)) requires a non-empty "
                "cron expression"
            )
        return WaitInstruction(
            kind="signal",
            signal_id=normalized_signal_id,
            data=data,
            signal_timeout_kind="cron",
            signal_timeout_cron=timeout.cron,
            signal_timeout_timezone=timeout.timezone or "UTC",
        )


wait = WaitNamespace()

