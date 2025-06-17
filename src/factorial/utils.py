import uuid
import re
from typing import Any, Callable, get_origin
from pydantic import BaseModel
import inspect


def to_snake_case(camel: str) -> str:
    snake = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", camel)
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", snake)
    return snake.lower()


def serialize_data(data: Any) -> Any:
    """Serialize data for JSON-safe event publishing"""
    if data is None or isinstance(data, (str, int, float, bool)):
        return data
    elif isinstance(data, BaseModel):
        return data.model_dump()
    elif isinstance(data, dict):
        return {k: serialize_data(v) for k, v in data.items()}  # type: ignore
    elif isinstance(data, (list, tuple)):
        return [serialize_data(item) for item in data]  # type: ignore
    elif hasattr(data, "model_dump") and callable(data.model_dump):
        return data.model_dump()
    elif hasattr(data, "to_dict") and callable(data.to_dict):
        return data.to_dict()
    else:
        return str(data)


def decode(data: bytes | str) -> str:
    return data.decode("utf-8") if isinstance(data, bytes) else data


def is_valid_task_id(task_id: str) -> bool:
    try:
        uuid.UUID(task_id)
    except ValueError:
        return False
    return True


def _has_valid_arity(sig: inspect.Signature, required: int) -> bool:
    """Return True if *sig* has exactly *required* mandatory positional params."""
    required_positional = [
        p
        for p in sig.parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        and p.default is inspect.Parameter.empty
    ]
    return len(required_positional) == required


def _compatible(annotation: Any, expected: type) -> bool:
    """Return True if *annotation* is empty or a subclass of *expected*."""
    if annotation is inspect.Parameter.empty:
        return True  # no type hint
    try:
        origin = get_origin(annotation) or annotation
        return isinstance(origin, type) and issubclass(origin, expected)
    except Exception:
        return False


def validate_callback_signature(
    cb_name: str,
    fn: Callable[..., Any] | None,
    expected_required: tuple[type, ...],
) -> None:
    """Validate lifecycle callback *fn* against expected positional parameters.

    Parameters
    ----------
    cb_name: str
        Human-readable name of the callback (used in error messages).
    fn: Callable | None
        The callback function to validate.  If ``None`` the function returns silently.
    expected_required: tuple[type, ...]
        Tuple of expected types for the **mandatory positional** parameters.  The
        length of this tuple defines the required arity.

    Raises
    ------
    TypeError
        If the function has the wrong arity or incompatible type annotations.
    """
    if fn is None:
        return

    sig = inspect.signature(fn)

    if not _has_valid_arity(sig, len(expected_required)):
        required_positional = [
            p.name
            for p in sig.parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            and p.default is inspect.Parameter.empty
        ]
        raise TypeError(
            f"{cb_name} callback must have exactly {len(expected_required)} required "
            f"positional parameter(s) (got {len(required_positional)}: {required_positional})."
        )

    for param, expected in zip(sig.parameters.values(), expected_required):
        if not _compatible(param.annotation, expected):
            raise TypeError(
                f"{cb_name} callback – parameter '{param.name}' should be compatible "
                f"with {expected.__name__} but has annotation {param.annotation}."
            )
