from __future__ import annotations

import base64
from dataclasses import is_dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Any

from pydantic import BaseModel


def serialize_data(data: Any) -> Any:
    """Serialize framework payloads into JSON-safe values."""
    if data is None or isinstance(data, (str, int, float, bool)):
        return data
    if isinstance(data, BaseModel):
        return data.model_dump()
    if isinstance(data, dict):
        return {key: serialize_data(value) for key, value in data.items()}
    if isinstance(data, (list, tuple, set, frozenset)):
        return [serialize_data(item) for item in data]
    if isinstance(data, (bytes, bytearray, memoryview)):
        try:
            return bytes(data).decode("utf-8")
        except UnicodeDecodeError:
            return base64.b64encode(bytes(data)).decode("ascii")
    if isinstance(data, (datetime, date, time)):
        return data.isoformat()
    if isinstance(data, timedelta):
        return data.total_seconds()
    if isinstance(data, Enum):
        return serialize_data(data.value)
    if is_dataclass(data) and not isinstance(data, type):
        result: dict[str, Any] = {}
        for field in data.__dataclass_fields__.values():
            value = getattr(data, field.name)
            try:
                result[field.name] = serialize_data(value)
            except Exception:
                try:
                    result[field.name] = repr(value)
                except Exception:
                    result[field.name] = "<unserialisable>"
        return result
    if callable(getattr(data, "model_dump", None)):
        return data.model_dump()  # type: ignore[attr-defined]
    if callable(getattr(data, "to_dict", None)):
        return data.to_dict()  # type: ignore[attr-defined]
    return str(data)


def decode(data: bytes | str) -> str:
    return data.decode("utf-8") if isinstance(data, bytes) else data


__all__ = [
    "decode",
    "serialize_data",
]
