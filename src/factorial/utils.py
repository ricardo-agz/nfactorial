import re
from typing import Any
from pydantic import BaseModel


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
