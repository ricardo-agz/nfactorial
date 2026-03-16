from __future__ import annotations

import base64
import json
import mimetypes
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import Any, Literal, TypeAlias, TypedDict, TypeGuard, cast

from typing_extensions import NotRequired

from factorial.core.utils import serialize_data


class InputTextDict(TypedDict):
    type: Literal["input_text"]
    text: str


class InputImageDict(TypedDict):
    type: Literal["input_image"]
    image_url: NotRequired[str]
    file_id: NotRequired[str]
    detail: NotRequired[Literal["auto", "low", "high"]]


class InputFileDict(TypedDict):
    type: Literal["input_file"]
    file_id: NotRequired[str]
    file_url: NotRequired[str]
    file_data: NotRequired[str]
    filename: NotRequired[str]


class SystemMessageDict(TypedDict):
    role: Literal["system"]
    content: str


class UserMessageDict(TypedDict):
    role: Literal["user"]
    content: str | list[InputTextDict | InputImageDict | InputFileDict]


class AssistantMessageDict(TypedDict):
    role: Literal["assistant"]
    content: str


class ToolCallDict(TypedDict):
    id: str
    name: str
    arguments: object


class ToolCallMessageDict(TypedDict):
    role: Literal["assistant_tool_calls"]
    calls: list[ToolCallDict]


class ToolResultMessageDict(TypedDict):
    role: Literal["tool"]
    tool_call_id: str
    output: object
    is_error: bool
    tool_name: NotRequired[str]
    model_output: NotRequired[str]


Message: TypeAlias = (
    SystemMessageDict
    | UserMessageDict
    | AssistantMessageDict
    | ToolCallMessageDict
    | ToolResultMessageDict
)

MessageLike: TypeAlias = Message | Mapping[str, Any]


NormalizedContentPart: TypeAlias = InputTextDict | InputImageDict | InputFileDict


@dataclass(frozen=True)
class ImageInput:
    path: str | PathLike[str] | None = None
    image_url: str | None = None
    file_id: str | None = None
    detail: Literal["auto", "low", "high"] = "auto"


@dataclass(frozen=True)
class FileInput:
    path: str | PathLike[str] | None = None
    file_id: str | None = None
    file_url: str | None = None
    file_data: bytes | str | None = None
    filename: str | None = None


ContentPartLike: TypeAlias = (
    str
    | InputTextDict
    | InputImageDict
    | InputFileDict
    | ImageInput
    | FileInput
)


def _is_text_part(part: NormalizedContentPart) -> TypeGuard[InputTextDict]:
    return part["type"] == "input_text"


def _is_image_part(part: NormalizedContentPart) -> TypeGuard[InputImageDict]:
    return part["type"] == "input_image"


def _is_file_part(part: NormalizedContentPart) -> TypeGuard[InputFileDict]:
    return part["type"] == "input_file"


def _is_system_message(message: Message) -> TypeGuard[SystemMessageDict]:
    return message["role"] == "system"


def _is_user_message(message: Message) -> TypeGuard[UserMessageDict]:
    return message["role"] == "user"


def _is_assistant_message(message: Message) -> TypeGuard[AssistantMessageDict]:
    return message["role"] == "assistant"


def _is_tool_call_message(message: Message) -> TypeGuard[ToolCallMessageDict]:
    return message["role"] == "assistant_tool_calls"


def _is_tool_result_message(message: Message) -> TypeGuard[ToolResultMessageDict]:
    return message["role"] == "tool"


def _read_bytes(path: str | PathLike[str]) -> bytes:
    return Path(path).read_bytes()


def _data_url_for_image(path: str | PathLike[str]) -> str:
    resolved_path = Path(path)
    mime_type, _ = mimetypes.guess_type(str(resolved_path))
    if mime_type is None:
        mime_type = "application/octet-stream"
    encoded = base64.b64encode(_read_bytes(resolved_path)).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _normalize_content_part(
    part: ContentPartLike,
) -> InputTextDict | InputImageDict | InputFileDict:
    if isinstance(part, str):
        return {"type": "input_text", "text": part}

    if isinstance(part, ImageInput):
        if part.path is not None:
            image_path_part: InputImageDict = {
                "type": "input_image",
                "image_url": _data_url_for_image(part.path),
                "detail": part.detail,
            }
            return image_path_part
        if part.image_url is not None:
            image_url_part: InputImageDict = {
                "type": "input_image",
                "image_url": part.image_url,
                "detail": part.detail,
            }
            return image_url_part
        if part.file_id is not None:
            image_file_part: InputImageDict = {
                "type": "input_image",
                "file_id": part.file_id,
                "detail": part.detail,
            }
            return image_file_part
        raise ValueError("image(...) requires path, image_url, or file_id")

    if isinstance(part, FileInput):
        if part.path is not None:
            resolved_path = Path(part.path)
            encoded = base64.b64encode(_read_bytes(resolved_path)).decode("ascii")
            file_part: InputFileDict = {
                "type": "input_file",
                "file_data": encoded,
                "filename": part.filename or resolved_path.name,
            }
            return file_part
        if part.file_id is not None:
            file_id_part: InputFileDict = {
                "type": "input_file",
                "file_id": part.file_id,
            }
            if part.filename is not None:
                file_id_part["filename"] = part.filename
            return file_id_part
        if part.file_url is not None:
            file_url_part: InputFileDict = {
                "type": "input_file",
                "file_url": part.file_url,
            }
            if part.filename is not None:
                file_url_part["filename"] = part.filename
            return file_url_part
        if part.file_data is not None:
            file_data = part.file_data
            if isinstance(file_data, bytes):
                file_data = base64.b64encode(file_data).decode("ascii")
            file_data_part: InputFileDict = {
                "type": "input_file",
                "file_data": file_data,
            }
            if part.filename is not None:
                file_data_part["filename"] = part.filename
            return file_data_part
        raise ValueError("file(...) requires path, file_id, file_url, or file_data")

    if isinstance(part, Mapping):
        part_type = part.get("type")
        if part_type == "input_text":
            text = part.get("text")
            if not isinstance(text, str):
                raise TypeError("input_text parts require a string 'text' field")
            text_part: InputTextDict = {"type": "input_text", "text": text}
            return text_part
        if part_type == "input_image":
            normalized: InputImageDict = {"type": "input_image"}
            image_url = part.get("image_url")
            file_id = part.get("file_id")
            detail = part.get("detail")
            if image_url is not None:
                normalized["image_url"] = str(image_url)
            if file_id is not None:
                normalized["file_id"] = str(file_id)
            if detail is not None:
                normalized["detail"] = cast(
                    Literal["auto", "low", "high"],
                    str(detail),
                )
            if "image_url" not in normalized and "file_id" not in normalized:
                raise ValueError("input_image parts require image_url or file_id")
            return normalized
        if part_type == "input_file":
            mapping_file_part: InputFileDict = {"type": "input_file"}
            file_id = part.get("file_id")
            file_url = part.get("file_url")
            mapping_file_data = part.get("file_data")
            filename = part.get("filename")
            if file_id is not None:
                mapping_file_part["file_id"] = str(file_id)
            if file_url is not None:
                mapping_file_part["file_url"] = str(file_url)
            if mapping_file_data is not None:
                if not isinstance(mapping_file_data, str):
                    raise TypeError("input_file.file_data must be a string")
                mapping_file_part["file_data"] = mapping_file_data
            if filename is not None:
                mapping_file_part["filename"] = str(filename)
            if not any(
                key in mapping_file_part for key in ("file_id", "file_url", "file_data")
            ):
                raise ValueError(
                    "input_file parts require file_id, file_url, or file_data"
                )
            return mapping_file_part

    raise TypeError(f"Unsupported content part: {type(part).__name__}")


def system(content: str) -> SystemMessageDict:
    return {"role": "system", "content": content}


def assistant(content: str) -> AssistantMessageDict:
    return {"role": "assistant", "content": content}


def user(*parts: ContentPartLike) -> UserMessageDict:
    if not parts:
        raise ValueError("user(...) requires at least one content part")

    if len(parts) == 1 and isinstance(parts[0], str):
        return {"role": "user", "content": parts[0]}

    normalized_parts = [_normalize_content_part(part) for part in parts]
    return {
        "role": "user",
        "content": normalized_parts,
    }


def tool_call(
    name: str,
    arguments: object,
    *,
    call_id: str | None = None,
) -> ToolCallDict:
    serialized_arguments = serialize_data(arguments)
    call_hash = abs(
        hash(
            (
                name,
                json.dumps(
                    serialized_arguments,
                    sort_keys=True,
                    default=str,
                ),
            )
        )
    )
    normalized_call_id = call_id or f"call_{call_hash}"
    return {
        "id": normalized_call_id,
        "name": name,
        "arguments": arguments,
    }


def tool_calls(*calls: ToolCallDict) -> ToolCallMessageDict:
    return {
        "role": "assistant_tool_calls",
        "calls": list(calls),
    }


def tool_result(
    tool_call_id: str,
    output: object,
    *,
    tool_name: str | None = None,
    is_error: bool = False,
    model_output: str | None = None,
) -> ToolResultMessageDict:
    result: ToolResultMessageDict = {
        "role": "tool",
        "tool_call_id": tool_call_id,
        "output": output,
        "is_error": is_error,
    }
    if tool_name is not None:
        result["tool_name"] = tool_name
    if model_output is not None:
        result["model_output"] = model_output
    return result


def image(
    *,
    image_url: str | None = None,
    file_id: str | None = None,
    path: str | None = None,
    detail: Literal["auto", "low", "high"] = "auto",
) -> ImageInput:
    return ImageInput(
        path=path,
        image_url=image_url,
        file_id=file_id,
        detail=detail,
    )


def file(
    *,
    file_id: str | None = None,
    file_url: str | None = None,
    file_data: bytes | str | None = None,
    path: str | None = None,
    filename: str | None = None,
) -> FileInput:
    return FileInput(
        path=path,
        file_id=file_id,
        file_url=file_url,
        file_data=file_data,
        filename=filename,
    )


def normalize_message(message: MessageLike) -> Message:
    if not isinstance(message, Mapping):
        raise TypeError("Messages must be mapping objects")

    role = message.get("role")
    if role == "system":
        content = message.get("content")
        if not isinstance(content, str):
            raise TypeError("system messages require string content")
        return {"role": "system", "content": content}

    if role == "user":
        content = message.get("content")
        if isinstance(content, str):
            return {"role": "user", "content": content}
        if isinstance(content, list):
            return {
                "role": "user",
                "content": [
                    _normalize_content_part(cast(ContentPartLike, part))
                    for part in content
                ],
            }
        raise TypeError("user messages require string content or a content-part list")

    if role == "assistant":
        content = message.get("content")
        if not isinstance(content, str):
            raise TypeError("assistant messages require string content")
        return {"role": "assistant", "content": content}

    if role == "assistant_tool_calls":
        calls = message.get("calls")
        if not isinstance(calls, list):
            raise TypeError("assistant_tool_calls messages require a list of calls")
        normalized_calls: list[ToolCallDict] = []
        for call in calls:
            if not isinstance(call, dict):
                raise TypeError("Tool calls must be mapping objects")
            call_id = call.get("id")
            call_name = call.get("name")
            arguments = call.get("arguments")
            if not isinstance(call_id, str) or not isinstance(call_name, str):
                raise TypeError("Tool calls require string id and name fields")
            normalized_calls.append(
                {
                    "id": call_id,
                    "name": call_name,
                    "arguments": arguments,
                }
            )
        return {"role": "assistant_tool_calls", "calls": normalized_calls}

    if role == "tool":
        tool_call_id = message.get("tool_call_id")
        if not isinstance(tool_call_id, str):
            raise TypeError("tool messages require a string tool_call_id")
        normalized_tool_result: ToolResultMessageDict = {
            "role": "tool",
            "tool_call_id": tool_call_id,
            "output": message.get("output"),
            "is_error": bool(message.get("is_error", False)),
        }
        tool_name = message.get("tool_name")
        if tool_name is not None:
            normalized_tool_result["tool_name"] = str(tool_name)
        model_output = message.get("model_output")
        if model_output is not None:
            normalized_tool_result["model_output"] = str(model_output)
        return normalized_tool_result

    raise ValueError(f"Unsupported message role: {role!r}")


def normalize_messages_input(
    input_value: str | Sequence[MessageLike],
) -> list[Message]:
    if isinstance(input_value, str):
        return [user(input_value)]
    return [normalize_message(message) for message in input_value]


def message_to_chat_message(message: Message) -> dict[str, Any]:
    if _is_system_message(message):
        return {"role": "system", "content": message["content"]}

    if _is_user_message(message):
        content = message["content"]
        if isinstance(content, str):
            return {"role": "user", "content": content}

        chat_parts: list[dict[str, Any]] = []
        for part in content:
            if _is_text_part(part):
                chat_parts.append({"type": "text", "text": part["text"]})
            elif _is_image_part(part):
                if "image_url" not in part:
                    raise ValueError(
                        "Current chat-completions transport requires "
                        "input_image.image_url"
                    )
                image_url_payload: dict[str, Any] = {"url": part["image_url"]}
                if "detail" in part:
                    image_url_payload["detail"] = part["detail"]
                chat_parts.append(
                    {
                        "type": "image_url",
                        "image_url": image_url_payload,
                    }
                )
            elif _is_file_part(part):
                file_descriptor = part.get("filename") or part.get("file_url") or "file"
                chat_parts.append(
                    {
                        "type": "text",
                        "text": f"[Attached file: {file_descriptor}]",
                    }
                )
        return {"role": "user", "content": chat_parts}

    if _is_assistant_message(message):
        return {"role": "assistant", "content": message["content"]}

    if _is_tool_call_message(message):
        tool_call_payloads = []
        for call in message["calls"]:
            tool_call_payloads.append(
                {
                    "id": call["id"],
                    "type": "function",
                    "function": {
                        "name": call["name"],
                        "arguments": json.dumps(
                            serialize_data(call["arguments"]),
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                    },
                }
            )
        return {
            "role": "assistant",
            "content": None,
            "tool_calls": tool_call_payloads,
        }

    if _is_tool_result_message(message):
        model_output = message.get("model_output")
        if model_output is None:
            serialized = serialize_data(message.get("output"))
            if isinstance(serialized, str):
                model_output = serialized
            else:
                model_output = json.dumps(
                    serialized,
                    ensure_ascii=False,
                    sort_keys=True,
                )
        return {
            "role": "tool",
            "tool_call_id": message["tool_call_id"],
            "content": model_output,
        }

    raise ValueError(f"Unsupported message role: {message['role']!r}")


def messages_to_chat_messages(messages: list[Message]) -> list[dict[str, Any]]:
    return [message_to_chat_message(message) for message in messages]


__all__ = [
    "AssistantMessageDict",
    "ContentPartLike",
    "FileInput",
    "ImageInput",
    "InputFileDict",
    "InputImageDict",
    "InputTextDict",
    "Message",
    "MessageLike",
    "SystemMessageDict",
    "ToolCallDict",
    "ToolCallMessageDict",
    "ToolResultMessageDict",
    "UserMessageDict",
    "assistant",
    "file",
    "image",
    "message_to_chat_message",
    "messages_to_chat_messages",
    "normalize_message",
    "normalize_messages_input",
    "system",
    "tool_call",
    "tool_calls",
    "tool_result",
    "user",
]
