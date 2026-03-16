"""High-signal contracts for message normalization helpers."""

from __future__ import annotations

import pytest

from factorial import (
    file,
    image,
    normalize_message,
    normalize_messages_input,
    tool_call,
    tool_calls,
    tool_result,
    user,
)


def test_string_input_normalizes_to_single_user_message() -> None:
    assert normalize_messages_input("Summarize the repo.") == [
        {"role": "user", "content": "Summarize the repo."}
    ]


def test_user_helper_normalizes_multimodal_path_inputs_at_boundary(
    tmp_path,
) -> None:
    image_path = tmp_path / "before.png"
    file_path = tmp_path / "requirements.pdf"
    image_path.write_bytes(b"\x89PNG\r\n\x1a\nfake-image")
    file_path.write_bytes(b"%PDF-1.7 fake-pdf")

    message = user(
        "Compare these inputs.",
        image(path=str(image_path), detail="high"),
        file(path=str(file_path)),
    )

    assert message["role"] == "user"
    assert isinstance(message["content"], list)
    assert message["content"][0] == {
        "type": "input_text",
        "text": "Compare these inputs.",
    }
    assert message["content"][1]["type"] == "input_image"
    assert message["content"][1]["detail"] == "high"
    assert message["content"][1]["image_url"].startswith("data:image/png;base64,")
    assert message["content"][2]["type"] == "input_file"
    assert message["content"][2]["filename"] == "requirements.pdf"
    assert isinstance(message["content"][2]["file_data"], str)


def test_normalize_message_accepts_raw_typed_content_parts() -> None:
    normalized = normalize_message(
        {
            "role": "user",
            "content": [
                {"type": "input_text", "text": "Inspect this image."},
                {
                    "type": "input_image",
                    "file_id": "file-image-1",
                    "detail": "low",
                },
                {
                    "type": "input_file",
                    "file_url": "https://example.test/spec.pdf",
                    "filename": "spec.pdf",
                },
            ],
        }
    )

    assert normalized == {
        "role": "user",
        "content": [
            {"type": "input_text", "text": "Inspect this image."},
            {
                "type": "input_image",
                "file_id": "file-image-1",
                "detail": "low",
            },
            {
                "type": "input_file",
                "file_url": "https://example.test/spec.pdf",
                "filename": "spec.pdf",
            },
        ],
    }


def test_tool_call_and_tool_result_helpers_build_transcript_messages() -> None:
    messages = normalize_messages_input(
        [
            user("Search for release notes."),
            tool_calls(
                tool_call(
                    "web_search",
                    {"query": "nfactorial release notes"},
                    call_id="call_1",
                )
            ),
            tool_result(
                "call_1",
                {"hits": 3},
                tool_name="web_search",
                model_output="Found 3 relevant results.",
            ),
        ]
    )

    assert messages == [
        {"role": "user", "content": "Search for release notes."},
        {
            "role": "assistant_tool_calls",
            "calls": [
                {
                    "id": "call_1",
                    "name": "web_search",
                    "arguments": {"query": "nfactorial release notes"},
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call_1",
            "tool_name": "web_search",
            "output": {"hits": 3},
            "is_error": False,
            "model_output": "Found 3 relevant results.",
        },
    ]
