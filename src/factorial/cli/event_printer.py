import atexit
import json
import sys
import time
from threading import Thread
from typing import Any
import click
from click import style


class ToolSpinner:
    """A simple spinner that shows while tools are executing."""

    def __init__(self):
        self.spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"  # Braille patterns for smooth animation
        self.idx = 0
        self.running = False
        self.thread = None
        self.current_tool = None

    def start(self, tool_name: str):
        """Start the spinner with a tool name."""
        if self.running:
            self.stop()  # Stop any existing spinner

        self.current_tool = tool_name
        self.running = True
        self.idx = 0
        self.thread = Thread(target=self._spin, daemon=True)
        self.thread.start()

    def stop(self):
        """Stop the spinner and clear the line."""
        if self.running:
            self.running = False
            if self.thread:
                self.thread.join(timeout=0.1)
            # Clear the spinner line
            sys.stdout.write("\r" + " " * 80 + "\r")
            sys.stdout.flush()

    def _spin(self):
        """Internal method to animate the spinner."""
        while self.running:
            char = self.spinner_chars[self.idx % len(self.spinner_chars)]
            tool_msg = (
                _get_tool_display_name(self.current_tool)
                if self.current_tool
                else "working"
            )
            message = (
                f"\r{style(char, fg='blue')} {style(tool_msg, fg='bright_black')}..."
            )
            sys.stdout.write(message)
            sys.stdout.flush()
            self.idx += 1
            time.sleep(0.08)  # 12.5 FPS for smooth animation


_spinner = ToolSpinner()
atexit.register(_spinner.stop)


def _get_tool_display_name(tool_name: str) -> str:
    """Convert tool names to human-friendly display names."""
    display_names = {
        "completion": "thinking",  # LLM completion phase
        "turn": "processing",  # Overall turn processing
        "think": "thinking",
        "plan": "planning",
        "design_doc": "designing",
        "read": "reading",
        "multi_read": "reading",
        "write": "writing",
        "edit_lines": "editing",
        "multi_edit": "editing",
        "find_replace": "replacing",
        "delete": "deleting",
        "tree": "exploring",
        "ls": "listing",
        "grep": "searching",
        "glob": "finding files",
        "create_react_app": "creating React app",
        "create_vite_app": "creating Vite app",
        "create_next_app": "creating Next.js app",
    }
    return display_names.get(tool_name, f"running {tool_name}")


async def event_printer(event) -> None:
    event_type: str = getattr(event, "event_type", "")

    if event_type == "progress_update_completion_started":
        _spinner.start("completion")

    elif event_type == "progress_update_completion_completed":
        _spinner.stop()

    elif event_type == "progress_update_completion_chunk":
        if not _spinner.running:
            _spinner.start("completion")

    elif event_type == "progress_update_run_turn_started":
        _spinner.start("turn")

    elif event_type == "progress_update_run_turn_completed":
        _spinner.stop()

    elif event_type == "progress_update_tool_action_started":
        try:
            data = event.data or {}
            args = data.get("args", [])
            if args:
                tool_call = args[0]
                tool_name = None

                if isinstance(tool_call, dict):
                    func_data = tool_call.get("function", {}) or {}
                    tool_name = func_data.get("name")
                else:
                    func_obj = getattr(tool_call, "function", None)
                    tool_name = getattr(func_obj, "name", None)

                if tool_name:
                    _spinner.start(tool_name)
        except Exception:
            _spinner.start("working")

    elif event_type == "progress_update_tool_action_completed":
        _spinner.stop()
        try:
            data = event.data or {}
            args = data.get("args", [])
            result_data = data.get("result")

            if not args:
                return

            tool_call = args[0]

            # Extract tool name and arguments
            tool_name = None
            arguments = None

            if isinstance(tool_call, dict):
                func_data = tool_call.get("function", {}) or {}
                tool_name = func_data.get("name")
                arguments = func_data.get("arguments")
            else:
                func_obj = getattr(tool_call, "function", None)
                tool_name = getattr(func_obj, "name", None)
                arguments = getattr(func_obj, "arguments", None)

            if not tool_name:
                click.echo("[agent] ran a tool")
                return

            # Parse arguments to get parameters
            parsed_args = {}
            if arguments:
                try:
                    if isinstance(arguments, str):
                        parsed_args = json.loads(arguments)
                    elif isinstance(arguments, dict):
                        parsed_args = arguments
                except (json.JSONDecodeError, TypeError):
                    pass

            result_metadata = None
            output_content = None
            if result_data and hasattr(result_data, "output_data"):
                result_metadata = result_data.output_data
                output_content = getattr(result_data, "output_str", None)
            elif isinstance(result_data, dict):
                result_metadata = result_data
                output_content = result_data.get("output_str")

            message = _format_tool_message(
                tool_name, parsed_args, result_metadata, output_content
            )
            if message:
                click.echo(message)
            else:
                click.echo(style("[agent] ", fg="blue") + f"ran tool: {tool_name}")

        except Exception:
            # Graceful degradation - just show basic tool execution
            click.echo(style("[agent] ", fg="blue") + "ran a tool")

    elif event_type.endswith("_failed"):
        # Stop spinner on failure and show detailed error
        _spinner.stop()
        error_msg = _format_error_message(event_type, event)
        click.echo(error_msg)


def _format_error_message(event_type: str, event) -> str:
    """Format a detailed error message from a failed event."""

    # Extract error details from the event
    error_text = None
    error_data = None

    try:
        if hasattr(event, "error") and event.error:
            error_text = str(event.error)
        elif hasattr(event, "data") and event.data:
            data = event.data
            if isinstance(data, dict):
                error_text = data.get("error")
                error_data = data
            elif hasattr(data, "error"):
                error_text = str(data.error)
    except Exception:
        pass

    # Determine the operation that failed and extract context
    operation = "operation"
    context_info = ""

    if "completion" in event_type:
        operation = "LLM completion"
    elif "tool_action" in event_type:
        operation = "tool execution"
        # Try to extract which tool failed
        try:
            if error_data and isinstance(error_data, dict):
                args = error_data.get("args", [])
                if args:
                    tool_call = args[0]
                    tool_name = None

                    if isinstance(tool_call, dict):
                        func_data = tool_call.get("function", {}) or {}
                        tool_name = func_data.get("name")
                    else:
                        func_obj = getattr(tool_call, "function", None)
                        tool_name = getattr(func_obj, "name", None)

                    if tool_name:
                        context_info = f" ({tool_name})"
        except Exception:
            pass
    elif "run_turn" in event_type:
        operation = "turn processing"

    # Base error message
    base_msg = style("[agent] ", fg="blue") + style(
        f"ERROR during {operation}{context_info}", fg="red", bold=True
    )

    # Add specific error details if available
    if error_text:
        # Clean up common error patterns
        if "http" in error_text.lower() and "429" in error_text:
            details = style(" - Rate limit exceeded (HTTP 429)", fg="yellow")
        elif "http" in error_text.lower() and "401" in error_text:
            details = style(" - Authentication failed (HTTP 401)", fg="yellow")
        elif "http" in error_text.lower() and "403" in error_text:
            details = style(" - Forbidden (HTTP 403)", fg="yellow")
        elif "http" in error_text.lower() and "500" in error_text:
            details = style(" - Server error (HTTP 500)", fg="yellow")
        elif "timeout" in error_text.lower():
            details = style(" - Request timeout", fg="yellow")
        elif "connection" in error_text.lower():
            details = style(" - Connection error", fg="yellow")
        elif "json" in error_text.lower() and "decode" in error_text.lower():
            details = style(" - Invalid JSON response", fg="yellow")
        elif "token" in error_text.lower() and "limit" in error_text.lower():
            details = style(" - Token limit exceeded", fg="yellow")
        elif (
            "file not found" in error_text.lower()
            or "no such file" in error_text.lower()
        ):
            details = style(" - File not found", fg="yellow")
        elif "permission denied" in error_text.lower():
            details = style(" - Permission denied", fg="yellow")
        elif "old_string not found" in error_text.lower():
            details = style(" - Text to replace not found", fg="yellow")
        elif "invalid json" in error_text.lower():
            details = style(" - Invalid JSON in arguments", fg="yellow")
        elif "model not found" in error_text.lower():
            details = style(" - Model not available", fg="yellow")
        elif "api key" in error_text.lower():
            details = style(" - API key issue", fg="yellow")
        else:
            # Generic error - truncate if too long
            clean_error = error_text.replace("\n", " ").replace("\r", " ")
            if len(clean_error) > 100:
                clean_error = clean_error[:100] + "..."
            details = style(f" - {clean_error}", fg="yellow")
    else:
        # No specific error details available
        details = style(" - Unknown error", fg="bright_black")

    return base_msg + details


def _format_tool_message(
    tool_name: str,
    args: dict[str, Any],
    result_metadata: Any,
    output_content: str | None = None,
) -> str | None:
    """Format a specific tool execution into a human-friendly message."""

    # File operations
    if tool_name == "read":
        file_path = args.get("file_path", "unknown")
        return style("[agent] ", fg="blue") + style(f"read '{file_path}'", fg="cyan")

    elif tool_name == "multi_read":
        file_paths = args.get("file_paths", [])
        num_files = len(file_paths) if isinstance(file_paths, list) else 0

        diag_count = 0
        error_count = 0
        if result_metadata and isinstance(result_metadata, dict):
            files_meta = result_metadata.get("files", [])
            if isinstance(files_meta, list):
                for fm in files_meta:
                    if isinstance(fm, dict):
                        if fm.get("has_diagnostics"):
                            diag_count += 1
                        if fm.get("error"):
                            error_count += 1

        extras = []
        if diag_count:
            extras.append(f"{diag_count} with diagnostics")
        if error_count:
            extras.append(f"{error_count} error" + ("s" if error_count != 1 else ""))
        extra_msg = (
            style(" (" + ", ".join(extras) + ")", fg="bright_black") if extras else ""
        )

        action_msg = style(
            f"read {num_files} file" + ("s" if num_files != 1 else ""), fg="cyan"
        )
        return style("[agent] ", fg="blue") + action_msg + extra_msg

    elif tool_name == "write":
        file_path = args.get("file_path", "unknown")
        overwrite = args.get("overwrite", False)
        if overwrite:
            action_msg = style(f"overwrote '{file_path}'", fg="yellow")
        else:
            action_msg = style(f"created '{file_path}'", fg="green")
        return style("[agent] ", fg="blue") + action_msg

    elif tool_name == "edit_lines":
        file_path = args.get("file_path", "unknown")
        start_line = args.get("start_line")
        end_line = args.get("end_line")

        if result_metadata and isinstance(result_metadata, dict):
            lines_added = result_metadata.get("lines_added", 0)
            lines_removed = result_metadata.get("lines_removed", 0)

            if lines_added or lines_removed:
                change_info = style(
                    f" (+{lines_added} -{lines_removed})", fg="bright_black"
                )
                return (
                    style("[agent] ", fg="blue")
                    + style(f"edited '{file_path}'", fg="green")
                    + change_info
                )

        if start_line and end_line:
            action_msg = style(
                f"edited lines {start_line}-{end_line} in '{file_path}'", fg="green"
            )
        elif start_line:
            action_msg = style(f"edited line {start_line} in '{file_path}'", fg="green")
        else:
            action_msg = style(f"edited '{file_path}'", fg="green")
        return style("[agent] ", fg="blue") + action_msg

    elif tool_name == "multi_edit":
        file_path = args.get("file_path", "unknown")
        edits = args.get("edits", [])
        num_edits = len(edits) if isinstance(edits, list) else 1

        if result_metadata and isinstance(result_metadata, dict):
            total_added = result_metadata.get("total_lines_added", 0)
            total_removed = result_metadata.get("total_lines_removed", 0)
            change_info = (
                style(f" (+{total_added} -{total_removed})", fg="bright_black")
                if total_added or total_removed
                else ""
            )
            action_msg = (
                style(f"made {num_edits} edits to '{file_path}'", fg="green")
                + change_info
            )
        else:
            action_msg = style(f"made {num_edits} edits to '{file_path}'", fg="green")

        return style("[agent] ", fg="blue") + action_msg

    elif tool_name == "find_replace":
        file_path = args.get("file_path", "unknown")
        old_string = args.get("old_string", "")
        replace_all = args.get("replace_all", False)

        if result_metadata and isinstance(result_metadata, dict):
            occurrences = result_metadata.get("occurrences_replaced", 1)
            if occurrences > 1:
                action_msg = style(
                    f"replaced {occurrences} occurrences in '{file_path}'", fg="green"
                )
                return style("[agent] ", fg="blue") + action_msg

        action = "replaced all" if replace_all else "replaced"
        # Truncate old_string if it's too long
        old_display = old_string[:30] + "..." if len(old_string) > 30 else old_string
        action_msg = style(f"{action} '{old_display}' in '{file_path}'", fg="green")
        return style("[agent] ", fg="blue") + action_msg

    elif tool_name == "delete":
        file_path = args.get("file_path", "unknown")
        return style("[agent] ", fg="blue") + style(
            f"deleted '{file_path}'", fg="red", bold=True
        )

    # Search operations
    elif tool_name == "tree":
        dir_path = args.get("dir_path", ".")
        return style("[agent] ", fg="blue") + style(f"tree '{dir_path}'", fg="magenta")

    elif tool_name == "ls":
        dir_path = args.get("dir_path", ".")
        return style("[agent] ", fg="blue") + style(f"ls '{dir_path}'", fg="magenta")

    elif tool_name == "grep":
        pattern = args.get("pattern", "")
        dir_path = args.get("dir_path", ".")

        if result_metadata and isinstance(result_metadata, dict):
            matches = result_metadata.get("match_count", 0)
            if matches > 0:
                match_info = style(f" ({matches} matches)", fg="bright_black")
                action_msg = (
                    style(f"grep '{pattern}' in '{dir_path}'", fg="magenta")
                    + match_info
                )
                return style("[agent] ", fg="blue") + action_msg

        action_msg = style(f"grep '{pattern}' in '{dir_path}'", fg="magenta")
        return style("[agent] ", fg="blue") + action_msg

    elif tool_name == "glob":
        pattern = args.get("pattern", "")
        dir_path = args.get("dir_path", ".")

        if result_metadata and isinstance(result_metadata, dict):
            file_count = result_metadata.get("file_count", 0)
            if file_count > 0:
                file_info = style(f" ({file_count} files)", fg="bright_black")
                action_msg = (
                    style(f"glob '{pattern}' in '{dir_path}'", fg="magenta") + file_info
                )
                return style("[agent] ", fg="blue") + action_msg

        action_msg = style(f"glob '{pattern}' in '{dir_path}'", fg="magenta")
        return style("[agent] ", fg="blue") + action_msg

    # Project operations
    elif tool_name in ["create_react_app", "create_vite_app", "create_next_app"]:
        project_name = args.get("project_name", "unknown")
        framework = tool_name.replace("create_", "").replace("_app", "")
        action_msg = style(
            f"created {framework} project '{project_name}'", fg="green", bold=True
        )
        return style("[agent] ", fg="blue") + action_msg

    # Thinking operations - show actual thoughts/content
    elif tool_name in ["think", "plan", "design_doc"]:
        if output_content:
            # Show the actual thinking content, truncated if too long
            content = output_content.strip()
            if len(content) > 200:
                content = content[:200] + "..."

            # Replace newlines with spaces for single-line display
            content = content.replace("\n", " ").replace("\r", " ")

            if tool_name == "think":
                return (
                    style("[agent] ", fg="blue")
                    + style("💭 ", fg="bright_white")
                    + style(content, fg="bright_black")
                )
            elif tool_name == "plan":
                return (
                    style("[agent] ", fg="blue")
                    + style("📋 ", fg="bright_white")
                    + style(content, fg="magenta")
                )
            elif tool_name == "design_doc":
                return (
                    style("[agent] ", fg="blue")
                    + style("📐 ", fg="bright_white")
                    + style(content, fg="magenta")
                )
        else:
            # Fallback if no content available
            return style("[agent] ", fg="blue") + style(
                f"thinking: {tool_name}", fg="magenta"
            )

    # Default fallback
    return None
