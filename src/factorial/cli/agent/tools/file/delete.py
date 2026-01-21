import os
from typing import Any


def delete(file_path: str) -> tuple[str, dict[str, Any]]:
    """
    Deletes a file from the file system.

    _CRITICAL_: ONLY delete files you have created that you are 100% confident are
    no longer needed or if the user has explicitly asked you to delete them.
    _CRITICAL_: DO NOT delete files that are currently being used by the project.
    If your intention is to remove a file, you must make sure it is no longer being
    used first.

    Arguments:
    file_path: The path to the file to delete

    Usage:
    * This tool will delete the file if it exists.
    """
    if not os.path.exists(file_path):
        return (
            f"Error: File {file_path} does not exist.",
            {"success": False, "error": "File not found"},
        )

    # Get file info before deletion
    file_size = os.path.getsize(file_path)

    os.remove(file_path)

    metadata = {
        "success": True,
        "file_size": file_size,
    }

    return f"File {file_path} deleted", metadata
