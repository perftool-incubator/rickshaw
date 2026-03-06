"""Filesystem utilities — port of Perl find_files() (L61-94)."""

from __future__ import annotations

import os

# Directory / file names to skip during recursive walk
_SKIP_NAMES: set[str] = {".", "..", ".git", "docs", ".github", "__pycache__"}

# File extensions to skip
_SKIP_EXTENSIONS: set[str] = {".md"}


def find_files(path: str) -> list[str]:
    """Recursively collect file paths under *path*, excluding common non-content entries.

    Matches the Perl ``find_files()`` behaviour:
    - Skips ``.git``, ``docs``, ``.github``, ``__pycache__`` directories
    - Skips ``.md`` files
    - If *path* is a regular file, returns ``[path]``
    - If *path* does not exist, returns ``[]``

    Returns a **sorted** list of absolute file paths.
    """
    results: list[str] = []

    if os.path.isdir(path):
        _walk(path, results)
    elif os.path.isfile(path):
        results.append(os.path.abspath(path))

    results.sort()
    return results


def _walk(directory: str, results: list[str]) -> None:
    """Recursive directory walker using os.scandir()."""
    try:
        entries = list(os.scandir(directory))
    except PermissionError:
        return

    for entry in entries:
        if entry.name in _SKIP_NAMES:
            continue
        if os.path.splitext(entry.name)[1] in _SKIP_EXTENSIONS:
            continue

        if entry.is_dir(follow_symlinks=True):
            _walk(entry.path, results)
        elif entry.is_file(follow_symlinks=True):
            results.append(entry.path)
