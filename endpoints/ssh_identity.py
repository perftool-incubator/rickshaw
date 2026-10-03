"""Small shared helpers for run-scoped SSH identity selection."""

import json
import os
from pathlib import Path


def load_profile_sockets():
    """Load the private, per-run profile-to-socket map if one was supplied."""

    filename = os.environ.get("CRUCIBLE_SSH_PROFILE_SOCKETS_FILE")
    if not filename:
        return {}
    try:
        path = Path(filename)
        if path.stat().st_size > 65536:
            raise ValueError("SSH identity socket map exceeds its size limit")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("SSH identity socket map is unavailable") from exc
    if not isinstance(value, dict) or any(
        not isinstance(name, str)
        or not isinstance(socket_path, str)
        or not socket_path.startswith("/")
        for name, socket_path in value.items()
    ):
        raise RuntimeError("SSH identity socket map is invalid")
    return value


def profile_socket(profile):
    """Return a selected profile's run-scoped agent socket or fail closed."""

    if profile is None:
        return None
    if not isinstance(profile, str) or not profile:
        raise ValueError("SSH identity profile must be a non-empty string")
    socket_path = load_profile_sockets().get(profile)
    if not socket_path:
        raise RuntimeError("requested SSH identity profile is unavailable")
    if not Path(socket_path).is_socket():
        raise RuntimeError("requested SSH identity profile is unavailable")
    return socket_path
