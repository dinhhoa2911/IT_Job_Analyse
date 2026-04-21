"""In-memory conversation history store keyed by conversation_id."""

from collections import defaultdict
from threading import Lock

_store: dict[str, list[dict]] = defaultdict(list)
_lock = Lock()

_MAX_TURNS = 10  # keep last 10 turns (20 messages) per conversation


def get_history(conversation_id: str) -> list[dict]:
    with _lock:
        return list(_store[conversation_id])


def add_turn(conversation_id: str, user_msg: str, assistant_msg: str) -> None:
    with _lock:
        history = _store[conversation_id]
        history.append({"role": "user", "content": user_msg})
        # Truncate assistant message to cap token usage in future context
        history.append({"role": "assistant", "content": assistant_msg[:800]})
        if len(history) > _MAX_TURNS * 2:
            _store[conversation_id] = history[-(_MAX_TURNS * 2):]


def clear(conversation_id: str) -> None:
    with _lock:
        _store.pop(conversation_id, None)
