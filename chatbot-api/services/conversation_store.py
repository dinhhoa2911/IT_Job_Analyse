"""
@file conversation_store.py
@brief Thread-safe in-memory conversation history store keyed by conversation_id.

Each conversation is capped at _MAX_TURNS (10) turns (20 messages) to bound
memory usage and keep the LLM context window manageable.  Assistant messages
are truncated to 800 characters before storage to further limit future context
token usage.

This store is process-local (no persistence); restarting the server clears all
history.  For production deployments, replace with a Redis-backed store.
"""

from collections import defaultdict
from threading import Lock

_store: dict[str, list[dict]] = defaultdict(list)
_lock = Lock()

_MAX_TURNS = 10  # keep last 10 turns (20 messages) per conversation


def get_history(conversation_id: str) -> list[dict]:
    """
    @brief Retrieve a snapshot of the conversation history for the given ID.

    Returns a shallow copy so callers cannot accidentally mutate the store.

    @param conversation_id  Client-generated conversation identifier.
    @return                 List of role/content dicts (OpenAI message format), oldest first.
    """
    with _lock:
        return list(_store[conversation_id])


def add_turn(conversation_id: str, user_msg: str, assistant_msg: str) -> None:
    """
    @brief Append a user/assistant turn to the conversation and enforce the turn cap.

    The assistant message is truncated to 800 characters to limit the token
    footprint when this history is injected into future LLM prompts.
    Older turns beyond _MAX_TURNS are discarded (sliding window).

    @param conversation_id  Client-generated conversation identifier.
    @param user_msg         The user's raw message text.
    @param assistant_msg    The assistant's generated response text.
    """
    with _lock:
        history = _store[conversation_id]
        history.append({"role": "user", "content": user_msg})
        # Truncate assistant message to cap token usage in future context
        history.append({"role": "assistant", "content": assistant_msg[:800]})
        if len(history) > _MAX_TURNS * 2:
            _store[conversation_id] = history[-(_MAX_TURNS * 2):]


def clear(conversation_id: str) -> None:
    """
    @brief Remove all history for the given conversation ID from the store.

    @param conversation_id  Client-generated conversation identifier to clear.
    """
    with _lock:
        _store.pop(conversation_id, None)
