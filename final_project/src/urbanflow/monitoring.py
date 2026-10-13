"""Visible, bounded polling used by UrbanFlow platform automation."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class PollResult:
    state: str
    terminal: bool
    timed_out: bool
    polls: int


def poll_state(
    fetch_state: Callable[[], str],
    *,
    terminal_states: set[str],
    timeout_seconds: float,
    poll_interval_seconds: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> PollResult:
    """Poll until a known terminal state or a real timeout is observed."""
    if timeout_seconds < 0 or poll_interval_seconds < 0:
        raise ValueError("Polling durations must be non-negative.")
    deadline = clock() + timeout_seconds
    polls = 0
    while True:
        state = str(fetch_state()).upper()
        polls += 1
        if state in terminal_states:
            return PollResult(state=state, terminal=True, timed_out=False, polls=polls)
        if clock() >= deadline:
            return PollResult(state=state, terminal=False, timed_out=True, polls=polls)
        sleep(poll_interval_seconds)


def state_name(value: object) -> str:
    """Normalize Databricks enum objects and strings without private SDK helpers."""
    raw = getattr(value, "value", value)
    return str(raw).split(".")[-1].upper()
