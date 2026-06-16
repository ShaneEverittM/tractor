"""ControlFlow enum and CrashPolicy protocol.

Kept in their own module so both `actor.py` and `runtime.py` can import
from here without creating a cycle.
"""

import logging
from enum import Enum
from typing import Protocol, runtime_checkable


class ControlFlow(Enum):
    """The decision an actor returns from `on_panic`."""

    Continue = "continue"
    Stop = "stop"


@runtime_checkable
class CrashPolicy(Protocol):
    """
    Observer called by the `Runtime` after every actor panic.

    Receives the `ControlFlow` decision already made by the actor's own
    `on_panic`, so it is purely an observer — it cannot override that
    decision. Use it for logging, metrics, alerting, or trace capture.
    """

    def on_crash(
        self,
        actor: object,
        exc: BaseException,
        flow: ControlFlow,
    ) -> None: ...


class LogCrashPolicy:
    """Default crash policy: logs the panic at ERROR level."""

    def on_crash(self, actor: object, exc: BaseException, flow: ControlFlow) -> None:
        logging.getLogger("tractor").error(
            "Actor %r panicked (flow=%s): %s", actor, flow.value, exc, exc_info=exc
        )


__all__ = ["ControlFlow", "CrashPolicy", "LogCrashPolicy"]
