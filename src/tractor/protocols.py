"""Structural protocols shared across the package.

These exist to decouple modules that would otherwise form import cycles. The
canonical example is :class:`RuntimeLike`: ``ActorRef`` and ``Message`` both need
to talk about "a runtime", but importing ``Runtime`` directly would create a
cycle (``runtime`` imports ``ref`` to spawn actors). Routing the dependency
through a structural protocol breaks that knot in one place.

This module imports nothing from ``tractor`` at runtime, so it is always safe to
import from anywhere in the package.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from asyncio import Future

    from tractor.actor import Actor
    from tractor.control_flow import ControlFlow
    from tractor.message import Message
    from tractor.ref import ActorRef


class RuntimeLike(Protocol):
    """The subset of ``Runtime`` that ``ActorRef`` and ``Message`` depend on.

    ``Runtime`` inherits this protocol so conformance is checked at the
    definition site rather than only structurally at call sites — if the two
    drift apart, the type checker flags ``Runtime`` directly.
    """

    def notify_crash(
        self, actor: object, exc: BaseException, flow: ControlFlow
    ) -> None: ...

    async def ask[A: Actor, R](self, ref: ActorRef[A], message: Message[A, R]) -> R: ...

    async def tell[A: Actor, R](
        self, ref: ActorRef[A], message: Message[A, R]
    ) -> None: ...

    async def forward[A: Actor, R](
        self, ref: ActorRef[A], message: Message[A, R]
    ) -> Future[R]: ...


__all__ = ["RuntimeLike"]
