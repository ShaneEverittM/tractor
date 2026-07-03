"""Abstract interfaces shared across the package.

These exist to decouple modules that would otherwise form import cycles. The
canonical example is `RuntimeLike`: `ActorRef` and `Message` both need to
talk about "a runtime", but importing `Runtime` directly would create a
cycle (`runtime` imports `ref` to spawn actors). What breaks the knot is
that this module is a leaf — it imports nothing from `tractor` at runtime —
so it is always safe to import from anywhere in the package.

These are nominal ABCs rather than structural protocols: the set of
implementations is closed and in-repo (`Runtime`, `Context`), and requiring
explicit inheritance means drift from an interface is flagged at the
implementation's definition site. Structural protocols are reserved for
seams users implement without subclassing tractor (see `CrashPolicy`).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from asyncio import Future

    from tractor.actor import Actor
    from tractor.control_flow import ControlFlow
    from tractor.message import Message
    from tractor.ref import ActorRef


class MessagePort(ABC):
    """Anything a send can be routed through: a `Runtime` or a handler's `Context`.

    `Message.sender` / `Message.teller` accept any port, so send handles can
    be built both outside actors (from the `Runtime`, the trace root) and
    inside handlers (from the `Context`, which keeps the send attributed to
    the sending actor).

    The parameters are positional-only so that `Runtime` (which names the
    recipient `ref`) and `Context` (which names it `target`) can both
    override them compatibly.
    """

    @abstractmethod
    async def ask[A: Actor, R](
        self, ref: ActorRef[A], message: Message[A, R], /
    ) -> R: ...

    @abstractmethod
    async def tell[A: Actor, R](
        self, ref: ActorRef[A], message: Message[A, R], /
    ) -> None: ...


class RuntimeLike(MessagePort):
    """The subset of `Runtime` that `ActorRef` and `Message` depend on.

    `Runtime` is the only implementation; depending on this interface
    instead of the concrete class is what keeps `ref` and `message` out of
    the import cycle described in the module docstring.
    """

    @abstractmethod
    def notify_crash(
        self, actor: object, exc: BaseException, flow: ControlFlow
    ) -> None: ...

    @abstractmethod
    async def forward[A: Actor, R](
        self, ref: ActorRef[A], message: Message[A, R]
    ) -> Future[R]: ...


__all__ = ["MessagePort", "RuntimeLike"]
