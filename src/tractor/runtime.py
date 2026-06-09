"""The ``Runtime`` — the top-level orchestration object for a tractor application."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tractor.control_flow import ControlFlow, CrashPolicy, LogCrashPolicy

if TYPE_CHECKING:
    from tractor.actor import Actor
    from tractor.message import Message
    from tractor.ref import ActorRef


class Runtime:
    """
    The top-level orchestration object.

    Create one at application startup and use it to spawn all actors::

        runtime = Runtime()
        ref = runtime.spawn(MyActor())
        await runtime.ask(ref, MyMessage())

    The runtime is also accessible from inside message handlers via
    ``ctx.ref._runtime``, and through ``ctx.tell`` / ``ctx.ask``.

    :param crash_policy: observer called after every actor panic (after the
        actor's own ``on_panic`` has already made its ``ControlFlow`` decision).
        Defaults to ``LogCrashPolicy``.
    """

    def __init__(self, crash_policy: CrashPolicy | None = None) -> None:
        self._crash_policy: CrashPolicy = crash_policy or LogCrashPolicy()

    def spawn[A: Actor](
        self,
        actor: A,
        *,
        capacity: int | None = None,
    ) -> ActorRef[A]:
        """
        Spawn ``actor`` and return a handle to it.

        :param actor: the actor instance to wrap and start
        :param capacity: inbox capacity (``None`` for unbounded)
        :return: an ``ActorRef`` through which messages can be sent
        """
        from tractor.ref import ActorRef  # deferred: breaks ref ↔ runtime cycle

        return ActorRef(actor, capacity=capacity, runtime=self)

    async def ask[A: Actor, R](
        self,
        ref: ActorRef[A],
        message: Message[A, R],
    ) -> R:
        """Send ``message`` to ``ref`` and wait for the reply."""
        return await ref.ask(message)

    async def tell[A: Actor, R](
        self,
        ref: ActorRef[A],
        message: Message[A, R],
    ) -> None:
        """Send ``message`` to ``ref`` without waiting for a reply."""
        await ref.tell(message)

    def notify_crash(
        self,
        actor: object,
        exc: BaseException,
        flow: ControlFlow,
    ) -> None:
        """Called by the driver unconditionally after every panic."""
        self._crash_policy.on_crash(actor, exc, flow)


__all__ = ["Runtime"]
