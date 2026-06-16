"""The `Runtime` — the top-level orchestration object for a tractor application."""

from __future__ import annotations

from asyncio import Future
from typing import TYPE_CHECKING, override

from tractor.control_flow import ControlFlow, CrashPolicy, LogCrashPolicy
from tractor.protocols import RuntimeLike

if TYPE_CHECKING:
    from tractor.actor import Actor
    from tractor.message import Message
    from tractor.ref import ActorRef


class Runtime(RuntimeLike):
    """
    The top-level orchestration object.

    Create one at application startup and use it to spawn all actors:

    ```python
    runtime = Runtime()
    ref = runtime.spawn(MyActor())
    await runtime.ask(ref, MyMessage())
    ```

    Inside message handlers, use `ctx.ask` / `ctx.tell` which forward here,
    carrying sender identity for future tracing.

    :param crash_policy: observer called after every actor panic (after the
        actor's own `on_panic` has already made its `ControlFlow` decision).
        Defaults to `LogCrashPolicy`.
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
        Spawn `actor` and return a handle to it.

        :param actor: the actor instance to wrap and start
        :param capacity: inbox capacity (`None` for unbounded)
        :return: an `ActorRef` through which messages can be addressed
        """
        from tractor.ref import ActorRef  # deferred: breaks ref ↔ runtime cycle

        return ActorRef(actor, capacity=capacity, runtime=self)

    @override
    async def ask[A: Actor, R](
        self,
        ref: ActorRef[A],
        message: Message[A, R],
    ) -> R:
        """Send `message` to `ref` and wait for the reply."""
        future = await ref._inbox.ask(message)  # pyright: ignore[reportPrivateUsage]
        return await future

    @override
    async def tell[A: Actor, R](
        self,
        ref: ActorRef[A],
        message: Message[A, R],
    ) -> None:
        """Send `message` to `ref` without waiting for a reply."""
        await ref._inbox.tell(message)  # pyright: ignore[reportPrivateUsage]

    @override
    async def forward[A: Actor, R](
        self,
        ref: ActorRef[A],
        message: Message[A, R],
    ) -> Future[R]:
        """
        Send `message` to `ref` and return its reply future *without* awaiting it.

        Like `ask`, but the caller is handed the pending reply future
        instead of blocking on it. This is the entry point for reply
        *forwarding* (see `Context.forward`): a handler delegates its own
        reply to `ref` by linking this future into the original caller's,
        leaving the delegating actor free to process its next message.

        :param ref: the target actor
        :param message: the message to send
        :return: a future for the reply
        """
        return await ref._inbox.ask(message)  # pyright: ignore[reportPrivateUsage]

    def try_tell[A: Actor, R](
        self,
        ref: ActorRef[A],
        message: Message[A, R],
    ) -> None:
        """
        Non-blocking tell. Raises `asyncio.QueueFull` if the inbox has no capacity.

        :param ref: the target actor
        :param message: the message to send
        """
        ref._inbox.try_tell(message)  # pyright: ignore[reportPrivateUsage]

    def try_ask[A: Actor, R](
        self,
        ref: ActorRef[A],
        message: Message[A, R],
    ) -> Future[R]:
        """
        Non-blocking ask. Raises `asyncio.QueueFull` if the inbox has no capacity.

        Returns a future that resolves to the reply once the actor processes it.

        :param ref: the target actor
        :param message: the message to send
        :return: a future for the reply
        """
        return ref._inbox.try_ask(message)  # pyright: ignore[reportPrivateUsage]

    @override
    def notify_crash(
        self,
        actor: object,
        exc: BaseException,
        flow: ControlFlow,
    ) -> None:
        """Called by the driver unconditionally after every panic."""
        self._crash_policy.on_crash(actor, exc, flow)


__all__ = ["Runtime"]
