"""The definition of the ``ActorRef`` class, for interacting with ``Actor``s."""

from __future__ import annotations

import asyncio
from asyncio import CancelledError
from contextlib import suppress
from typing import Protocol, final

from tractor.actor import Actor
from tractor.control_flow import ControlFlow
from tractor.handles import InboxHandle, ResponderHandle
from tractor.inbox import Inbox
from tractor.message import Context, Message
from tractor.request import Ask, Tell

_default_runtime: _RuntimeLike | None = None


def _get_default_runtime() -> _RuntimeLike:
    """Return the module-level default runtime, creating it on first access."""
    global _default_runtime
    if _default_runtime is None:
        from tractor.runtime import Runtime

        _default_runtime = Runtime()
    return _default_runtime


class _RuntimeLike(Protocol):
    """Structural type satisfied by ``Runtime``; used in ``ActorRef`` to avoid a cycle."""

    def notify_crash(
        self, actor: object, exc: BaseException, flow: ControlFlow
    ) -> None: ...

    async def ask[A: Actor, R](
        self, ref: ActorRef[A], message: Message[A, R]
    ) -> R: ...

    async def tell[A: Actor, R](
        self, ref: ActorRef[A], message: Message[A, R]
    ) -> None: ...


@final
class ActorRef[A: Actor]:
    """A handle to an actor of type ``A``."""

    def __init__(
        self,
        actor: A,
        *,
        capacity: int | None = None,
        runtime: _RuntimeLike | None = None,
    ):
        """
        Wrap an actor in an ``ActorRef`` object.

        Prefer ``Runtime.spawn()`` over constructing this directly — it ensures
        the actor is registered with a runtime and its sends are observable.

        :param actor: the actor to wrap
        :param capacity: inbox capacity (``None`` for unbounded)
        :param runtime: the runtime to use; defaults to the module-level singleton
        """
        if runtime is None:
            runtime = _get_default_runtime()
        self._actor = actor
        self._inbox = Inbox[A](capacity)
        self._runtime: _RuntimeLike = runtime
        self._task = asyncio.create_task(self._driver())

    async def _driver(self) -> None:
        try:
            try:
                await self._actor.on_start()
            except CancelledError:
                raise
            except BaseException as exc:
                flow = await self._actor.on_panic(exc)
                self._runtime.notify_crash(self._actor, exc, flow)
                return  # on_stop still called from outer finally

            inbox = InboxHandle(self._recv)
            while True:
                try:
                    handle = await self._actor.step(inbox)
                except CancelledError:
                    raise
                except BaseException as exc:
                    flow = await self._actor.on_panic(exc)
                    self._runtime.notify_crash(self._actor, exc, flow)
                    if flow is ControlFlow.Stop:
                        break
                    continue

                if handle is not None:
                    try:
                        await handle.respond()
                    except CancelledError:
                        raise
                    except BaseException as exc:
                        # Reply future already resolved in Responder.respond()
                        flow = await self._actor.on_panic(exc)
                        self._runtime.notify_crash(self._actor, exc, flow)
                        if flow is ControlFlow.Stop:
                            break
        finally:
            self._inbox.drain()
            try:
                await self._actor.on_stop()
            except CancelledError:
                raise
            except BaseException as exc:
                self._runtime.notify_crash(self._actor, exc, ControlFlow.Stop)

    async def _recv(self) -> ResponderHandle:
        """Receive the next message, bound to this actor and a fresh context."""
        responder = await self._inbox.get()
        return ResponderHandle(lambda: responder.respond(self._actor, Context(self)))

    def ask[R](self, message: Message[A, R]) -> Ask[A, R]:
        """
        Ask this actor to process a message.

        :param message: the message to send
        :return: an ``Awaitable`` to configure transmission and retrieve the reply
        """
        return Ask(self._inbox, message)

    def tell[R](self, message: Message[A, R]) -> Tell[A, R]:
        """
        Tell this actor to process a message.

        :param message: the message to send
        :return: an ``Awaitable`` to configure transmission
        """
        return Tell(self._inbox, message)

    async def stop(self) -> None:
        """Gracefully stop this actor."""
        # Yield once so the driver task has had at least one event loop turn.
        # In Python 3.14+, cancelling a task before its first turn skips the
        # coroutine body entirely — on_start and on_stop would never run.
        await asyncio.sleep(0)
        _ = self._task.cancel()
        with suppress(CancelledError):
            await self._task
        self._inbox.shutdown()
        self._inbox.drain()  # resolve any messages enqueued in the race window


__all__ = ["ActorRef"]
