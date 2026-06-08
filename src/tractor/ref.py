"""The definition of the ``ActorRef`` class, for interacting with ``Actor``s."""

import asyncio
from asyncio import CancelledError
from contextlib import suppress
from typing import final

from tractor.actor import Actor
from tractor.handles import InboxHandle, ResponderHandle
from tractor.inbox import Inbox
from tractor.message import Context, Message
from tractor.request import Ask, Tell


@final
class ActorRef[A: Actor]:
    """A handle to an actor of type ``A``."""

    def __init__(self, actor: A, *, capacity: int | None = None):
        """
        Wrap an actor in an ``ActorRef`` object.

        :param actor: the actor to wrap
        :param capacity: the most messages that may wait unprocessed in the
            inbox before senders must wait — or, for the ``try_*`` send
            variants, be rejected with ``asyncio.QueueFull``. ``None`` (the
            default) leaves the inbox unbounded.
        """
        self._actor = actor
        self._inbox = Inbox[A](capacity)
        self._task = asyncio.create_task(self._driver())

    async def _driver(self) -> None:
        inbox = InboxHandle(self._recv)
        while True:
            handle = await self._actor.step(inbox)
            if handle is not None:
                await handle.respond()

    async def _recv(self) -> ResponderHandle:
        """Receive the next message, bound to this actor and a fresh context."""
        responder = await self._inbox.get()
        return ResponderHandle(lambda: responder.respond(self._actor, Context(self)))

    def ask[R](self, message: Message[A, R]) -> Ask[A, R]:
        """
        Ask this actor to process a message.

        See the configuration methods on ``Ask`` to
        control things like timeouts.

        :param message: the message to send
        :return: an ``Awaitable`` to configure transmission and retrieve the reply
        """
        return Ask(self._inbox, message)

    def tell[R](self, message: Message[A, R]) -> Tell[A, R]:
        """
        Tell this actor to process a message.

        See the configuration methods on ``Tell`` to
        control things like timeouts.

        :param message: the message to send
        :return: a ``Awaitable`` to configure transmission
        """
        return Tell(self._inbox, message)

    async def stop(self) -> None:
        """Gracefully stop this actor."""

        _ = self._task.cancel()
        with suppress(CancelledError):
            await self._task
        self._inbox.shutdown()


__all__ = ["ActorRef"]
