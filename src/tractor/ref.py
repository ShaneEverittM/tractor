"""The definition of the ``ActorRef`` class, for interacting with ``Actor``s."""

import asyncio
from asyncio import CancelledError
from contextlib import suppress
from typing import final

from tractor.actor import Actor
from tractor.inbox import Inbox
from tractor.message import Context, Message
from tractor.request import AskRequest, TellRequest


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
        while True:
            responder = await self._actor.step(self._inbox)
            if responder is not None:
                await responder.respond(self._actor, Context(self))

    def ask[R](self, message: Message[A, R]) -> AskRequest[A, R]:
        """
        Ask this actor to process a message.

        See the configuration methods on ``AskRequest`` to
        control things like timeouts.

        :param message: the message to send
        :return: an ``Awaitable`` to configure transmission and retrieve the reply
        """
        return AskRequest(self._inbox, message)

    def tell[R](self, message: Message[A, R]) -> TellRequest[A, R]:
        """
        Tell this actor to process a message.

        See the configuration methods on ``AskRequest`` to
        control things like timeouts.

        :param message: the message to send
        :return: a ``Awaitable`` to configure transmission
        """
        return TellRequest(self._inbox, message)

    async def stop(self) -> None:
        """Gracefully stop this actor."""

        _ = self._task.cancel()
        with suppress(CancelledError):
            await self._task
        self._inbox.shutdown()


__all__ = ["ActorRef"]
