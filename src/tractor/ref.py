"""The definition of the ``ActorRef`` class, for interacting with ``Actor``s."""

import asyncio
from asyncio import CancelledError
from contextlib import suppress
from typing import final

from tractor.actor import Actor
from tractor.inbox import Inbox
from tractor.message import Message
from tractor.request import AskRequest, TellRequest


@final
class ActorRef[A: Actor]:
    """A handle to an actor of type ``A``."""

    def __init__(self, actor: A):
        """
        Wrap an actor in an ``ActorRef`` object.

        :param actor: the actor to wrap
        """
        self._actor = actor
        self._inbox = Inbox[A]()
        self._task = asyncio.create_task(self._driver())

    async def _driver(self) -> None:
        responder = await self._inbox.get()
        await responder.respond(self._actor)

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
