"""The definition of an actor's message box."""

import asyncio
from asyncio import Future, Queue
from typing import final

from tractor.actor import Actor
from tractor.message import Message, Responder


@final
class Inbox[A: Actor](Queue[Responder[A, object]]):
    """
    The inbox of an ``Actor``.

    This is a specialization of ``asyncio.Queue`` with methods
    that enqueue and create replies atomically.
    """

    def __init__(self, capacity: int | None = None):
        """
        Create an inbox.

        :param capacity: the most messages that may wait unprocessed before
            senders block (or, for the ``try_*`` variants, are rejected with
            ``asyncio.QueueFull``); ``None`` leaves the inbox unbounded
        """
        super().__init__(maxsize=capacity if capacity is not None else 0)

    async def ask[R](
        self, message: Message[A, R], timeout: float | None = None
    ) -> Future[R]:
        """
        Enqueue a message, waiting for capacity and its reply.

        :param message: the message to enqueue
        :param timeout: how long to wait for capacity in the queue
        :return: a future that will resolve to the reply
        """
        responder, handle = Responder(message).ask()
        put = self.put(responder)
        await asyncio.wait_for(put, timeout)
        return handle

    def try_ask[R](self, message: Message[A, R]) -> Future[R]:
        """
        Try to enqueue a message, then wait for its reply.

        :param message: the message to enqueue
        :return: a future that will resolve to the reply
        :raises QueueFull if no capacity is available
        """
        responder, reply = Responder(message).ask()
        self.put_nowait(responder)
        return reply

    async def tell[R](self, message: Message[A, R]) -> None:
        """
        Enqueue a message, waiting for capacity.

        :param message: the message to enqueue
        """
        responder = Responder(message).tell()
        await self.put(responder)

    def try_tell[R](self, message: Message[A, R]) -> None:
        """
        Try to enqueue a message.

        :param message: the message to enqueue
        :raises QueueFull if no capacity is available
        """
        responder = Responder(message).tell()
        self.put_nowait(responder)


__all__ = ["Inbox"]
