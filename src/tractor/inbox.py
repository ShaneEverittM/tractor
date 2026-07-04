"""The definition of an actor's message box."""

import asyncio
from asyncio import Future, Queue
from typing import final, override

from tractor.actor import Actor
from tractor.errors import ActorStoppedError
from tractor.message import AnyResponder, Message, Responder


@final
class Inbox(Queue[AnyResponder]):
    """
    The inbox of an `Actor`.

    This is a specialization of `asyncio.Queue` with methods
    that enqueue and create replies atomically.

    Enqueueing into a shut-down inbox means the actor has stopped, so the
    `put` overrides translate `asyncio.QueueShutDown` — an internal detail of
    the queue — into `ActorStoppedError`, the exception callers are told to
    expect. Every send path (`ask`, `tell`, and the `try_*` variants) funnels
    through one of them.
    """

    def __init__(self, capacity: int | None = None):
        """
        Create an inbox.

        :param capacity: the most messages that may wait unprocessed before
            senders block (or, for the `try_*` variants, are rejected with
            `asyncio.QueueFull`); `None` leaves the inbox unbounded
        """
        super().__init__(maxsize=capacity if capacity is not None else 0)

    @override
    async def put(self, item: AnyResponder) -> None:
        """
        Enqueue `item`, waiting for capacity.

        :raises ActorStoppedError: if the actor has stopped
        """
        try:
            await super().put(item)
        except asyncio.QueueShutDown:
            raise ActorStoppedError() from None

    @override
    def put_nowait(self, item: AnyResponder) -> None:
        """
        Enqueue `item` if capacity is available now.

        :raises QueueFull: if no capacity is available
        :raises ActorStoppedError: if the actor has stopped
        """
        try:
            super().put_nowait(item)
        except asyncio.QueueShutDown:
            raise ActorStoppedError() from None

    async def ask[A: Actor, R](self, message: Message[A, R]) -> Future[R]:
        """
        Enqueue a message, waiting for capacity and its reply.

        :param message: the message to enqueue
        :return: a future that will resolve to the reply
        :raises ActorStoppedError: if the actor has stopped
        """
        responder, handle = Responder(message).ask()
        await self.put(responder)
        return handle

    def try_ask[A: Actor, R](self, message: Message[A, R]) -> Future[R]:
        """
        Try to enqueue a message, then wait for its reply.

        :param message: the message to enqueue
        :return: a future that will resolve to the reply
        :raises QueueFull: if no capacity is available
        :raises ActorStoppedError: if the actor has stopped
        """
        responder, reply = Responder(message).ask()
        self.put_nowait(responder)
        return reply

    async def tell[A: Actor, R](self, message: Message[A, R]) -> None:
        """
        Enqueue a message, waiting for capacity.

        :param message: the message to enqueue
        :raises ActorStoppedError: if the actor has stopped
        """
        responder = Responder(message).tell()
        await self.put(responder)

    def try_tell[A: Actor, R](self, message: Message[A, R]) -> None:
        """
        Try to enqueue a message.

        :param message: the message to enqueue
        :raises QueueFull: if no capacity is available
        :raises ActorStoppedError: if the actor has stopped
        """
        responder = Responder(message).tell()
        self.put_nowait(responder)

    def drain(self) -> None:
        """
        Drain all pending responders, resolving reply futures with `ActorStoppedError`.

        Called from the driver's `finally` block and from `ActorRef.stop()`
        to ensure waiting `ask` callers are unblocked rather than hanging.
        """
        while True:
            try:
                self.get_nowait().set_stopped()
            except asyncio.QueueEmpty, asyncio.QueueShutDown:
                break


__all__ = ["Inbox"]
