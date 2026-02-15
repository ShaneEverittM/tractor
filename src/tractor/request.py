"""Definitions of the requests types."""

from asyncio import Future
from collections.abc import Awaitable, Generator
from typing import Self, final, override

from tractor.actor import Actor
from tractor.inbox import Inbox
from tractor.message import Message


class Request[A: Actor, R]:
    """The base class to all kinds of actor requests."""

    def __init__(self, message: Message[A, R]) -> None:
        self._message: Message[A, R] | None = message
        self._inbox_timeout: float | None = None

    def _take_message(self) -> Message[A, R]:
        assert self._message is not None, "Message taken twice!"
        message = self._message
        self._message = None
        return message

    def with_inbox_timeout(self, seconds: float) -> Self:
        """
        Set the timeout.

        This is a chainable configuration method.

        :param seconds: how long to wait for capacity in the actor's inbox
        :return: this same request object
        """
        self._inbox_timeout = seconds
        return self


@final
class AskRequest[A: Actor, R](Awaitable[R], Request[A, R]):
    """A request that waits for a reply."""

    def __init__(
        self,
        inbox: Inbox[A],
        message: Message[A, R],
    ) -> None:
        """
        Construct a new ``AskRequest``.

        :param inbox: the inbox in which to send the message
        :param message: what message to send
        """
        super().__init__(message)

        self._inbox = inbox
        self._reply: Future[R] | None = None
        self._reply_timeout: float | None = None

    async def _enqueue(self) -> Future[R]:
        message = self._take_message()
        reply = await self._inbox.ask(message, self._inbox_timeout)
        return reply

    def _try_enqueue(self) -> Future[R]:
        message = self._take_message()
        reply = self._inbox.try_ask(message)
        return reply

    async def enqueue(self) -> Self:
        """Enqueue the message, but don't wait for a reply yet."""
        self._reply = await self._enqueue()
        return self

    def try_enqueue(self) -> Self:
        self._reply = self._try_enqueue()
        return self

    async def ask(self) -> R:
        """
        Send this ask request and wait for the reply.

        This is identical to simply awaiting the object directly.

        :return: the reply
        """
        reply = self._reply or await self._enqueue()
        return await reply

    def try_ask(self) -> Future[R]:
        """
        Send this ask request without waiting for inbox capacity.

        :return: the reply
        :raises ``asyncio.QueueFull`` if there is no capacity
        """
        reply = self._reply or self._try_enqueue()
        return reply

    @override
    def __await__(self) -> Generator[None, None, R]:
        return self.ask().__await__()


@final
class TellRequest[A: Actor, R](Awaitable[None], Request[A, R]):
    """A request that does not wait for a reply."""

    def __init__(
        self,
        inbox: Inbox[A],
        message: Message[A, R],
    ) -> None:
        """
        Construct a new ``TellRequest``.

        :param inbox: the inbox in which to send the message
        :param message: what message to send
        """

        super().__init__(message)

        self._message: Message[A, R] | None = message
        self._inbox = inbox

    async def send(self) -> None:
        """
        Send this request, waiting for inbox capacity.

        This is identical to simply awaiting the object directly.
        """
        message = self._take_message()
        await self._inbox.tell(message)

    def try_send(self) -> None:
        """
        Try to send this request, without waiting for inbox capacity.

        :raises QueueFull: if the inbox capacity is reached
        """
        message = self._take_message()
        self._inbox.try_tell(message)

    @override
    def __await__(self) -> Generator[None, None, None]:
        return self.send().__await__()
