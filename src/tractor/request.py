from asyncio import Future
from collections.abc import Awaitable, Generator
from typing import Self, final, override

from tractor.actor import Actor
from tractor.inbox import Inbox
from tractor.message import Message


class Request[A: Actor, R]:
    def __init__(self, message: Message[A, R]) -> None:
        self._message: Message[A, R] | None = message
        self._inbox_timeout: float | None = None

    def _take_message(self) -> Message[A, R]:
        assert self._message is not None, "Message taken twice!"
        message = self._message
        self._message = None
        return message

    def with_inbox_timout(self, seconds: float) -> Self:
        self._inbox_timeout = seconds
        return self


@final
class AskRequest[A: Actor, R](Awaitable[R], Request[A, R]):
    def __init__(
        self,
        inbox: Inbox[A],
        message: Message[A, R],
    ) -> None:
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
        self._reply = await self._enqueue()
        return self

    def try_enqueue(self) -> Self:
        self._reply = self._try_enqueue()
        return self

    async def send(self) -> R:
        reply = self._reply or await self._enqueue()
        return await reply

    def try_send(self) -> Future[R]:
        reply = self._reply or self._try_enqueue()
        return reply

    @override
    def __await__(self) -> Generator[None, None, R]:
        return self.send().__await__()


@final
class TellRequest[A: Actor, R](Awaitable[None], Request[A, R]):
    def __init__(
        self,
        inbox: Inbox[A],
        message: Message[A, R],
    ) -> None:
        super().__init__(message)

        self._message: Message[A, R] | None = message
        self._inbox = inbox

    async def send(self) -> None:
        message = self._take_message()
        await self._inbox.tell(message)

    def try_send(self) -> None:
        message = self._take_message()
        self._inbox.try_tell(message)

    @override
    def __await__(self) -> Generator[None, None, None]:
        return self.send().__await__()
