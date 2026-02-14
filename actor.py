"""A delightfully simple actor system."""

import asyncio
from abc import ABC, abstractmethod
from asyncio import Future, Queue
from collections.abc import Awaitable, Generator
from typing import Self, cast, final, override


@abstractmethod
class Message[A, R](ABC):
    """The base class for messages an Actor processes."""

    @abstractmethod
    async def reply(self, actor: A) -> R:
        """Compute the reply for this message."""
        ...


class Actor(ABC):
    def ref(self) -> ActorRef[Self]:
        return ActorRef(self)

    @abstractmethod
    def accepts[R](self, message: type[Message[Self, R]]) -> bool:
        return False


@final
class Inbox[A: Actor](Queue[tuple[Message[A, object], Future[object] | None]]):
    def __init__(self, actor: A):
        super().__init__()
        self.actor = actor

    def _filter[R](self, message: Message[A, R]):
        if not self.actor.accepts(type(message)):
            raise TypeError(
                f"Actor {self.actor} does not accept message {type(message)}"
            )

    async def ask[R](
        self, message: Message[A, R], timeout: float | None = None
    ) -> Future[R]:
        self._filter(message)

        reply = Future[object]()
        put = self.put((message, reply))
        await asyncio.wait_for(put, timeout)
        return cast(Future[R], reply)

    def try_ask(self, message: Message[A, object]) -> Future[object] | None:
        self._filter(message)

        reply = Future[object]()
        self.put_nowait((message, reply))
        return reply

    async def tell(self, message: Message[A, object]) -> None:
        self._filter(message)

        await self.put((message, None))

    def try_tell(self, message: Message[A, object]) -> None:
        self._filter(message)

        self.put_nowait((message, None))


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
        return cast(Future[R], reply)

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


@final
class ActorRef[A: Actor]:
    def __init__(self, actor: A):
        self.actor = actor
        self.inbox = Inbox[A](actor)
        self.task = asyncio.create_task(self.driver())

    async def driver(self):
        message, reply = await self.inbox.get()
        response = await message.reply(self.actor)
        if reply:
            reply.set_result(response)

    def ask[R](self, message: Message[A, R]) -> AskRequest[A, R]:
        return AskRequest(self.inbox, message)

    def tell[R](self, message: Message[A, R]) -> TellRequest[A, R]:
        return TellRequest(self.inbox, message)
