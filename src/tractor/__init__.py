"""A delightfully simple, type-safe actor system."""

import asyncio
from abc import ABC, abstractmethod
from asyncio import CancelledError, Future, Queue
from collections.abc import Awaitable, Generator
from contextlib import suppress
from typing import Generic, Self, TypeVar, cast, final, override


@abstractmethod
class Message[A: Actor, R](ABC):
    """The base class for messages an Actor processes."""

    @abstractmethod
    async def reply(self, actor: A) -> R:
        """Compute the reply for this message."""
        ...


class Actor:
    def ref(self) -> ActorRef[Self]:
        """Get an ActorRef for this actor."""
        return ActorRef(self)


A = TypeVar("A", bound=Actor)
R = TypeVar("R", covariant=True)


@final
class Responder(Generic[A, R]):
    """
    A container for correlating a message with its response.

    Due to variance rules, we have to manually create the generics for this type
    in order to allow it to concretely bind the message with its reply R during
    construction, but allow type erasue as a Responder[A, object] so that it can
    be placed in the actor's inbox. Since the inbox driver calls Responder.respond,
    we maintain the gaurantee that the produced response is of type R, even after
    erasing R to object.

    It also controlls the relationship between the message and the Future object,
    so that replies always go to the right client.
    """

    def __init__(self, message: Message[A, R]):
        self._message = message
        self._reply: Future[R] | None = None

    def tell(self) -> Self:
        return self

    def ask(self) -> tuple[Self, Future[R]]:
        reply = Future[R]()
        self._reply = reply
        return self, reply

    async def respond(self, actor: A) -> None:
        response = await self._message.reply(actor)
        if self._reply:
            self._reply.set_result(response)


@final
class Inbox[A: Actor](Queue[Responder[A, object]]):
    def __init__(self, actor: A):
        super().__init__()
        self._actor = actor

    async def ask[R](
        self, message: Message[A, R], timeout: float | None = None
    ) -> Future[R]:
        responder, handle = Responder(message).ask()
        put = self.put(responder)
        await asyncio.wait_for(put, timeout)
        return handle

    def try_ask[R](self, message: Message[A, R]) -> Future[R] | None:
        responder, reply = Responder(message).ask()
        self.put_nowait(responder)
        return reply

    async def tell[R](self, message: Message[A, R]) -> None:
        responder = Responder(message).tell()
        await self.put(responder)

    def try_tell[R](self, message: Message[A, R]) -> None:
        responder = Responder(message).tell()
        self.put_nowait(responder)


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
        responder = await self.inbox.get()
        await responder.respond(self.actor)

    def ask[R](self, message: Message[A, R]) -> AskRequest[A, R]:
        return AskRequest(self.inbox, message)

    def tell[R](self, message: Message[A, R]) -> TellRequest[A, R]:
        return TellRequest(self.inbox, message)

    def stop(self):
        self.task.cancel()
        with suppress(CancelledError):
            await self.task
        self.inbox.shutdown()
