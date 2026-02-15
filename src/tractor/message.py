from abc import ABC, abstractmethod
from asyncio import Future
from typing import Generic, Self, TypeVar, final

from tractor.actor import Actor


@abstractmethod
class Message[A: Actor, R](ABC):
    """The base class for messages an Actor processes."""

    @abstractmethod
    async def reply(self, actor: A) -> R:
        """Compute the reply for this message."""
        ...

    def responder(self) -> Responder[A, R]:
        return Responder(self)


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
