from abc import ABC, abstractmethod
from asyncio import Future
from typing import Generic, Self, TypeVar, final

from tractor.actor import Actor


@abstractmethod
class Message[A: Actor, R](ABC):
    """The base class for the messages an Actor processes."""

    @abstractmethod
    async def reply(self, actor: A) -> R:
        """Compute the reply for this message."""
        ...

    def responder(self) -> Responder[A, R]:
        """Get the responder for this message."""
        return Responder(self)


A = TypeVar("A", bound=Actor)
R = TypeVar("R", covariant=True)


@final
class Responder(Generic[A, R]):
    """
    A container for correlating a message with its response.

    Due to variance rules, we have to control the generics for this type
    manually. This allows it to bind the message with its reply R during
    construction, but allow type erasure as a ``Responder[A, object]`` so that it can
    be placed in the actor's inbox. Since the inbox driver calls ``Responder.respond``,
    we maintain the guarantee that the produced response is of type ``R``, even after
    erasing ``R`` to ``object``.

    It also controls the relationship between the message and the Future object,
    so that replies always go to the right client.
    """

    def __init__(self, message: Message[A, R]):
        """
        Generate a responder for this message.

        Call one of ``tell` or ``ask`` to chain configuration.

        :param message: the message
        """
        self._message = message
        self._reply: Future[R] | None = None

    def tell(self) -> Self:
        """Configure this responder to tell only."""
        return self

    def ask(self) -> tuple[Self, Future[R]]:
        """
        Configure this responder to request a reply.

        :return: the responder and the future to await the reply
        """
        reply = Future[R]()
        self._reply = reply
        return self, reply

    async def respond(self, actor: A) -> None:
        """
        Respond to this message.

        :param actor: the actor whose state should be passed to ``Message.reply``
        """
        response = await self._message.reply(actor)
        if self._reply:
            self._reply.set_result(response)


__all__ = ["Message", "Responder"]
