from abc import ABC, abstractmethod
from asyncio import Future
from typing import Generic, Self, TypeVar, final, TYPE_CHECKING

from tractor.actor import Actor

if TYPE_CHECKING:
    from tractor.ref import ActorRef


# TODO: Make this a protocol, and make the type inside the sender
#       method so users can't make it.


@final
class Sender[M, R]:
    def __init__[A: Actor](self, actor: ActorRef[A]):
        self.actor = actor

    async def send(self, message: M) -> R:
        # SAFETY: We know we only construct this from Message.sender,
        # which guarantees we create an object whose generic M is inferred
        # from said message. With that in mind, callers of Sender.send will
        # get a type error if they don't send that specific message.
        # Furthermore, since we took in an ActorRef[A] that was the same
        # generic A that the message targeted, we know this actor accepts this message.
        return await self.actor.ask(message)  # pyright: ignore[reportUnknownVariableType, reportArgumentType]


@final
class Context[A: Actor]:
    def __init__(self, actor: ActorRef[A]):
        self._actor = actor

    @property
    def ref(self) -> ActorRef[A]:
        return self._actor


@abstractmethod
class Message[A: Actor, R](ABC):
    """The base class for the messages an Actor processes."""

    @abstractmethod
    async def reply(self, actor: A, ctx: Context[A]) -> R:
        """Compute the reply for this message."""
        ...

    @classmethod
    def sender(cls, actor: ActorRef[A]) -> Sender[Self, R]:
        return Sender(actor)

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

    async def respond(self, actor: A, ctx: Context[A]) -> None:
        """
        Respond to this message.

        :param actor: the actor whose state should be passed to ``Message.reply``
        :param ctx: the context
        """
        response = await self._message.reply(actor, ctx)
        if self._reply:
            self._reply.set_result(response)


__all__ = ["Message", "Responder", "Sender", "Context"]
