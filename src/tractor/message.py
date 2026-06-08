from abc import ABC, abstractmethod
from asyncio import Future
from collections.abc import Awaitable, Callable
from typing import Generic, Self, TypeVar, final, TYPE_CHECKING

from tractor.actor import Actor

if TYPE_CHECKING:
    from tractor.ref import ActorRef


# TODO: Make this a Protocol so it can't be constructed directly; it should
#       only ever come from Message.sender.


@final
class Sender[M, R]:
    """
    A handle that sends exactly one message type ``M`` and yields its reply ``R``.

    It is built by ``Message.sender`` from an ``ActorRef[A]``, capturing that
    ref's bound ``ask`` while the ``M``/``A``/``R`` relationship is still known.
    Storing the closure rather than the ref lets ``send`` stay fully type-safe:
    the actor type ``A`` does not need to leak into ``Sender``'s parameters, yet
    no unsound cast is required.
    """

    def __init__(self, send: Callable[[M], Awaitable[R]]):
        self._send = send

    async def send(self, message: M) -> R:
        return await self._send(message)


@final
class Context[A: Actor]:
    def __init__(self, actor: ActorRef[A]):
        self._actor = actor

    @property
    def ref(self) -> ActorRef[A]:
        return self._actor


class Message[A: Actor, R](ABC):
    """
    The base class for the messages an ``Actor`` processes.

    A ``Message`` is a typed envelope: it names the actor type ``A`` it targets
    and the reply type ``R`` it produces. Its ``dispatch`` method should hold
    only routing — delegating to a method on ``A`` that owns the behavior and
    state — rather than the business logic itself.
    """

    @abstractmethod
    async def dispatch(self, actor: A, ctx: Context[A]) -> R:
        """
        Route this message to its handler on ``actor`` and return the reply.

        Keep this thin: call a method on ``actor`` that does the real work. The
        result is checked against ``R``, so a handler whose return type doesn't
        match the message's declared reply type is a type error right here.
        """
        ...

    @classmethod
    def sender(cls, actor: ActorRef[A]) -> Sender[Self, R]:
        return Sender(actor.ask)

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

        :param actor: the actor whose state should be passed to ``Message.dispatch``
        :param ctx: the context
        """
        response = await self._message.dispatch(actor, ctx)
        if self._reply:
            self._reply.set_result(response)


__all__ = ["Message", "Responder", "Sender", "Context"]
