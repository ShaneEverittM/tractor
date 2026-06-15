from abc import ABC, abstractmethod
from asyncio import Future
from collections.abc import Awaitable, Callable
from typing import Generic, Self, TypeVar, cast, final, TYPE_CHECKING

from tractor.actor import Actor

if TYPE_CHECKING:
    from tractor.ref import ActorRef, _RuntimeLike  # pyright: ignore[reportPrivateUsage]


# TODO: Make this a Protocol so it can't be constructed directly; it should
#       only ever come from Message.sender.


@final
class Sender[M, R]:
    """
    A handle that sends exactly one message type ``M`` and yields its reply ``R``.

    Built by ``Message.sender`` from a ``Runtime`` and an ``ActorRef[A]``,
    capturing a closure over ``runtime.ask`` while the ``M``/``A``/``R``
    relationship is still known. Storing the closure rather than the ref lets
    ``send`` stay fully type-safe: the actor type ``A`` does not need to leak
    into ``Sender``'s parameters, yet no unsound cast is required.
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

    async def tell[B: Actor, R](
        self, target: ActorRef[B], message: Message[B, R]
    ) -> None:
        """Send ``message`` to ``target`` without waiting for a reply."""
        await self._actor._runtime.tell(target, message)  # pyright: ignore[reportPrivateUsage]

    async def ask[B: Actor, R](self, target: ActorRef[B], message: Message[B, R]) -> R:
        """Send ``message`` to ``target`` and wait for the reply."""
        return await self._actor._runtime.ask(target, message)  # pyright: ignore[reportPrivateUsage]

    def forward[B: Actor, R](self, target: ActorRef[B], message: Message[B, R]) -> R:
        """
        Delegate this handler's reply to ``target``.

        ``return`` (do **not** ``await``) the result from a ``dispatch`` handler
        to hand the current ask's reply off to ``target``: ``message`` is sent to
        ``target`` and *its* reply — value or exception — is delivered straight to
        the original caller's future. The delegating actor does not block waiting
        for that reply; it is free to process its next message immediately.

        This returns an opaque proxy that the driver recognizes when ``dispatch``
        returns. The declared ``-> R`` is a contained cast (the proxy is not
        really an ``R``); it lets a handler ``return ctx.forward(...)`` while the
        type checker still enforces that ``message``'s reply type matches the
        handler's declared reply type ``R``.
        """
        return cast(R, _Forward(target, message))

    async def _forward[B: Actor, R](
        self, fwd: _Forward[B, R], reply: Future[R] | None
    ) -> None:
        """Carry out a ``forward`` directive returned from ``dispatch``.

        Routes the forwarded send through the runtime (keeping it observable) and
        links the target's reply future into the original caller's ``reply``.
        Invoked by ``Responder.respond``; not part of the public handler API.
        """
        runtime = self._actor._runtime  # pyright: ignore[reportPrivateUsage]
        if reply is None:
            # The original ask was a tell: just deliver the forwarded message.
            await runtime.tell(fwd._target, fwd._message)
            return
        try:
            source = await runtime.forward(fwd._target, fwd._message)
        except BaseException as exc:
            # Forwarding to a stopped/full target must not crash this actor; the
            # original caller sees the failure instead.
            if not reply.done():
                reply.set_exception(exc)
            return
        _link_reply(source, reply)


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
    def sender(cls, runtime: _RuntimeLike, ref: ActorRef[A]) -> Sender[Self, R]:
        return Sender(lambda msg: runtime.ask(ref, msg))

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
        try:
            response = await self._message.dispatch(actor, ctx)
        except BaseException as exc:
            if self._reply is not None and not self._reply.done():
                self._reply.set_exception(exc)
            raise
        else:
            if isinstance(response, _Forward):
                # The handler delegated its reply; hand it off rather than
                # resolving the caller's future with the proxy itself.
                reply, self._reply = self._reply, None
                await ctx._forward(response, reply)
                return
            if self._reply is not None and not self._reply.done():
                self._reply.set_result(response)

    def set_stopped(self) -> None:
        """Resolve the reply future with ``ActorStoppedError`` (inbox drain helper)."""
        from tractor.errors import ActorStoppedError

        if self._reply is not None and not self._reply.done():
            self._reply.set_exception(ActorStoppedError())


@final
class _Forward(Generic[A, R]):
    """
    An opaque directive returned from a handler to delegate its reply.

    Carries the ``target`` and the ``message`` to send it. ``Context.forward``
    constructs one (cast to the reply type ``R``); ``Responder.respond``
    recognizes it and routes the handoff through ``Context._forward``. Users
    never construct or name this type directly.
    """

    def __init__(self, target: ActorRef[A], message: Message[A, R]):
        self._target = target
        self._message = message


def _link_reply[T](source: Future[T], reply: Future[T]) -> None:
    """Copy ``source``'s eventual result, exception, or cancellation into ``reply``."""

    def _on_done(done: Future[T]) -> None:
        if reply.done():
            return
        if done.cancelled():
            _ = reply.cancel()
            return
        exc = done.exception()
        if exc is not None:
            reply.set_exception(exc)
        else:
            reply.set_result(done.result())

    source.add_done_callback(_on_done)


__all__ = ["Message", "Responder", "Sender", "Context"]
