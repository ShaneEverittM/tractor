from abc import ABC, abstractmethod
from asyncio import CancelledError, Future
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Generic, Never, Self, TypeVar, cast, final, override

from tractor.actor import Actor
from tractor.errors import ActorStoppedError
from tractor.protocols import MessagePort, RuntimeLike

if TYPE_CHECKING:
    from tractor.ref import ActorRef


# TODO: Make this a Protocol so it can't be constructed directly; it should
#       only ever come from Message.sender.


@final
class Sender[M, R]:
    """
    A handle that sends exactly one message type `M` and yields its reply `R`.

    Built by `Message.sender` from a `MessagePort` and an `ActorRef[A]`,
    capturing a closure over the port's `ask` while the `M`/`A`/`R`
    relationship is still known. Storing the closure rather than the ref lets
    `send` stay fully type-safe: the actor type `A` does not need to leak
    into `Sender`'s parameters, yet no unsound cast is required.

    `send` awaits the full round trip — it returns only once the recipient
    has processed the message. Where waiting would couple your progress to
    the recipient's (completion notifications, fan-out), use the
    tell-flavored `TellSender` instead.
    """

    def __init__(self, send: Callable[[M], Awaitable[R]]):
        self._send = send

    async def send(self, message: M) -> R:
        return await self._send(message)


@final
class TellSender[M]:
    """
    A handle that sends exactly one message type `M` without awaiting replies.

    The tell-flavored counterpart to `Sender`, built by `Message.teller`:
    `send` returns once the message is *enqueued*, not once it is processed,
    so a slow or busy recipient never blocks the sending side.
    """

    def __init__(self, send: Callable[[M], Awaitable[None]]):
        self._send = send

    async def send(self, message: M) -> None:
        await self._send(message)


@final
class Context[A: Actor](MessagePort):
    """
    The handler-side view of the runtime, passed into every `dispatch`.

    Sends made through it (`tell` / `ask` / `forward`) route to the `Runtime`
    with sender identity implicit from the context. As a `MessagePort`, it
    can also back `Message.sender` / `Message.teller` handles built inside a
    handler.
    """

    def __init__(self, actor: ActorRef[A]):
        self._actor = actor

    @property
    def ref(self) -> ActorRef[A]:
        return self._actor

    @override
    async def tell[B: Actor, R](
        self, target: ActorRef[B], message: Message[B, R]
    ) -> None:
        """Send `message` to `target` without waiting for a reply."""
        await self._actor._runtime.tell(target, message)  # pyright: ignore[reportPrivateUsage]

    @override
    async def ask[B: Actor, R](self, target: ActorRef[B], message: Message[B, R]) -> R:
        """Send `message` to `target` and wait for the reply."""
        return await self._actor._runtime.ask(target, message)  # pyright: ignore[reportPrivateUsage]

    def forward[B: Actor, R](self, target: ActorRef[B], message: Message[B, R]) -> R:
        """
        Delegate this handler's reply to `target`.

        `return` (do **not** `await`) the result from a `dispatch` handler
        to hand the current ask's reply off to `target`: `message` is sent to
        `target` and *its* reply — value or exception — is delivered straight to
        the original caller's future. The delegating actor does not block waiting
        for that reply; it is free to process its next message immediately.

        This returns an opaque proxy that the driver recognizes when `dispatch`
        returns. The declared `-> R` is a contained cast (the proxy is not
        really an `R`); it lets a handler `return ctx.forward(...)` while the
        type checker still enforces that `message`'s reply type matches the
        handler's declared reply type `R`.
        """
        return cast(R, _Forward(target, message))

    @property
    def runtime(self) -> RuntimeLike:
        return self._actor.runtime

    async def _forward[B: Actor, R](
        self, fwd: _Forward[B, R], reply: Future[R] | None
    ) -> None:
        """Carry out a `forward` directive returned from `dispatch`.

        Routes the forwarded send through the runtime (keeping it observable) and
        links the target's reply future into the original caller's `reply`.
        Invoked by `Responder.respond`; not part of the public handler API.
        """
        if reply is None:
            # The original ask was a tell: just deliver the forwarded message.
            await self.runtime.tell(fwd.target, fwd.message)
            return
        try:
            source = await self.runtime.forward(fwd.target, fwd.message)
        except BaseException as exc:
            # Forwarding to a stopped/full target must not crash this actor; the
            # original caller sees the failure instead.
            if not reply.done():
                reply.set_exception(exc)
            return
        _link_reply(source, reply)

    def spawn[B: Actor](
        self,
        actor: B,
        *,
        capacity: int | None = None,
    ) -> ActorRef[B]:
        return self.runtime.spawn(actor, capacity=capacity)


class Message[A: Actor, R](ABC):
    """
    The base class for the messages an `Actor` processes.

    A `Message` is a typed envelope: it names the actor type `A` it targets
    and the reply type `R` it produces. Its `dispatch` method should hold
    only routing — delegating to a method on `A` that owns the behavior and
    state — rather than the business logic itself.
    """

    @abstractmethod
    async def dispatch(self, actor: A, ctx: Context[A]) -> R:
        """
        Route this message to its handler on `actor` and return the reply.

        Keep this thin: call a method on `actor` that does the real work. The
        result is checked against `R`, so a handler whose return type doesn't
        match the message's declared reply type is a type error right here.
        """
        ...

    @classmethod
    def sender(cls, port: MessagePort, ref: ActorRef[A]) -> Sender[Self, R]:
        """
        Build an ask-flavored send handle for this message type, aimed at `ref`.

        `port` is whatever the send should be routed through: the `Runtime`
        when building from the outside, or the handler's `Context` when
        building inside an actor — the latter keeps the send attributed to
        the sending actor.
        """
        return Sender(lambda msg: port.ask(ref, msg))

    @classmethod
    def teller(cls, port: MessagePort, ref: ActorRef[A]) -> TellSender[Self]:
        """
        Build a tell-flavored send handle for this message type, aimed at `ref`.

        Like `sender`, but the handle's `send` returns as soon as the message
        is enqueued instead of waiting for the reply. `port` is the `Runtime`
        or, inside a handler, the `Context` (which keeps sender attribution).
        """
        return TellSender(lambda msg: port.tell(ref, msg))

    def responder(self) -> Responder[A, R]:
        """Get the responder for this message."""
        return Responder(self)


A = TypeVar("A", bound=Actor, contravariant=True)
R = TypeVar("R", covariant=True)


@final
class Responder(Generic[A, R]):
    """
    A container for correlating a message with its response.

    Due to variance rules, we have to control the generics for this type
    manually. This allows it to bind the message with its reply R during
    construction, but allow type erasure as a `Responder[A, object]` so that it can
    be placed in the actor's inbox. Since the inbox driver calls `Responder.respond`,
    we maintain the guarantee that the produced response is of type `R`, even after
    erasing `R` to `object`.

    It also controls the relationship between the message and the Future object,
    so that replies always go to the right client.
    """

    def __init__(self, message: Message[A, R]):
        """
        Generate a responder for this message.

        Call one of `tell` or `ask` to chain configuration.

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

        :param actor: the actor whose state should be passed to `Message.dispatch`
        :param ctx: the context
        """
        try:
            response = await self._message.dispatch(actor, ctx)
        except CancelledError:
            # The driver is being cancelled (`ActorRef.stop`) mid-dispatch. To
            # the caller that means "the actor stopped", so resolve the reply
            # with `ActorStoppedError` rather than transferring the
            # cancellation onto their task; the raise still hands the
            # `CancelledError` back to the driver for its own teardown.
            self.set_stopped()
            raise
        except BaseException as exc:
            if self._reply is not None and not self._reply.done():
                self._reply.set_exception(exc)
            raise
        else:
            if isinstance(response, _Forward):
                # The handler delegated its reply; hand it off rather than
                # resolving the caller's future with the proxy itself. The
                # cast undoes the one in `Context.forward`, which is the only
                # place a `_Forward` can enter a dispatch's return value — its
                # reply type is guaranteed there to be the handler's `R`.
                fwd = cast("_Forward[Actor, R]", response)
                reply, self._reply = self._reply, None
                await ctx._forward(fwd, reply)  # pyright: ignore[reportPrivateUsage]
                return
            if self._reply is not None and not self._reply.done():
                self._reply.set_result(response)

    def set_stopped(self) -> None:
        """Resolve the reply future with `ActorStoppedError` (inbox drain helper)."""
        if self._reply is not None and not self._reply.done():
            self._reply.set_exception(ActorStoppedError())


@final
class _Forward[A: Actor, R]:
    """
    An opaque directive returned from a handler to delegate its reply.

    Carries the `target` and the `message` to send it. `Context.forward`
    constructs one (cast to the reply type `R`); `Responder.respond`
    recognizes it and routes the handoff through `Context._forward`. Users
    never construct or name this type directly.

    Deliberately *not* sharing `Responder`'s contravariant `A`: the public
    `target`/`message` attributes put `A` in positions where contravariance
    would be unsound, and only invariance (inferred here) is honest.
    """

    def __init__(self, target: ActorRef[A], message: Message[A, R]):
        self.target = target
        self.message = message


def _link_reply[T](source: Future[T], reply: Future[T]) -> None:
    """Copy `source`'s eventual result, exception, or cancellation into `reply`."""

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


type AnyResponder = Responder[Never, object]
"""
The maximally permissive Responder type.

Since A is contravariant and R is covariant, erasing each to its
"accepts everything" extreme means the *bottom* of the hierarchy (`Never`)
for A and the *top* (`object`) for R; every concrete `Responder[A, R]` is
assignable to this.
"""


__all__ = ["AnyResponder", "Message", "Responder", "Sender", "TellSender", "Context"]
