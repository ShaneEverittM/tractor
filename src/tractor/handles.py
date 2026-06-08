"""The driver-facing handles passed to and returned from ``Actor.step``.

These are deliberately *not* parameterized by the actor type — they are the
type-erased, receive/respond-only handles onto an ``Inbox`` and its
``Responder``s. Keeping the ``step`` boundary non-generic is what lets actors
override it with a precise, ignore-free signature: all of the actor-typed
bridging (pairing a received ``Responder`` with the actor and its context)
happens inside the driver, where it is fully type-checked. See
``ActorRef._driver``.
"""

from collections.abc import Awaitable, Callable
from typing import final


@final
class ResponderHandle:
    """
    A type-erased handle onto a single ``Responder``.

    Returned from :meth:`tractor.Actor.step` — directly, or after winning a
    ``select`` — for the driver to run. The actor and context the response needs
    are already sealed inside, so :meth:`respond` takes no arguments; a ``step``
    override just passes the handle through without inspecting it.
    """

    def __init__(self, respond: Callable[[], Awaitable[None]]):
        self._respond = respond

    async def respond(self) -> None:
        """Run the sealed response. Invoked by the driver, not by ``step`` overrides."""
        await self._respond()


@final
class InboxHandle:
    """
    A type-erased, receive-only handle onto an actor's ``Inbox``.

    Passed to :meth:`tractor.Actor.step`. Call :meth:`recv` to await the next
    message as a :class:`ResponderHandle`. Override ``step`` to ``select`` over
    ``inbox.recv()`` and other sources — keeping ``recv`` first so a tie never
    drops a message.
    """

    def __init__(self, recv: Callable[[], Awaitable[ResponderHandle]]):
        self._recv = recv

    async def recv(self) -> ResponderHandle:
        """Await the next message from the actor's inbox."""
        return await self._recv()


__all__ = ["InboxHandle", "ResponderHandle"]
