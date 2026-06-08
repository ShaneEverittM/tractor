"""The definition of the core ``Actor`` base class."""

from typing import Self, TYPE_CHECKING

if TYPE_CHECKING:
    from tractor.inbox import Inbox
    from tractor.message import Responder


class Actor:
    """
    The base class for actors.

    Defines lifecycle methods and the driver's ``step``, but all are optional
    with sensible defaults.
    """

    async def on_start(self):
        """Called when the actor is started."""
        pass

    async def on_stop(self):
        """Called when the actor is stopped."""
        pass

    async def step(self, inbox: Inbox[Self]) -> Responder[Self, object] | None:
        """
        Await the next unit of work for the driver to process.

        By default this just receives the next message from the mailbox.
        Override it to wait on additional sources — a future, a stream, a timer —
        alongside the mailbox, typically with :func:`tractor.select.select`::

            async def step(self, inbox):
                match await select(inbox.get(), my_other_source()):
                    case Sel0(responder):
                        return responder   # a real message: dispatch it
                    case Sel1(event):
                        ...                 # handle the event inline
                        return None         # nothing to dispatch; loop again

        Rules for overrides:

        * **Always keep awaiting the mailbox**, and put ``inbox.get()`` first, so
          a tie never drops a message (``select`` is biased to its first argument).
        * Return a :class:`~tractor.message.Responder` for the driver to dispatch,
          or ``None`` to skip dispatch and loop again — e.g. after handling an
          external event inline.
        """
        return await inbox.get()


__all__ = ["Actor"]
