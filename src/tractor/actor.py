"""The definition of the core ``Actor`` base class."""

from tractor.handles import InboxHandle, ResponderHandle


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

    async def step(self, inbox: InboxHandle) -> ResponderHandle | None:
        """
        Await the next unit of work for the driver to process.

        By default this just receives the next message from the inbox. Override
        it to wait on additional sources — a future, a stream, a timer —
        alongside the inbox, typically with :func:`tractor.select.select`::

            async def step(self, inbox):
                match await select(inbox.recv(), my_other_source()):
                    case Sel0(handle):
                        return handle   # a real message: hand it to the driver
                    case Sel1(event):
                        ...              # handle the event inline
                        return None      # nothing to dispatch; loop again

        Rules for overrides:

        * **Always keep awaiting the inbox**, and put ``inbox.recv()`` first, so
          a tie never drops a message (``select`` is biased to its first
          argument).
        * Return the :class:`~tractor.handles.ResponderHandle` for the driver to
          run, or ``None`` to skip and loop again — e.g. after handling an
          external event inline.
        """
        return await inbox.recv()


__all__ = ["Actor"]
