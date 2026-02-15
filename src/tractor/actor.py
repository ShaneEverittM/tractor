"""The definition of the core ``Actor`` base class."""


class Actor:
    """
    The base class for actors.

    Defines lifecycle methods, but all are optional with sensible defaults.
    """

    async def on_start(self):
        """Called when the actor is started."""
        pass

    async def on_stop(self):
        """Called when the actor is stopped."""
        pass
