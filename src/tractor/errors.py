"""Shared exception types for the tractor actor library."""


class ActorStoppedError(Exception):
    """Raised on reply futures when the actor stops before processing the message."""


__all__ = ["ActorStoppedError"]
