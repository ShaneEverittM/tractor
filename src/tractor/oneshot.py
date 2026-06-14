"""A typed oneshot channel: one sender, one receiver, one value.

Analogous to ``tokio::sync::oneshot`` — create a matched ``(sender, receiver)``
pair, hand each end to a different task, and the receiver blocks until the
sender fires::

    tx, rx = oneshot(str)
    ...
    tx.send("done")    # sender side
    result = await rx  # receiver side
"""

from asyncio import Future, get_running_loop
from collections.abc import Generator
from typing import final


class DoubleSendError(RuntimeError):
    """Raised if you call ``Sender.send`` twice."""

    def __init__(self) -> None:
        super().__init__("Sender.send called twice")


@final
class Sender[T]:
    """Write end of a oneshot channel. Call ``send`` exactly once."""

    __slots__ = ("_future", "_sent")

    def __init__(self, future: Future[T]) -> None:
        self._future = future
        self._sent = False

    def send(self, value: T | BaseException) -> None:
        """
        Send a value on this channel.

        :param value: the value, or error, to send
        """
        if self._sent:
            raise DoubleSendError()
        if isinstance(value, BaseException):
            self._future.set_exception(value)
        else:
            self._future.set_result(value)

        self._sent = True


@final
class Receiver[T]:
    """Read end of a oneshot channel. ``await`` it to block until the sender fires."""

    __slots__ = ("_future",)

    def __init__(self, future: Future[T]) -> None:
        self._future = future

    def __await__(self) -> Generator[object, None, T]:
        return self._future.__await__()


def oneshot[T](_: type[T]) -> tuple[Sender[T], Receiver[T]]:
    """Return a matched ``(sender, receiver)`` pair."""
    future: Future[T] = get_running_loop().create_future()
    return Sender(future), Receiver(future)


__all__ = ["oneshot", "Sender", "Receiver"]
