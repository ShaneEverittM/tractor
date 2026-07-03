from asyncio import Event, QueueFull, timeout
from typing import override, final

import pytest

from tractor import Actor, Context, Message, Runtime


@final
class Blocker(Actor):
    def __init__(self, started: Event, gate: Event):
        self.started = started
        self.gate = gate


class Block(Message[Blocker, None]):
    @override
    async def dispatch(self, actor: Blocker, ctx: Context[Blocker]) -> None:
        actor.started.set()
        _ = await actor.gate.wait()


async def test_bounded_inbox_rejects_when_full():
    started = Event()
    gate = Event()
    runtime = Runtime()
    a = runtime.spawn(Blocker(started, gate), capacity=1)

    # The driver takes the first message and parks in its handler.
    runtime.try_tell(a, Block())
    async with timeout(1):
        _ = await started.wait()  # confirmed in-flight; the inbox is empty again

    # One more fills the single inbox slot...
    runtime.try_tell(a, Block())

    # ...so the next try-send is rejected rather than silently queued.
    with pytest.raises(QueueFull):
        runtime.try_tell(a, Block())

    gate.set()  # let it drain
    await a.stop()


async def test_unbounded_inbox_never_rejects():
    started = Event()
    gate = Event()
    runtime = Runtime()
    a = runtime.spawn(Blocker(started, gate))  # default: unbounded

    runtime.try_tell(a, Block())
    async with timeout(1):
        _ = await started.wait()

    # Far more than any small bound would allow; none are rejected.
    for _ in range(100):
        runtime.try_tell(a, Block())

    gate.set()
    await a.stop()
