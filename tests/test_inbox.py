from asyncio import Event, QueueFull, timeout
from typing import override, final

import pytest

from tractor import Actor, ActorRef, Message
from tractor.message import Context


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
    a = ActorRef(Blocker(started, gate), capacity=1)

    # The driver takes the first message and parks in its handler.
    a.tell(Block()).try_send()
    async with timeout(1):
        _ = await started.wait()  # confirmed in-flight; the inbox is empty again

    # One more fills the single inbox slot...
    a.tell(Block()).try_send()

    # ...so the next try-send is rejected rather than silently queued.
    with pytest.raises(QueueFull):
        a.tell(Block()).try_send()

    gate.set()  # let it drain
    await a.stop()


async def test_unbounded_inbox_never_rejects():
    started = Event()
    gate = Event()
    a = ActorRef(Blocker(started, gate))  # default: unbounded

    a.tell(Block()).try_send()
    async with timeout(1):
        _ = await started.wait()

    # Far more than any small bound would allow; none are rejected.
    for _ in range(100):
        a.tell(Block()).try_send()

    gate.set()
    await a.stop()
