import asyncio
from dataclasses import dataclass
from typing import final, override

from tractor import Actor, Message, ActorRef
from tractor.message import Context


@final
class Counter(Actor):
    def __init__(self):
        super().__init__()
        self.value: int = 0

    async def increment(self, by: int) -> None:
        self.value += by


@final
@dataclass
class Increment(Message[Counter, None]):
    by: int

    @override
    async def dispatch(self, actor: Counter, ctx: Context[Counter]) -> None:
        await actor.increment(self.by)


async def test_ask():
    m = Counter()
    a = ActorRef(m)
    a.tell(Increment(3)).try_send()
    await asyncio.sleep(1)
    assert m.value == 3
