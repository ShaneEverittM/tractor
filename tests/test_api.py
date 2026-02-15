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


@final
@dataclass
class Increment(Message[Counter, None]):
    by: int

    @override
    async def reply(self, actor: Counter, ctx: Context[Counter]):
        actor.value += self.by


async def test_ask():
    m = Counter()
    a = ActorRef(m)
    a.tell(Increment(3)).try_send()
    await asyncio.sleep(1)
    assert m.value == 3
