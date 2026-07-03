import asyncio
from dataclasses import dataclass
from typing import final, override

from tractor import Actor, Context, Message, Runtime


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
    runtime = Runtime()
    a = runtime.spawn(m)
    runtime.try_tell(a, Increment(3))
    await asyncio.sleep(1)
    assert m.value == 3
