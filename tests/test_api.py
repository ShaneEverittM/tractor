from dataclasses import dataclass
from typing import final, override

from tractor import Actor, Message, ActorRef


@final
@dataclass
class Foo(Message["Multiplier", float]):
    bar: int

    @override
    async def reply(self, actor: Multiplier) -> float:
        return self.bar * actor.scalar


@final
class Multiplier(Actor):
    def __init__(self, scalar: float):
        super().__init__()
        self.scalar = scalar


async def test_ask():
    a = ActorRef(Multiplier(2.5))
    r = await a.ask(Foo(2))
    assert r == 5
