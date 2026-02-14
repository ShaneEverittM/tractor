from dataclasses import dataclass
from typing import final, override

from tractor import Actor, Message


@final
@dataclass
class Foo(Message["Multipliler", float]):
    bar: int

    @override
    async def reply(self, actor: Multipliler) -> float:
        return self.bar * actor.scalar


@final
class Multipliler(Actor):
    def __init__(self, scalar: float):
        super().__init__()
        self.scalar = scalar


async def test_ask():
    a = Multipliler(2.5).ref()
    r = await a.ask(Foo(2))
    assert r == 5
