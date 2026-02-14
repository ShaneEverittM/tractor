import asyncio
from dataclasses import dataclass
from typing import final, override

from actor import Actor, Message


@final
@dataclass
class Foo(Message["MyActor", float]):
    bar: int

    @override
    async def reply(self, actor: MyActor) -> float:
        return self.bar * actor.factor


@final
class MyActor(Actor):
    def __init__(self, factor: float):
        super().__init__()
        self.factor = factor


async def main():
    foo = MyActor(2.5).ref()

    r = await foo.ask(Foo(1))
    print(f"Asked MyActor {Foo(1)}, got {r}")

    await foo.tell(Foo(2))
    print(f"Told MyActor {Foo(2)}")


if __name__ == "__main__":
    asyncio.run(main())
