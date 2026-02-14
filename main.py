import asyncio
from typing import final, override

from actor import Message, Actor


@final
class MyActor(Actor):
    def __init__(self, factor: float):
        super().__init__()
        self.factor = factor


@final
class Foo(Message[MyActor, float]):
    def __init__(self, bar: int):
        self.bar = bar

    @override
    async def reply(self, actor: MyActor) -> float:
        return self.bar * actor.factor


async def main():
    foo = MyActor(2.5).ref()
    r = await foo.ask(Foo(1))
    print(r)


if __name__ == "__main__":
    asyncio.run(main())
