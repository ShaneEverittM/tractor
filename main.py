import asyncio
from abc import ABC, abstractmethod
from asyncio import Queue, Future
from typing import final, override, cast


@abstractmethod
class Message[A, R](ABC):
    @abstractmethod
    async def reply(self, actor: A) -> R:
        ...


class Actor(ABC):
    def __init__(self):
        pass


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


@final
class ActorRef[A: Actor]:
    def __init__(self, actor: A):
        self.actor = actor
        self.inbox = Queue[tuple[Message[A, object], Future[object]]]()
        self.task = asyncio.create_task(self.driver())

    async def driver(self):
        message, reply = await self.inbox.get()
        response = await message.reply(self.actor)
        reply.set_result(response)

    def ask[R](self, message: Message[A, R]) -> Future[R]:
        reply = Future[object]()
        self.inbox.put_nowait((message, reply))
        return cast(Future[R], reply)


async def main():
    foo = ActorRef(MyActor(2.5))
    r = await foo.ask(Foo(1))
    print(r)


if __name__ == '__main__':
    asyncio.run(main())
