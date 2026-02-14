import asyncio

from asyncio import Future
from abc import abstractmethod, ABC
from asyncio import Queue
from typing import Self, final, cast


@abstractmethod
class Message[A, R](ABC):
    @abstractmethod
    async def reply(self, actor: A) -> R:
        ...


class Actor(ABC):
    def ref(self) -> ActorRef[Self]:
        return ActorRef(self)


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

    async def ask[R](self, message: Message[A, R]) -> Future[R]:
        reply = Future[object]()
        self.inbox.put_nowait((message, reply))
        return cast(Future[R], reply)
