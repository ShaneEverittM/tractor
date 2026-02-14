import asyncio
from abc import ABC, abstractmethod
from asyncio import AbstractEventLoop, Future, Queue
from collections.abc import Generator
from typing import Self, cast, final, override


@abstractmethod
class Message[A, R](ABC):
    @abstractmethod
    async def reply(self, actor: A) -> R: ...


class Actor(ABC):
    def ref(self) -> ActorRef[Self]:
        return ActorRef(self)


@final
class Reply[A, R](Future[R]):
    def __init__(
        self,
        inbox: Queue[tuple[Message[A, object], Future[object]]],
        message: Message[A, R],
        *,
        loop: AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(loop=loop)
        self._sent = False
        self._message: Message[A, R] | None = message
        self._inbox = inbox
        self._reply: Future[R] | None = None
        self._inbox_timeout: float | None = None
        self._reply_timeout: float | None = None

    def _take_message(self) -> Message[A, R]:
        assert self._message is not None, "Message taken twice!"
        message = self._message
        self._message = None
        return message

    async def _enqueue(self) -> Future[R]:
        reply = Future[object]()
        message = self._take_message()
        put = self._inbox.put((message, reply))
        await asyncio.wait_for(put, self._inbox_timeout)
        return cast(Future[R], reply)

    def _try_enqueue(self):
        reply = Future[object]()
        message = self._take_message()
        self._inbox.put_nowait((message, reply))
        return cast(Future[R], reply)

    def with_inbox_timout(self, seconds: float) -> Self:
        self._inbox_timeout = seconds
        return self

    async def enqueue(self) -> Self:
        self._reply = await self._enqueue()
        return self

    def try_enqueue(self) -> Self:
        self._reply = self._try_enqueue()
        return self

    async def send(self) -> R:
        reply = self._reply or await self._enqueue()
        return await reply

    def try_send(self) -> Future[R]:
        reply = self._reply or self._try_enqueue()
        return reply

    @override
    def __await__(self) -> Generator[None, None, R]:
        return self.send().__await__()


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

    def ask[R](self, message: Message[A, R]) -> Reply[A, R]:
        return Reply(self.inbox, message)
