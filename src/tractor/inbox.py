import asyncio
from asyncio import Future, Queue
from typing import final
from tractor.message import Message, Responder
from tractor.actor import Actor


@final
class Inbox[A: Actor](Queue[Responder[A, object]]):
    def __init__(self, actor: A):
        super().__init__()
        self._actor = actor

    async def ask[R](
        self, message: Message[A, R], timeout: float | None = None
    ) -> Future[R]:
        responder, handle = Responder(message).ask()
        put = self.put(responder)
        await asyncio.wait_for(put, timeout)
        return handle

    def try_ask[R](self, message: Message[A, R]) -> Future[R]:
        responder, reply = Responder(message).ask()
        self.put_nowait(responder)
        return reply

    async def tell[R](self, message: Message[A, R]) -> None:
        responder = Responder(message).tell()
        await self.put(responder)

    def try_tell[R](self, message: Message[A, R]) -> None:
        responder = Responder(message).tell()
        self.put_nowait(responder)
