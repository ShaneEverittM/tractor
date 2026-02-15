import asyncio
from asyncio import CancelledError
from contextlib import suppress
from typing import final

from tractor.inbox import Inbox
from tractor.message import Message
from tractor.request import AskRequest, TellRequest
from tractor.actor import Actor


@final
class ActorRef[A: Actor]:
    def __init__(self, actor: A):
        self.actor = actor
        self.inbox = Inbox[A](actor)
        self.task = asyncio.create_task(self.driver())

    async def driver(self):
        responder = await self.inbox.get()
        await responder.respond(self.actor)

    def ask[R](self, message: Message[A, R]) -> AskRequest[A, R]:
        return AskRequest(self.inbox, message)

    def tell[R](self, message: Message[A, R]) -> TellRequest[A, R]:
        return TellRequest(self.inbox, message)

    async def stop(self):
        _ = self.task.cancel()
        with suppress(CancelledError):
            await self.task
        self.inbox.shutdown()
