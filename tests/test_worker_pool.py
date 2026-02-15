import asyncio
from asyncio import Event, timeout
from tractor import Actor, Message, ActorRef
from tractor.message import Context
from tractor.actors.pool import WorkerPool, CreateTask
from typing import override, final


async def worker():
    await asyncio.sleep(0.5)


@final
class ActorOne(Actor):
    def __init__(self, done: Event):
        self.done = done


class WorkerDone(Message[ActorOne, None]):
    @override
    async def reply(self, actor: ActorOne, ctx: Context[ActorOne]):
        actor.done.set()


async def test_create_worker():
    done = Event()
    a1 = ActorRef(ActorOne(done))
    ct = CreateTask(worker(), WorkerDone(), WorkerDone.sender(a1))
    pool = ActorRef(WorkerPool())
    await pool.ask(ct)
    async with timeout(1):
        _ = await done.wait()
