"""A task pool actor and its messages."""

import asyncio
from asyncio import Task
from dataclasses import dataclass
from typing import override, final
from collections.abc import Awaitable

from tractor import Actor, Message
from tractor.message import Context, Sender


@final
class WorkerPool(Actor):
    """An actor managing a pool of workers."""

    def __init__(self, limit: int | None = None):
        """Create worker pool"""
        self.pool: dict[Task[None], Awaitable[None]] = {}
        self.limit = limit


@dataclass
class TaskDone(Message[WorkerPool, None]):
    """An internal message send to the pool when a task completes."""

    task: Task[None]

    @override
    async def reply(self, actor: WorkerPool, ctx: Context[WorkerPool]):
        await actor.pool[self.task]


@final
class CreateTask[M](Message[WorkerPool, None]):
    """A message to request work done on the pool."""

    def __init__(
        self,
        task: Awaitable[None],
        message: M,
        sender: Sender[M, None],
    ):
        """
        Request a task to be started in the worker pool.

        :param task: the work to do
        :param message: the message you would like to receive when the work is done
        :param sender: the sender on which to send the message
        """
        self._task = task
        self._message = message
        self._sender = sender

    @override
    async def reply(self, actor: WorkerPool, ctx: Context[WorkerPool]):
        task = asyncio.ensure_future(self._task)
        task.add_done_callback(lambda _: ctx.ref.tell(TaskDone(task)).try_send())
        actor.pool[task] = self._sender.send(self._message)
