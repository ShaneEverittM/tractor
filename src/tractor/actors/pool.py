"""A worker-pool actor that bounds how many tasks run concurrently."""

import asyncio
from asyncio import FIRST_COMPLETED, Future, Task
from collections import deque
from collections.abc import Awaitable, Callable, Generator
from dataclasses import dataclass
from typing import final, override

from tractor import Actor, Message
from tractor.combinators import first
from tractor.handles import InboxHandle, ResponderHandle
from tractor.message import Context, TellSender


@final
class Reservation(Awaitable[None]):
    """
    A place in a `WorkerPool`, returned by a `Submit` submission.

    A reservation is *granted* the moment the pool has a free slot for your
    task, at which point the task begins running. Awaiting the reservation
    blocks until that moment:

    ```python
    reservation = await pool.ask(Submit(work, Done(), sender))
    await reservation   # returns once your task has actually started
    ```

    Awaiting is how you apply back-pressure: in a submit loop, `await` each
    reservation before submitting the next task and you will never have more
    than the pool's `limit` of work outstanding. Awaiting is optional, though
    — the task runs regardless of whether anyone waits on its reservation, so a
    reservation you drop is harmless. It is purely an "it has started" signal.
    """

    def __init__(self, started: Future[None]):
        self._started: Future[None] = started

    @property
    def pending(self) -> bool:
        """`True` while the task is still queued, waiting for a free slot."""
        return not self._started.done()

    @override
    def __await__(self) -> Generator[None, None, None]:
        return self._started.__await__()


@final
@dataclass
class _Waiter:
    """A queued submission, parked until a slot frees up."""

    reservation: Future[None]
    work: Callable[[], Awaitable[None]]
    on_done: Callable[[], Awaitable[None]]


@final
class WorkerPool(Actor):
    """
    An actor that runs submitted tasks, capping how many run at once.

    Submit work with one of two messages:

    * `Submit` — *back-pressure*, always accepted. Replies with a
      `Reservation`; await it to block until your task actually starts.
      When the pool is full, queued submissions are granted slots strictly
      first-come-first-served as running tasks finish.
    * `TrySubmit` — *early reject*. Replies `True` if the task started
      immediately, or `False` if it could not. It never waits and never
      queues.

    `Submit` owns the fair waiter queue; `TrySubmit` is the
    non-queuing shortcut over the same capacity. Because a queued `Submit`
    sits ahead of any later submission, a `TrySubmit` cannot jump the line
    — it is rejected whenever anyone is already waiting.

    The pool overrides `step` to wait on its inbox *and* on its running
    tasks at once: when a task finishes it is reaped right there in the driver —
    its slot handed to the next waiter, its completion notification sent — with
    no self-sent message and no completion callback.
    """

    def __init__(self, limit: int | None = None):
        """
        Create a worker pool.

        :param limit: the most tasks allowed to run at once, or `None` for no
            limit (every submission then starts immediately)
        """
        self.limit = limit
        self._running = 0
        self._waiters: deque[_Waiter] = deque()
        # Running task -> factory for the notification to send when it finishes.
        self._pool: dict[Task[None], Callable[[], Awaitable[None]]] = {}

    @override
    async def step(self, inbox: InboxHandle) -> ResponderHandle | None:
        """Wait on the inbox and on task completions simultaneously."""
        match await first(inbox.recv(), self._next_finished()):
            case ResponderHandle() as handle:
                return handle
            case Task() as finished:
                await self._complete(finished)
                return None

    async def _next_finished(self) -> Task[None]:
        """
        Resolve to a running task as soon as one of them finishes.

        While the pool is idle this never resolves, so `step` waits only on the
        inbox until there is work to watch.
        """
        if not self._pool:
            _ = await asyncio.Event().wait()  # never set: only the inbox matters
        done, _ = await asyncio.wait(self._pool.keys(), return_when=FIRST_COMPLETED)
        return next(iter(done))

    async def _complete(self, finished: Task[None]) -> None:
        """Reap `finished`: free its slot, then send its completion notification."""
        self._reap()
        on_done = self._pool.pop(finished)
        await on_done()

    def _reap(self) -> None:
        """Free one slot, handing it to the next waiter if any are queued."""
        if self._waiters:
            waiter = self._waiters.popleft()
            self._schedule(waiter.work, waiter.on_done)  # reuse the slot
            waiter.reservation.set_result(None)
        else:
            self._running -= 1

    def submit(
        self,
        work: Callable[[], Awaitable[None]],
        on_done: Callable[[], Awaitable[None]],
    ) -> Reservation:
        """
        Submit `work`, queueing for a slot if the pool is full (back-pressure).

        If a slot is free the task starts now and the returned reservation is
        already granted; otherwise the submission joins the back of the fair
        waiter queue and its reservation is granted later, when a slot frees.

        :param work: a factory producing the work to run, called once a slot is claimed
        :param on_done: a factory producing the completion notification
        :return: a `Reservation` that resolves when the task starts running
        """
        started = Future[None]()
        if self._has_free_slot():
            self._begin(work, on_done)
            started.set_result(None)
        else:
            self._waiters.append(_Waiter(started, work, on_done))
        return Reservation(started)

    def try_submit(
        self,
        work: Callable[[], Awaitable[None]],
        on_done: Callable[[], Awaitable[None]],
    ) -> bool:
        """
        Submit `work` only if it can start right now; otherwise reject it.

        Rejects (returns `False`) when the pool is full *or* when other
        submissions are already queued ahead of it. On rejection neither factory
        is invoked, so a rejected submission constructs nothing.

        :param work: a factory producing the work to run, called only if accepted
        :param on_done: a factory producing the completion notification
        :return: `True` if the task started, `False` if it was rejected
        """
        if not self._has_free_slot():
            return False
        self._begin(work, on_done)
        return True

    def _has_free_slot(self) -> bool:
        """
        Whether a task may start immediately.

        A freed slot is always handed straight to the next waiter, so whenever
        slots are free the waiter queue is empty. Checking capacity therefore
        also proves nobody is queued ahead.
        """
        return self.limit is None or self._running < self.limit

    def _begin(
        self,
        work: Callable[[], Awaitable[None]],
        on_done: Callable[[], Awaitable[None]],
    ) -> None:
        """Claim a fresh slot and schedule `work` in it."""
        self._running += 1
        self._schedule(work, on_done)

    def _schedule(
        self,
        work: Callable[[], Awaitable[None]],
        on_done: Callable[[], Awaitable[None]],
    ) -> None:
        """Start the work coroutine and remember how to announce its completion."""
        future = asyncio.ensure_future(work())
        self._pool[future] = on_done


class _Submission[M]:
    """Shared construction for the two task-submission messages."""

    def __init__(
        self,
        task: Callable[[], Awaitable[None]],
        message: M,
        sender: TellSender[M],
    ):
        """
        :param task: a factory producing the work to run on the pool
        :param message: the message to deliver once the work finishes
        :param sender: the tell-flavored sender that delivers `message`;
            tell semantics keep the pool live — the notification only
            enqueues, so a slow recipient cannot stall the pool's driver
        """
        self._task: Callable[[], Awaitable[None]] = task
        self._message: M = message
        self._sender: TellSender[M] = sender

    def _notify(self) -> Awaitable[None]:
        """Produce the completion notification (invoked only if the task runs)."""
        return self._sender.send(self._message)


@final
class Submit[M](_Submission[M], Message[WorkerPool, Reservation]):
    """
    Submit work to a `WorkerPool` with back-pressure; always accepted.

    Replies with a `Reservation`. Await it to block until your task
    starts; awaiting each reservation before submitting the next caps your
    outstanding work at the pool's limit.
    """

    @override
    async def dispatch(
        self, actor: WorkerPool, ctx: Context[WorkerPool]
    ) -> Reservation:
        return actor.submit(self._task, self._notify)


@final
class TrySubmit[M](_Submission[M], Message[WorkerPool, bool]):
    """
    Submit work to a `WorkerPool` only if a slot is free now; else reject.

    Replies `True` if the task started immediately, or `False` if the pool
    was full or other submissions were already queued ahead of it.
    """

    @override
    async def dispatch(self, actor: WorkerPool, ctx: Context[WorkerPool]) -> bool:
        return actor.try_submit(self._task, self._notify)


__all__ = ["WorkerPool", "Submit", "TrySubmit", "Reservation"]
