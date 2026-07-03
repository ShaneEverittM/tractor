import asyncio
from asyncio import Event, timeout
from tractor import Actor, Message, Runtime
from tractor.message import Context
from tractor.actors.pool import WorkerPool, Submit, TrySubmit
from typing import override, final


async def worker() -> None:
    await asyncio.sleep(0.5)


async def quick() -> None:
    return


@final
class ActorOne(Actor):
    def __init__(self, done: Event):
        self.done = done

    def mark_done(self) -> None:
        self.done.set()


class WorkerDone(Message[ActorOne, None]):
    @override
    async def dispatch(self, actor: ActorOne, ctx: Context[ActorOne]) -> None:
        actor.mark_done()


async def test_create_worker():
    done = Event()
    runtime = Runtime()
    a1 = runtime.spawn(ActorOne(done))
    pool = runtime.spawn(WorkerPool())
    reservation = await runtime.ask(
        pool, Submit(worker, WorkerDone(), WorkerDone.teller(runtime, a1))
    )
    await reservation
    async with timeout(1):
        _ = await done.wait()


async def test_unlimited_pool_grants_immediately():
    runtime = Runtime()
    a1 = runtime.spawn(ActorOne(Event()))
    pool = runtime.spawn(WorkerPool())
    res = await runtime.ask(
        pool, Submit(quick, WorkerDone(), WorkerDone.teller(runtime, a1))
    )
    assert res.pending is False
    await res


async def test_try_submit_rejects_when_full():
    runtime = Runtime()
    a1 = runtime.spawn(ActorOne(Event()))
    pool = runtime.spawn(WorkerPool(limit=1))

    started = Event()
    release = Event()

    async def slow() -> None:
        started.set()
        _ = await release.wait()

    # The single slot is taken by the first task...
    accepted = await runtime.ask(
        pool, TrySubmit(slow, WorkerDone(), WorkerDone.teller(runtime, a1))
    )
    assert accepted is True
    async with timeout(1):
        _ = await started.wait()  # ensure it is running and holding the slot

    # ...so a second early-reject submission bounces immediately.
    rejected = await runtime.ask(
        pool, TrySubmit(quick, WorkerDone(), WorkerDone.teller(runtime, a1))
    )
    assert rejected is False

    # Free the slot; an early-reject submission is then accepted again.
    release.set()
    async with timeout(1):
        while not await runtime.ask(
            pool, TrySubmit(quick, WorkerDone(), WorkerDone.teller(runtime, a1))
        ):
            await asyncio.sleep(0.01)


async def test_submit_queues_until_a_slot_frees():
    runtime = Runtime()
    a1 = runtime.spawn(ActorOne(Event()))
    pool = runtime.spawn(WorkerPool(limit=1))

    release = Event()

    async def slow() -> None:
        _ = await release.wait()

    # First submission gets the only slot immediately.
    res1 = await runtime.ask(
        pool, Submit(slow, WorkerDone(), WorkerDone.teller(runtime, a1))
    )
    assert res1.pending is False
    await res1

    # Second submission is accepted but queued; its reservation stays pending.
    second_ran = Event()

    async def second() -> None:
        second_ran.set()

    res2 = await runtime.ask(
        pool, Submit(second, WorkerDone(), WorkerDone.teller(runtime, a1))
    )
    assert res2.pending is True
    await asyncio.sleep(0.05)
    assert res2.pending is True  # still queued while the first task holds the slot

    # Releasing the first frees the slot, which is handed to the queued waiter.
    release.set()
    async with timeout(1):
        await res2
    assert res2.pending is False
    async with timeout(1):
        _ = await second_ran.wait()


async def test_try_submit_does_not_jump_the_queue():
    runtime = Runtime()
    a1 = runtime.spawn(ActorOne(Event()))
    pool = runtime.spawn(WorkerPool(limit=1))

    release = Event()

    async def slow() -> None:
        _ = await release.wait()

    # Fill the slot, then queue a back-pressure waiter behind it.
    res1 = await runtime.ask(
        pool, Submit(slow, WorkerDone(), WorkerDone.teller(runtime, a1))
    )
    await res1
    res2 = await runtime.ask(
        pool, Submit(quick, WorkerDone(), WorkerDone.teller(runtime, a1))
    )
    assert res2.pending is True

    # A try-submission must be rejected: a waiter is already ahead of it.
    accepted = await runtime.ask(
        pool, TrySubmit(quick, WorkerDone(), WorkerDone.teller(runtime, a1))
    )
    assert accepted is False

    # Drain.
    release.set()
    async with timeout(1):
        await res2


@final
class BusyTarget(Actor):
    """Parks in its notification handler, never releasing it."""

    def __init__(self):
        self.notified = Event()


class BusyDone(Message[BusyTarget, None]):
    @override
    async def dispatch(self, actor: BusyTarget, ctx: Context[BusyTarget]) -> None:
        actor.notified.set()
        _ = await Event().wait()  # never set


async def test_busy_notification_target_does_not_stall_pool():
    runtime = Runtime()
    target = BusyTarget()
    target_ref = runtime.spawn(target)
    pool = runtime.spawn(WorkerPool(limit=1))

    # The first task completes; its notification parks the target's handler.
    res1 = await runtime.ask(
        pool, Submit(quick, BusyDone(), BusyDone.teller(runtime, target_ref))
    )
    await res1
    async with timeout(1):
        _ = await target.notified.wait()

    # Tell semantics: the pool only enqueues the notification, so it keeps
    # reaping and granting slots while the target is still busy with it.
    second_ran = Event()

    async def second() -> None:
        second_ran.set()

    async with timeout(1):
        res2 = await runtime.ask(
            pool, Submit(second, BusyDone(), BusyDone.teller(runtime, target_ref))
        )
        await res2
        _ = await second_ran.wait()
