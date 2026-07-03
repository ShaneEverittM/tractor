"""
pub_sub.py — a small pub/sub example for tractor.

Demonstrates:
  - Message[A, R] with tell (R=None) and ask (R=data)
  - TellSender fanout inside a handler — enqueue-only, so one slow
    listener never delays the rest
  - on_start / on_stop lifecycle hooks
  - Actor.step override with select for a periodic heartbeat
"""

import asyncio
from dataclasses import dataclass
from typing import final, override

from tractor import (
    Actor,
    Context,
    InboxHandle,
    Message,
    ResponderHandle,
    Runtime,
    Sel0,
    Sel1,
    TellSender,
    select,
)


# ---------------------------------------------------------------------------
# Actors
# ---------------------------------------------------------------------------


@final
class Listener(Actor):
    def __init__(self, name: str) -> None:
        self.name = name
        self._events: list[str] = []

    @override
    async def on_start(self) -> None:
        print(f"[{self.name}] started")

    @override
    async def on_stop(self) -> None:
        print(f"[{self.name}] stopped — received {len(self._events)} event(s)")

    async def receive(self, text: str) -> None:
        self._events.append(text)
        print(f"  [{self.name}] ← {text!r}")

    async def snapshot(self) -> list[str]:
        return list(self._events)


@final
class Logger(Actor):
    """Logs every event; emits a heartbeat line when the inbox is quiet."""

    _IDLE_SECS = 0.5

    def __init__(self) -> None:
        self._count = 0

    @override
    async def step(self, inbox: InboxHandle) -> ResponderHandle | None:
        match await select(inbox.recv(), asyncio.sleep(self._IDLE_SECS)):
            case Sel0(handle):
                return handle
            case Sel1(_):
                print(f"  [Logger] heartbeat — {self._count} event(s) so far")
                return None

    async def log(self, text: str) -> None:
        self._count += 1
        print(f"  [Logger] #{self._count}: {text!r}")


@final
class Hub(Actor):
    """Maintains a subscriber list and fans out published events."""

    def __init__(self, logger: "TellSender[LogEvent] | None" = None) -> None:
        self._subscribers: list[TellSender[Event]] = []
        self._logger = logger

    async def subscribe(self, sender: "TellSender[Event]") -> None:
        self._subscribers.append(sender)

    async def publish(self, text: str) -> None:
        # Tell-flavored senders only enqueue, so one slow listener never
        # delays the rest of the fanout.
        for sender in self._subscribers:
            await sender.send(Event(text))
        if self._logger is not None:
            await self._logger.send(LogEvent(text))


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


@dataclass
class Event(Message[Listener, None]):
    text: str

    @override
    async def dispatch(self, actor: Listener, ctx: Context[Listener]) -> None:
        await actor.receive(self.text)


@dataclass
class Snapshot(Message[Listener, list[str]]):
    @override
    async def dispatch(self, actor: Listener, ctx: Context[Listener]) -> list[str]:
        return await actor.snapshot()


@dataclass
class LogEvent(Message[Logger, None]):
    text: str

    @override
    async def dispatch(self, actor: Logger, ctx: Context[Logger]) -> None:
        await actor.log(self.text)


@dataclass
class Subscribe(Message[Hub, None]):
    sink: TellSender[Event]

    @override
    async def dispatch(self, actor: Hub, ctx: Context[Hub]) -> None:
        await actor.subscribe(self.sink)


@dataclass
class Publish(Message[Hub, None]):
    text: str

    @override
    async def dispatch(self, actor: Hub, ctx: Context[Hub]) -> None:
        await actor.publish(self.text)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    runtime = Runtime()

    logger_ref = runtime.spawn(Logger())
    alice_ref = runtime.spawn(Listener("Alice"))
    bob_ref = runtime.spawn(Listener("Bob"))

    hub_ref = runtime.spawn(Hub(logger=LogEvent.teller(runtime, logger_ref)))

    await runtime.tell(hub_ref, Subscribe(Event.teller(runtime, alice_ref)))
    await runtime.tell(hub_ref, Subscribe(Event.teller(runtime, bob_ref)))

    print("--- publishing ---")
    for i in range(4):
        await runtime.tell(hub_ref, Publish(f"event-{i}"))
        await asyncio.sleep(0.1)

    print("--- idle (waiting for Logger heartbeat) ---")
    await asyncio.sleep(0.7)

    alice_events = await runtime.ask(alice_ref, Snapshot())
    bob_events = await runtime.ask(bob_ref, Snapshot())
    print(f"\nAlice received: {alice_events}")
    print(f"Bob received:   {bob_events}")

    _ = await asyncio.gather(
        hub_ref.stop(), logger_ref.stop(), alice_ref.stop(), bob_ref.stop()
    )


if __name__ == "__main__":
    asyncio.run(main())
