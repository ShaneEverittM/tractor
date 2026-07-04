"""
The two ways to define a message, from most explicit to most compact.

1. A hand-written `Message` dataclass with an explicit ``dispatch`` — a
   real, named, introspectable message type you can construct, inspect,
   and pattern-match before sending.
2. An ``@handler`` call site (``Listener.snapshot()``) — no message type at
   all; the envelope is synthesized from the method and its arguments.
"""

from dataclasses import dataclass
from typing import final, override

from tractor import (
    Actor,
    Context,
    Message,
    Runtime,
    handler,
    main,
)


@final
class Listener(Actor):
    def __init__(self, name: str) -> None:
        self.name = name
        self._events: list[str] = []

    async def receive(self, text: str) -> None:
        self._events.append(text)
        print(f"  [{self.name}] <- {text!r}")

    @handler
    async def snapshot(self) -> list[str]:
        return list(self._events)


# 1. Fully explicit: a dataclass envelope with hand-written routing.
@dataclass
class Announce(Message[Listener, None]):
    text: str

    @override
    async def dispatch(self, actor: Listener, ctx: Context[Listener]) -> None:
        await actor.receive(self.text)


@main
async def app(rt: Runtime) -> None:
    async with rt:
        alice = rt.spawn(Listener("Alice"))

        await rt.tell(alice, Announce("hello, hand-written message"))

        # 2. No message type at all: `@handler` synthesizes the envelope.
        events = await rt.ask(alice, Listener.snapshot())
        print(f"snapshot: {events}")
