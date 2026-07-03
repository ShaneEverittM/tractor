"""Tests for the `@handler` decorator and its synthesized messages."""

from typing import final

from tractor import Actor, Runtime, handler


@final
class Greeter(Actor):
    def __init__(self) -> None:
        self.greeted: list[str] = []

    @handler
    async def greet(self, name: str, punctuation: str = "!") -> str:
        greeting = f"hello, {name}{punctuation}"
        self.greeted.append(name)
        return greeting


async def test_handler_message_routes_to_method() -> None:
    runtime = Runtime()
    ref = runtime.spawn(Greeter())

    assert await runtime.ask(ref, Greeter.greet(name="alice")) == "hello, alice!"
    assert (
        await runtime.ask(ref, Greeter.greet("bob", punctuation="?")) == "hello, bob?"
    )

    await ref.stop()


async def test_tell_reaches_actor_state() -> None:
    runtime = Runtime()
    greeter = Greeter()
    ref = runtime.spawn(greeter)

    await runtime.tell(ref, Greeter.greet(name="carol"))
    # An ask afterward proves the tell was processed first (FIFO inbox).
    _ = await runtime.ask(ref, Greeter.greet(name="dave"))
    assert greeter.greeted == ["carol", "dave"]

    await ref.stop()


async def test_method_stays_callable_directly() -> None:
    """`@handler` does not get in the way of normal instance calls."""
    greeter = Greeter()
    assert await greeter.greet("bob") == "hello, bob!"
    assert greeter.greeted == ["bob"]
