from typing import override
from tractor import Actor, ActorRef, Context, Message, Runtime


class ActorOne(Actor):
    pass


class ActorTwo(Actor):
    pass


class MessageForActorOne(Message[ActorOne, int]):
    @override
    async def dispatch(self, actor: ActorOne, ctx: Context[ActorOne]) -> int:
        return 42


class MessageForActorTwo(Message[ActorTwo, float]):
    @override
    async def dispatch(self, actor: ActorTwo, ctx: Context[ActorTwo]) -> float:
        return 3.14


# Because we throw an error if a type ignore is unnecessary,
# if we type ignore the expected errors, we will get an error
# if the _don't_ fire. This test doesn't actually need to do
# anything at runtime.
async def test_type_safety() -> None:
    runtime = Runtime()
    actor1 = runtime.spawn(ActorOne())
    actor2 = runtime.spawn(ActorTwo())

    # Using ask should propagate the message's return type.
    r1: int = await runtime.ask(actor1, MessageForActorOne())
    r2: float = await runtime.ask(actor2, MessageForActorTwo())

    # And obviously it should work.
    assert r1 == 42
    assert r2 == 3.14

    # You should not be able to send a message to the wrong actor.
    await runtime.tell(actor1, MessageForActorTwo())  # pyright: ignore[reportArgumentType]
    await runtime.tell(actor2, MessageForActorOne())  # pyright: ignore[reportArgumentType]

    # You should not be able to misuse Senders.
    sender1 = MessageForActorOne.sender(runtime, actor1)
    misuse = sender1.send(MessageForActorTwo())  # pyright: ignore[reportArgumentType]
    misuse.close()  # only constructed to assert the type error above; don't leak it

    # This isn't really a public API, but it's useful to make sure we don't code bugs.
    responder1, reply1 = MessageForActorOne().responder().ask()
    a1 = ActorOne()
    ref = ActorRef(a1)
    ctx = Context(ref)
    await responder1.respond(a1, ctx)
    assert await reply1 == 42

    await actor1.stop()
    await actor2.stop()
