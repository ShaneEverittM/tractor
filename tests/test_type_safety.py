from typing import override
from tractor import Actor, ActorRef, Message
from tractor.message import Context


class ActorOne(Actor):
    pass


class ActorTwo(Actor):
    pass


class MessageForActorOne(Message[ActorOne, int]):
    @override
    async def reply(self, actor: ActorOne, ctx: Context[ActorOne]) -> int:
        return 42


class MessageForActorTwo(Message[ActorTwo, float]):
    @override
    async def reply(self, actor: ActorTwo, ctx: Context[ActorTwo]) -> float:
        return 3.14


# Because we throw an error if a type ignore is unnecessary,
# if we type ignore the expected errors, we will get an error
# if the _don't_ fire. This test doesn't actually need to do
# anything at runtime.
async def test_type_safety() -> None:
    actor1 = ActorRef(ActorOne())
    actor2 = ActorRef(ActorTwo())

    # Using ask should propagate the message's return type.
    r1: int = await actor1.ask(MessageForActorOne())
    r2: float = await actor2.ask(MessageForActorTwo())

    # And obviously it should work.
    assert r1 == 42
    assert r2 == 3.14

    # You should not be able to send a message to the wrong actor.
    await actor1.tell(MessageForActorTwo())  # pyright: ignore[reportArgumentType]
    await actor2.tell(MessageForActorOne())  # pyright: ignore[reportArgumentType]

    # You should not be able to misuse Senders.
    sender1 = MessageForActorOne.sender(actor1)
    _ = sender1.send(MessageForActorTwo())  # pyright: ignore[reportArgumentType]

    # This isn't really a public API, but it's useful to make sure we don't code bugs.
    responder1, reply1 = MessageForActorOne().responder().ask()
    a1 = ActorOne()
    ref = ActorRef(a1)
    ctx = Context(ref)
    await responder1.respond(a1, ctx)
    assert await reply1 == 42
