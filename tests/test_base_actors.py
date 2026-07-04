from typing import override

from tractor import Actor, Context, Message, Runtime


class HealthCheckable(Actor):
    async def check(self) -> bool:
        return True


class HealthCheck(Message[HealthCheckable, bool]):
    @override
    async def dispatch(
        self, actor: HealthCheckable, ctx: Context[HealthCheckable]
    ) -> bool:
        return await actor.check()


class Service(HealthCheckable, Actor):
    pass


class UnhealthyService(HealthCheckable, Actor):
    @override
    async def check(self) -> bool:
        return False


async def test_capability():
    async with Runtime() as rt:
        healthy = rt.spawn(Service())
        unhealthy = rt.spawn(UnhealthyService())
        assert await rt.ask(healthy, HealthCheck())
        assert not await rt.ask(unhealthy, HealthCheck())
