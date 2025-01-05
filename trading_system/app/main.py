import asyncio
from app.strategies.momentumV1 import MomentumStrategyV1


async def main():
    momentum_strategy = MomentumStrategyV1(executor_broker_name='Upstox',
                                           executor_account_name='2HCB67')
    await momentum_strategy.initialize()
    await momentum_strategy.execute()

asyncio.run(main())



