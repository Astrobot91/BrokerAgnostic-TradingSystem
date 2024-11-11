import asyncio
from app.strategies.sample_strategy import MomentumStrategyV1


async def main():
    momentum_strategy = MomentumStrategyV1(datafeed_broker_name='Upstox',
                                           executor_broker_name='Upstox',
                                           datafeed_account_name='2HCB67',
                                           executor_account_name='2HCB67')
    await momentum_strategy.initialize()
    await momentum_strategy.execute()
    
asyncio.run(main())



