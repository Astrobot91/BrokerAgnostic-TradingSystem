import asyncio
from app.brokers.dhan_broker import DhanBroker

async def main():
    broker = DhanBroker("ACC1")
    await broker.initialize()
    
    # Get LTP for NSE equity
    ltp = await broker.ltp_quote("NSE_EQ:500003")
    print(f"LTP: {ltp}")

if __name__ == "__main__":
    asyncio.run(main())