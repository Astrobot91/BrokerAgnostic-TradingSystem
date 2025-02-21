import asyncio
from app.brokers.dhan_broker import DhanBroker

async def main():
    broker = DhanBroker("ACC1")
    await broker.initialize()
    
    try:
        # Add some debug prints
        print("Fetching LTP...")
        ltp = await broker.ltp_quote("NSE_EQ:11868")
        print(f"LTP: {ltp}")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())