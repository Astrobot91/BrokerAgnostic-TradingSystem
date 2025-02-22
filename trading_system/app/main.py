# import asyncio
# from app.strategies.momentumV1 import MomentumStrategyV1


# async def main():
#     momentum_strategy = MomentumStrategyV1(executor_broker_name='Upstox',
#                                            executor_account_name='2HCB67')
#     await momentum_strategy.initialize()
#     await momentum_strategy.execute()

# asyncio.run(main())

from fastapi import FastAPI, HTTPException
from app.brokers.dhan_broker import DhanBroker
import asyncio

app = FastAPI()

# Create a broker instance
broker = DhanBroker("ACC1")

@app.on_event("startup")
async def startup_event():
    """Initialize broker on startup"""
    await broker.initialize()

@app.get("/ltp/{exchange}/{token}")
async def get_ltp(exchange: str, token: str):
    """
    Get Last Traded Price
    Example: /ltp/NSE_EQ/10099
    """
    try:
        exchange_token = f"{exchange}:{token}"
        ltp = await broker.ltp_quote(exchange_token)
        return {"status": "success", "ltp": ltp}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ohlc/{exchange}/{token}")
async def get_ohlc(exchange: str, token: str):
    """
    Get OHLC Data
    Example: /ohlc/NSE_EQ/10099
    """
    try:
        exchange_token = f"{exchange}:{token}"
        ohlc_data = await broker.ohlc_quote(exchange_token)
        return {
            "status": "success",
            "data": ohlc_data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

