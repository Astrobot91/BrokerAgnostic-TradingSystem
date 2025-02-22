import asyncio
import aiohttp
import requests
from datetime import datetime

def sync_test():
    """Synchronous tests using requests"""
    print("\n=== Running Synchronous Tests ===")
    
    # Test LTP endpoint
    try:
        print("\nTesting LTP Endpoint...")
        response = requests.get("http://localhost:8000/ltp/NSE_EQ/10099")
        print(f"Status Code: {response.status_code}")
        print("Response:", response.json())
    except Exception as e:
        print(f"LTP Error: {str(e)}")

    # Test OHLC endpoint
    try:
        print("\nTesting OHLC Endpoint...")
        response = requests.get("http://localhost:8000/ohlc/NSE_EQ/10099")
        print(f"Status Code: {response.status_code}")
        print("Response:", response.json())
    except Exception as e:
        print(f"OHLC Error: {str(e)}")

async def async_test():
    """Asynchronous tests using aiohttp"""
    print("\n=== Running Asynchronous Tests ===")
    
    async with aiohttp.ClientSession() as session:
        # Test LTP endpoint
        try:
            print("\nTesting LTP Endpoint...")
            async with session.get("http://localhost:8000/ltp/NSE_EQ/10099") as response:
                print(f"Status Code: {response.status}")
                data = await response.json()
                print("Response:", data)
        except Exception as e:
            print(f"LTP Error: {str(e)}")

        # Test OHLC endpoint
        try:
            print("\nTesting OHLC Endpoint...")
            async with session.get("http://localhost:8000/ohlc/NSE_EQ/10099") as response:
                print(f"Status Code: {response.status}")
                data = await response.json()
                print("Response:", data)
        except Exception as e:
            print(f"OHLC Error: {str(e)}")

async def load_test():
    """Simple load test - multiple concurrent requests"""
    print("\n=== Running Load Test ===")
    
    async with aiohttp.ClientSession() as session:
        # Make 5 concurrent requests to each endpoint
        ltp_tasks = [session.get("http://localhost:8000/ltp/NSE_EQ/10099") for _ in range(5)]
        ohlc_tasks = [session.get("http://localhost:8000/ohlc/NSE_EQ/10099") for _ in range(5)]
        
        print("\nTesting multiple LTP requests...")
        start_time = datetime.now()
        responses = await asyncio.gather(*ltp_tasks)
        end_time = datetime.now()
        print(f"Time taken for 5 LTP requests: {(end_time - start_time).total_seconds()} seconds")
        
        print("\nTesting multiple OHLC requests...")
        start_time = datetime.now()
        responses = await asyncio.gather(*ohlc_tasks)
        end_time = datetime.now()
        print(f"Time taken for 5 OHLC requests: {(end_time - start_time).total_seconds()} seconds")

async def main():
    # Run synchronous tests
    sync_test()
    
    # Run asynchronous tests
    await async_test()
    
    # Run load test
    await load_test()

if __name__ == "__main__":
    print("Starting API Tests...")
    print("Make sure your FastAPI server is running on http://localhost:8000")
    print("===========================================")
    
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Test failed: {str(e)}")
    finally:
        print("\nTests completed!")