import re
import requests
import polars as pl
from bs4 import BeautifulSoup
from app.brokers.base_broker import BaseBroker
from typing import Dict, Any


class BrokerDataFeed:
    
    def __init__(self, broker: BaseBroker):
        self.broker = broker
        self.logger = self.broker.logger

    async def historical_data(self,
                              exchange_token: str,
                              interval: str,
                              from_date: str,
                              to_date: str) -> pl.DataFrame:
        """
        Gets historical data from Upstox.

        Args:
            exchange_token (str): The unique identifier for the instrument within the Upstox API.
            interval (str): The time interval for the data (e.g., '1minute', '5minute', '15minute', '1day').
            from_date (str): The starting date for the historical data in 'YYYY-MM-DD' format.
            to_date (str): The ending date for the historical data in 'YYYY-MM-DD' format.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the historical candle data.
                - `datetime`: The timestamp for each data point.
                - `open`: The opening price of the instrument.
                - `high`: The highest price of the instrument.
                - `low`: The lowest price of the instrument.
                - `close`: The closing price of the instrument.
                - `volume`: The trading volume.
                - `oi`: The open interest.

        Raises:
            Exception: For any other exceptions that occur during data retrieval.
        """
        try:
            data = await self.broker.historical_data(exchange_token=exchange_token,
                                               interval=interval,
                                               from_date=from_date,
                                               to_date=to_date)
            return data
        
        except Exception as e:
            error_msg = f"Exception occured while getting historical data for {exchange_token} from: {from_date} to: {to_date} at interval: {interval}: {e}"
            self.logger.error(error_msg) 
            raise Exception(error_msg)

    async def ltp_quote(self, exchange_token: str) -> float:
        """
        Asynchronously retrieves the Last Traded Price (LTP) for a specified exchange token.

        This function fetches the LTP for a given exchange token from Upstox's market quote API. 
        It verifies that the token exists in the local instrument data, then retrieves the price data.

        Args:
            exchange_token (str): The unique identifier for the instrument on the exchange.

        Returns:
            float: The last traded price of the instrument associated with the given exchange token.

        Raises:
            ValueError: If the exchange token is not found in the instrument data.
            Exception: If the API request fails or if LTP retrieval is unsuccessful due to server issues or invalid responses.
        
        Example:
            exchange_token = '74989'

        Notes:
            - Requires `instrument_df` to contain the exchange token and `instrument_key` mappings.
            - Requires a valid Upstox API access token and active session to function.
        """
        try:
            data = await self.broker.ltp_quote(exchange_token=exchange_token)
            return data
        except Exception as e:
            error_msg = f"Exception while getting LTP quote for {exchange_token}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        
    async def ohlc_quote(self, exchange_token: str, interval: str) -> Dict[str, float]:
        """
        Asynchronously retrieves Open-High-Low-Close (OHLC) data for a specified exchange token and time interval.

        This function fetches the OHLC data for a given exchange token from Upstox's market quote API,
        for a specified time interval such as '1day' or '5min'. It verifies that the token exists in 
        the local instrument data before making the request.

        Args:
            exchange_token (str): The unique identifier for the instrument on the exchange.
            interval (str): The time interval for OHLC data (e.g., '1d' (1 day), '1I' (1 minute), '5I' (5 minutes)).

        Returns:
            Dict[str, float]: A dictionary containing the OHLC data, with keys 'open', 'high', 'low', and 'close' 
                              representing respective prices.

        Raises:
            ValueError: If the exchange token is not found in the instrument data.
            Exception: If the API request fails or if OHLC data retrieval is unsuccessful due to server issues or invalid responses.
        
        Example:
            exchange_token = '74989'
            interval = '1day'
            ohlc_data = await broker.ohlc_quote(exchange_token, interval)
            print(f"OHLC data: {ohlc_data}")
        """
        try:
            data = await self.broker.ohlc_quote(exchange_token=exchange_token, interval=interval)
            return data
        except Exception as e:
            error_msg = f"Exception while getting OHLC quote for exchange token: {exchange_token}, interval: {interval}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    async def full_market_quote(self, exchange_token: str) -> Dict[str, Any]:
        """
        Asynchronously retrieves the full market quote for a specified exchange token.

        This function fetches comprehensive market data for a given exchange token from Upstox's market 
        quote API. It verifies the existence of the exchange token in the instrument data before making 
        the request, ensuring that only valid tokens are processed.

        Args:
            exchange_token (str): The unique identifier for the instrument on the exchange.

        Returns:
            Dict[str, Any]: A dictionary containing the full market quote data for the specified 
                            exchange token, including details such as bid-ask prices, 
                            last traded price, volume, and other key metrics.

        Raises:
            ValueError: If the exchange token is not found in the instrument data.
            Exception: If the API request fails or if full market quote retrieval is unsuccessful 
                       due to server issues or invalid responses.

        Example:
            exchange_token = '74989'
        """
        try:
            data = await self.broker.full_market_quote(exchange_token=exchange_token)
            return data
        except Exception as e:
            error_msg = f"Exception while getting full market quote for exchange token: {exchange_token}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def market_holidays(self):
        """
        Fetches market holiday information for a specific date.

        This function sends an asynchronous GET request to the market holidays API endpoint to retrieve holiday data
        for the given date. It logs an error and returns None if the API response is unsuccessful or if the holiday
        data is missing. In case of an exception, it logs the exception details and returns None.

        Returns:
            Optional[Dict[str, Any]]: The holiday data as a dictionary if the request is successful and data is available;
                                    otherwise, returns None.
        
        Raises:
            Exception: Logs and raises an exception if there's an error in fetching or processing the data.
        """
        try:
            data = await self.broker.market_holidays()
            return data
        except Exception as e:
            error_msg = f"Exception while getting market holiday data"
            self.logger.error(error_msg)
            raise Exception(error_msg)


class FundamentalDataFeed:

    def __init__(self, broker: BaseBroker): 
        self.logger = broker.logger

    def marketCapData(self, isin_code: str = None) -> str:
        try:
            search_url = f"https://www.tijorifinance.com/api/v1/ind/company_search/?q={isin_code}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            search_response = requests.get(search_url, headers=headers)
            search_response.raise_for_status()

            data = search_response.json()
            
            if data and len(data) > 0:
                slug = data[0].get('slug')
                if not slug:
                    self.logger.warning(f'Slug not found in search response.')

                company_url = f"https://www.tijorifinance.com/company/{slug}"
                company_response = requests.get(company_url, headers=headers)
                company_response.raise_for_status()

                soup = BeautifulSoup(company_response.text, 'html.parser')
                
                target_element = soup.select('div.mybody div.wrapper div.company_summary div.company_details__wrapper div:nth-child(1) div.company_details_value')
                
                if target_element:
                    text = target_element[0].text.strip()
                    numbers = re.findall(r'[\d,]+\.?\d*', text)
                    numbers = [number.replace(',', '') for number in numbers]
                    if numbers:
                        market_cap = float(numbers[0])
                        return market_cap
                    else:
                        self.logger.warning('No numbers found in the marketcap text.')
                else:
                    self.logger.warning('Market cap element not found on the page.')
            else:
                self.logger.warning(f'No data found for the provided ISIN code: {isin_code}')
        
            return None
        except requests.HTTPError as http_err:
            self.logger.error(f'HTTP error occured: {http_err}')
            return None
        except Exception as e:
            self.logger.error(f'An error occured: {e}')
            return None
