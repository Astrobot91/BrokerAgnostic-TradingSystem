import re
import aiohttp
import requests
import polars as pl
from bs4 import BeautifulSoup
from datetime import datetime,  timedelta
from app.utils.logger import get_logger
from app.brokers.broker_factory import BrokerFactory


class BrokerDataFeed:
    
    def __init__(self, broker_name: str, account_name: str):
        self.broker_name = broker_name
        self.account_name = account_name
        self.logger =  get_logger(main_folder_name='BrokerDatafeed', broker_name=self.broker_name, account_name=self.account_name)
        self.broker = None

    async def initialize(self):
        try:
            self.broker = BrokerFactory.get_broker(broker_name=self.broker_name, account_name=self.account_name)
            await self.broker.initialize()
            self.logger.info(f'BrokerDataFeed initialized for broker: {self.broker_name}, account: {self.account_name}')
        except Exception as e:
            error_msg = f"Failed to initialize BrokerDataFeed for broker: {self.broker_name}, account: {self.account_name}, Error: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def historical_data(self,
                            trading_symbol: str,
                            exchange: str,
                            instrument_type: str,
                            interval: str,
                            from_date: str,
                            to_date: str) -> pl.DataFrame:
        """
        Gets historical data from Upstox.

        Args:
            trading_symbol (str): The trading symbol of the instrument.
            exchange (str): The exchange code (e.g., 'NSE', 'BSE', etc.).
            instrument_type (str): The instrument type (e.g., 'EQ' for equity, 'FO' for futures and options).
            interval (str): The interval for the historical data (e.g., '1minute', '5minute', '1day').
            from_date (str): The start date for the historical data in 'YYYY-MM-DD' format.
            to_date (str): The end date for the historical data in 'YYYY-MM-DD' format.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the historical candle data.

        Raises:
            Exception: For any other exceptions that occur during data retrieval.
        """
        try:
            data = await self.broker.historical_data(trading_symbol=trading_symbol,
                                               exchange=exchange,
                                               instrument_type=instrument_type,
                                               interval=interval,
                                               from_date=from_date,
                                               to_date=to_date)
            return data
        
        except Exception as e:
            error_msg = f"Exception occured while getting historical data for {trading_symbol} from: {from_date} to: {to_date} at interval: {interval}: {e}"
            self.logger.error(error_msg) 
            raise Exception(error_msg)

    async def ltp_quote(self,
                        tradingsymbol_list: dict) -> dict:
        """
        Gets last traded price for dict of stocks.
        
        Args:
            trading_symbol_list (dict): The list of trading symbols with their exchange ('NSE', 'BSE'.) and instrument type ('EQ', 'FO'.).
        
        Example: 
            trading_symbol_list = {
                            RELIANCE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE : {
                                'exchange' : 'BSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE24DEC1100CE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'FO'
                                }
                            }
            
        Returns:
            dict: Last traded price data.
        
        Raises:
            Exception: If getting LTP quote fails.
        """
        try:
            data = await self.broker.ltp_quote(tradingsymbol_list=tradingsymbol_list)
            return data
        except Exception as e:
            error_msg = f"Exception while getting LTP quote for {tradingsymbol_list}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        
    async def ohlc_quote(self,
                         tradingsymbol_list: dict,
                         interval: str) -> dict:
        """
        Gets OHLC quotes for list of stocks.
        
        Args:
            trading_symbol_list (dict): The list of trading symbols with their exchange ('NSE', 'BSE'.) and instrument type ('EQ', 'FO'.).
        
        Example: 
            trading_symbol_list = {
                            RELIANCE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE : {
                                'exchange' : 'BSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE24DEC1100CE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'FO'
                                }
                            }
            
        Returns:
            dict: OHLC price data for all instruments.
        
        Raises:
            Exception: If OHLC quote fails.
        """
        try:
            data = await self.broker.ohlc_quote(tradingsymbol_list=tradingsymbol_list, interval=interval)
            return data
        except Exception as e:
            error_msg = f"Exception while getting OHLC quote for {tradingsymbol_list}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    async def full_market_quote(self, tradingsymbol_list: dict) -> dict:
        """
        Gets full market quotes with limit order depth for list of stocks.
        
        Args:
            trading_symbol_list (dict): The list of trading symbols with their exchange ('NSE', 'BSE'.) and instrument type ('EQ', 'FO'.).

        Example: 
            trading_symbol_list = {
                            RELIANCE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE : {
                                'exchange' : 'BSE',
                                'instrument_type' : 'EQ'
                                },
                            RELIANCE24DEC1100CE : {
                                'exchange' : 'NSE',
                                'instrument_type' : 'FO'
                                }
                            }
            
        Returns:
            dict: Full market quote with limit order depth data.
        
        Raises:
            Exception: If full market quote fails.
        """
        try:
            data = await self.broker.full_market_quote(tradingsymbol_list=tradingsymbol_list)
            return data
        except Exception as e:
            error_msg = f"Exception while getting full market quote for {tradingsymbol_list}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)


class FundamentalDataFeed:

    @staticmethod
    def marketCapData(isin_code: str = None) -> str:

        logger = get_logger(main_folder_name='FundamentalDataFeed')
        
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
                    logger.warning(f'Slug not found in search response.')

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
                        logger.warning('No numbers found in the marketcap text.')
                else:
                    logger.warning('Market cap element not found on the page.')
            else:
                logger.warning(f'No data found for the provided ISIN code: {isin_code}')
        
            return None
        except requests.HTTPError as http_err:
            logger.error(f'HTTP error occured: {http_err}')
            return None
        except Exception as e:
            logger.error(f'An error occured: {e}')
            return None
