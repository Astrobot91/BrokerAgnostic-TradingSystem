import pytz
import json
import gzip
import asyncio
import aiohttp
import polars as pl
from io import BytesIO
import upstox_client
from datetime import datetime
from upstox_client.rest import ApiException
from app.utils.logger import get_logger
from app.brokers.base_broker import BaseBroker
from app.authentication.upstox_authenticator import UpstoxAuthenticator


async def upstox_instruments():
    '''
    Mapping Index tradingsymbols into the upstox master file.
    Add the 'instrument_key' with it's respective 'tradingsymbol'

    '''
    instrument_keys = [
        "NSE_INDEX|Nifty 50", "NSE_INDEX|NIFTY100 EQL Wgt", "NSE_INDEX|NIFTY50 EQL Wgt", "NSE_INDEX|NiftyM150Momntm50",
        "NSE_INDEX|Nifty Auto", "NSE_INDEX|Nifty Commodities", "NSE_INDEX|Nifty Mid Liq 15", "NSE_INDEX|Nifty GS 10Yr Cln",
        "NSE_INDEX|NIFTY TOTAL MKT", "NSE_INDEX|Nifty Serv Sector", "NSE_INDEX|Nifty100 Liq 15", "NSE_INDEX|Nifty Bank",
        "NSE_INDEX|Nifty Next 50", "NSE_INDEX|NIFTY AlphaLowVol", "NSE_INDEX|Nifty Energy", "NSE_INDEX|Nifty Div Opps 50",
        "NSE_INDEX|NIFTY SMLCAP 50", "NSE_INDEX|Nifty PSE", "NSE_INDEX|NIFTY M150 QLTY50", "NSE_INDEX|NIFTY100 LowVol30",
        "NSE_INDEX|Nifty200Momentm30", "NSE_INDEX|Nifty100ESGSecLdr", "NSE_INDEX|Nifty GS 10Yr", "NSE_INDEX|NIFTY Alpha 50",
        "NSE_INDEX|Nifty 500", "NSE_INDEX|Nifty Realty", "NSE_INDEX|NIFTY INDIA MFG", "NSE_INDEX|NIFTY200 QUALTY30",
        "NSE_INDEX|Nifty GrowSect 15", "NSE_INDEX|NIFTY100 ESG", "NSE_INDEX|Nifty GS 8 13Yr", "NSE_INDEX|Nifty Infra",
        "NSE_INDEX|NIFTY SMLCAP 250", "NSE_INDEX|Nifty50 PR 1x Inv", "NSE_INDEX|NIFTY MIDCAP 100", "NSE_INDEX|Nifty FinSrv25 50",
        "NSE_INDEX|NIFTY CONSR DURBL", "NSE_INDEX|India VIX", "NSE_INDEX|Nifty Pharma", "NSE_INDEX|NIFTY MIDCAP 150",
        "NSE_INDEX|Nifty50 TR 1x Inv", "NSE_INDEX|Nifty PSU Bank", "NSE_INDEX|NIFTY HEALTHCARE", "NSE_INDEX|NIFTY500 MULTICAP",
        "NSE_INDEX|Nifty IT", "NSE_INDEX|NIFTY MIDSML 400", "NSE_INDEX|Nifty Media", "NSE_INDEX|Nifty 100",
        "NSE_INDEX|NIFTY100 Qualty30", "NSE_INDEX|NIFTY LARGEMID250", "NSE_INDEX|NIFTY SMLCAP 100", "NSE_INDEX|Nifty Midcap 50",
        "NSE_INDEX|NIFTY MICROCAP250", "NSE_INDEX|Nifty50 PR 2x Lev", "NSE_INDEX|Nifty200 Alpha 30", "NSE_INDEX|Nifty Fin Service",
        "NSE_INDEX|Nifty FMCG", "NSE_INDEX|Nifty50 Value 20", "NSE_INDEX|Nifty50 TR 2x Lev", "NSE_INDEX|Nifty50 Div Point",
        "NSE_INDEX|Nifty MNC", "NSE_INDEX|Nifty Consumption", "NSE_INDEX|Nifty Pvt Bank", "NSE_INDEX|Nifty CPSE",
        "NSE_INDEX|Nifty GS 11 15Yr", "NSE_INDEX|Nifty Metal", "NSE_INDEX|Nifty GS 15YrPlus", "NSE_INDEX|Nifty 200",
        "NSE_INDEX|NIFTY MID SELECT", "NSE_INDEX|Nifty GS Compsite", "NSE_INDEX|Nifty GS 4 8Yr", "NSE_INDEX|NIFTY IND DIGITAL",
        "NSE_INDEX|Nifty Tata 25 Cap", "NSE_INDEX|Nifty Multi Mfg", "NSE_INDEX|NIFTY OIL AND GAS", "NSE_INDEX|Nifty MidSml Hlth",
        "NSE_INDEX|Nifty Multi Infra", "MCX_INDEX|MCXBULLDEX", "BSE_INDEX|INDSTR", "BSE_INDEX|ALLCAP", "BSE_INDEX|SMLCAP",
        "BSE_INDEX|BSEFMC", "BSE_INDEX|REALTY", "BSE_INDEX|SMEIPO", "BSE_INDEX|LRGCAP", "BSE_INDEX|BSE500", "BSE_INDEX|FINSER",
        "BSE_INDEX|AUTO", "BSE_INDEX|BSECD", "BSE_INDEX|MFG", "BSE_INDEX|OILGAS", "BSE_INDEX|UTILS", "BSE_INDEX|GREENX",
        "BSE_INDEX|MIDSEL", "BSE_INDEX|BSEHC", "BSE_INDEX|ENERGY", "BSE_INDEX|SMLSEL", "BSE_INDEX|SENSEX", "BSE_INDEX|MIDCAP",
        "BSE_INDEX|CONDIS", "BSE_INDEX|COMDTY", "BSE_INDEX|BANKEX", "BSE_INDEX|CPSE", "BSE_INDEX|TECK", "BSE_INDEX|POWER",
        "BSE_INDEX|BSECG", "BSE_INDEX|CARBON", "BSE_INDEX|BSE200", "BSE_INDEX|BSE100", "BSE_INDEX|TELCOM", "BSE_INDEX|BSEIPO",
        "BSE_INDEX|BSEIT", "BSE_INDEX|INFRA", "BSE_INDEX|METAL", "BSE_INDEX|BSEPSU", "BSE_INDEX|SENSEX50"
    ]

    tradingsymbol_mappings = {
        'instrument_key': [],
        'trading_symbol': []
    }
    for key in instrument_keys:
        tradingsymbol_mappings['instrument_key'].append(key)
        tradingsymbol_mappings['trading_symbol'].append(key.split('|')[1].strip().upper())
    tradingsymbol_mappings_df = pl.DataFrame(tradingsymbol_mappings)

    instrument_link = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz'

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(instrument_link) as response:
                if response.status == 200:
                    compressed_data = BytesIO(await response.read())
                    with gzip.GzipFile(fileobj=compressed_data) as f:
                        data = json.load(f)
                        instrument_df = pl.DataFrame(data)
                        instrument_df = instrument_df.join(tradingsymbol_mappings_df, on='instrument_key', how='left')
                        instrument_df = instrument_df.with_columns(
                            pl.when(pl.col("trading_symbol") == '')
                            .then(pl.col("trading_symbol_right"))
                            .otherwise(pl.col("trading_symbol"))
                            .alias("trading_symbol")
                        ).drop("trading_symbol_right")
                        temp_df = instrument_df.to_pandas()
                        temp_df.to_csv('Upstox_Master_File.csv')
                        return instrument_df
                else:
                    error_msg = f"Failed to retrieve Upstox instrument data. Status code: {response.status}"
                    print(error_msg)
                    raise Exception(error_msg)
    except Exception as e:
        raise Exception(f"Error fetching instrument data: {e}")

class UpstoxBroker(BaseBroker):

    def __init__(self, account_name: str):
        self.account_name = account_name
        self.broker_name = 'Upstox'
        self.logger = get_logger(
            main_folder_name=f'Brokers/UpstoxBroker/Accounts/{self.account_name}',
            broker_name=self.broker_name,
            account_name=f'{self.account_name}'
        )
        self.authenticator = UpstoxAuthenticator(account_name=self.account_name)
        self.access_token = None
        self.configuration = None
        self.api_version = '2.0'
        self.order_instance = None
        self.portfolio_instance = None
        self.mktQuote_instance = None
        self.historical_instance = None
        self.instrument_df = None

    async def initialize(self):
        try:
            self.logger.info(f'Initializing UpstoxBroker for {self.account_name}')
            self.access_token = await self.authenticator.authenticate()
            self.configuration = upstox_client.Configuration()
            self.configuration.access_token = self.access_token
            self.order_instance = upstox_client.OrderApi(upstox_client.ApiClient(self.configuration))
            self.portfolio_instance = upstox_client.PortfolioApi(upstox_client.ApiClient(self.configuration))
            self.mktQuote_instance = upstox_client.MarketQuoteApi(upstox_client.ApiClient(self.configuration))
            self.historical_instance = upstox_client.HistoryApi()
            self.instrument_df = await upstox_instruments()
            if self.instrument_df is None:
                raise Exception("Instrument data could not be loaded.")
        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            raise

    async def place_order(self,
                          trading_symbol: str,
                          transaction_type: str,
                          quantity: int,
                          product: str,
                          exchange: str,
                          instrument_type: str = 'EQ',
                          order_type: str = 'MARKET',
                          price: float = None,
                          trigger_price: float = 0,
                          validity: str = None,
                          disclosed_quantity: int = None,
                          is_amo: bool = False,
                          tag: str = None) -> dict:
        """
        Places an order with Upstox.

        Args:
            trading_symbol (str): The trading symbol.
            transaction_type (str): 'BUY' or 'SELL'.
            quantity (int): The quantity to trade.
            product (str): Product code ('I for Intraday', 'D for Delivery', etc.).
            exchange (str): Exchange code ('NSE', 'BSE', etc.).
            instrument_type (str, optional): Instrument type ('EQ', 'FO'). Defaults to 'EQ'.
            order_type (str, optional): Order type ('MARKET', 'LIMIT', 'SL', 'SL-M'.). Defaults to 'MARKET'.
            price (float, optional): Price for LIMIT orders. Defaults to None.
            trigger_price (float, optional): Trigger price for SL orders. Defaults to 0.
            validity (str, optional): Validity of the order ('DAY', 'IOC'). Defaults to None.
            disclosed_quantity (int, optional): Disclosed quantity. Defaults to None.
            is_amo (bool, optional): After Market Order flag. Defaults to False.
            tag (str, optional): Order tag. Defaults to None.

        Returns:
            dict: Order response data.

        Raises:
            ValueError: If the instrument is not found.
            Exception: If order placement fails.
            ApiException: If an API error occurs.
        """
        try:
            instrument_rows = self.instrument_df.filter(
                (pl.col('exchange') == exchange) &
                (pl.col('instrument_type') == instrument_type)
                (pl.col('trading_symbol') == trading_symbol)
            )
            if instrument_rows.is_empty():
                error_msg = f"Instrument {trading_symbol} not found for exchange {exchange} and instrument type {instrument_type}."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]

            order_request = upstox_client.PlaceOrderRequest(
                instrument_token=instrument_key,
                transaction_type=transaction_type,
                order_type=order_type,
                quantity=quantity,
                product=product,
                price=price,
                trigger_price=trigger_price,
                validity=validity,
                disclosed_quantity=disclosed_quantity,
                is_amo=is_amo,
                tag=tag
            )

            order_response = await asyncio.to_thread(
                self.order_instance.place_order, body=order_request, api_version=self.api_version
            )
            if order_response._status == 'success':
                self.logger.info(f"Order placed successfully: {order_response._data}")
                return order_response._data
            else:
                error_msg = f"Order placement failed: {order_response.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except ApiException as e:
            self.logger.error(f"API exception during order placement: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception during order placement: {e}")
            raise

    async def modify_order(self,
                           order_id: str,
                           quantity: int = None,
                           price: float = None,
                           order_type: str = None,
                           trigger_price: float = None,
                           disclosed_quantity: int = None,
                           validity: str = None) -> dict:
        """
        Modifies an existing order.

        Args:
            order_id (str): Order ID of the placed order.
            quantity (int, optional): The quantity to modify. Defaults to None.
            price (float, optional): Price for LIMIT orders. Defaults to None.
            order_type (str, optional): Order type ('MARKET', 'LIMIT', etc.). Defaults to None.
            trigger_price (float, optional): Trigger price for SL orders. Defaults to None.
            disclosed_quantity (int, optional): Disclosed quantity. Defaults to None.
            validity (str, optional): Validity of the order ('DAY', 'IOC'). Defaults to None.

        Returns:
            dict: Order response data.

        Raises:
            Exception: If order modification fails.
            ApiException: If an API error occurs.
        """
        try:
            order_request = upstox_client.ModifyOrderRequest(
                order_id=order_id,
                quantity=quantity,
                price=price,
                order_type=order_type,
                trigger_price=trigger_price,
                validity=validity,
                disclosed_quantity=disclosed_quantity
            )
            order_response = await asyncio.to_thread(
                self.order_instance.modify_order, body=order_request, api_version=self.api_version
            )
            if order_response._status == 'success':
                self.logger.info(f"Order modified successfully: {order_response._data}")
                return order_response._data
            else:
                error_msg = f"Order modification failed: {order_response.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except ApiException as e:
            self.logger.error(f"API exception during order modification: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception during order modification: {e}")
            raise

    async def cancel_order(self, order_id: str) -> dict:
        """
        Cancels an existing order.

        Args:
            order_id (str): Order ID to cancel.

        Returns:
            dict: Order response data.

        Raises:
            Exception: If order cancellation fails.
            ApiException: If an API error occurs.
        """
        try:
            order_response = await asyncio.to_thread(
                self.order_instance.cancel_order, order_id=order_id, api_version=self.api_version
            )
            if order_response._status == 'success':
                self.logger.info(f"Order cancelled successfully: {order_response._data}")
                return order_response._data
            else:
                error_msg = f"Order cancellation failed: {order_response.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except ApiException as e:
            self.logger.error(f"API exception during order cancellation: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception during order cancellation: {e}")
            raise

    async def get_order_details(self, order_id: str) -> dict:
        """
        Retrieves order details for a specific order ID.

        Args:
            order_id (str): The order ID.

        Returns:
            dict: Order details.

        Raises:
            Exception: If unable to get order details.
            ApiException: If an API error occurs.
        """
        try:
            order_details = await asyncio.to_thread(
                self.order_instance.get_order_details, order_id=order_id, api_version=self.api_version
            )
            if order_details._status == 'success':
                self.logger.info(f"Order details retrieved successfully: {order_details._data}")
                return order_details._data
            else:
                error_msg = f"Unable to get order details: {order_details.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except ApiException as e:
            self.logger.error(f"API exception occurred while getting order details: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception occurred while getting order details: {e}")
            raise

    async def get_orderbook(self) -> list:
        """
        Retrieves the current order book.

        Returns:
            list: List of orders.

        Raises:
            Exception: If unable to get order book.
            ApiException: If an API error occurs.
        """
        try:
            orderbook_data = await asyncio.to_thread(
                self.order_instance.get_order_book, api_version=self.api_version
            )
            if orderbook_data._status == 'success':
                self.logger.info(f"Order book retrieved successfully.")
                return orderbook_data._data
            else:
                error_msg = f"Unable to get order book: {orderbook_data.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except ApiException as e:
            self.logger.error(f"API exception occurred while getting order book: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception occurred while getting order book: {e}")
            raise

    async def get_current_positions(self) -> list:
        """
        Retrieves the current positions.

        Returns:
            list: List of current positions.

        Raises:
            Exception: If unable to get current positions.
            ApiException: If an API error occurs.
        """
        try:
            current_positions = await asyncio.to_thread(
                self.portfolio_instance.get_positions, api_version=self.api_version
            )
            if current_positions._status == 'success':
                self.logger.info(f"Current positions retrieved successfully.")
                return current_positions._data
            else:
                error_msg = f"Unable to get current positions: {current_positions.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except ApiException as e:
            self.logger.error(f"API exception occurred while getting current positions: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception occurred while getting current positions: {e}")
            raise

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
            ValueError: If the instrument is not found.
            Exception: If getting LTP quote fails.
            ApiException: If an API error occurs.
        """
        upstox_tradingsymbol_list = []
        for tradingsymbol, value in tradingsymbol_list.items():
            exchange = value['exchange']
            instrument_type = value['instrument_type']
            instrument_rows = self.instrument_df.filter(
                (pl.col('segment') == f'{exchange}_{instrument_type}') &
                (pl.col('trading_symbol') == tradingsymbol)
            )
            if instrument_rows.is_empty():
                error_msg = f"Instrument {tradingsymbol} not found for exchange {exchange} and instrument type {instrument_type}."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]
            upstox_tradingsymbol_list.append(instrument_key)
        
        try:
            instrument_key_list = ','.join(upstox_tradingsymbol_list)
            ltp_response = await asyncio.to_thread(
                self.mktQuote_instance.ltp, symbol=instrument_key_list, api_version=self.api_version
            )
            if ltp_response._status == 'success':
                data = await self.convert_quote(dictionary=ltp_response._data)
                return data
            else:
                error_msg = f"LTP response failed: {ltp_response.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except ApiException as e:
            self.logger.error(f"API exception while getting LTP quote: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception while getting LTP quote: {e}")
            raise
    
    async def ohlc_quote(self,
                  tradingsymbol_list: dict,
                  interval: str) -> dict:
        """
        Gets OHLC quotes for list of stocks.
        
        Args:
            trading_symbol_list (dict): The list of trading symbols with their exchange ('NSE', 'BSE'.) and instrument type ('EQ', 'FO'.).
            interval (str): The interval for getting OHLC quotes ('1d')

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
            ValueError: If the instrument is not found.
            Exception: If OHLC quote fails.
            ApiException: If an API error occurs.
        """
        interval_dict = {
            'day': '1d'
        }
        if interval in interval_dict:
            interval = interval_dict[interval]

        upstox_tradingsymbol_list = []
        for tradingsymbol, value in tradingsymbol_list.items():
            exchange = value['exchange']
            instrument_type = value['instrument_type']
            instrument_rows = self.instrument_df.filter(
                (pl.col('segment') == f'{exchange}_{instrument_type}') &
                (pl.col('trading_symbol') == tradingsymbol)
            )
            if instrument_rows.is_empty():
                error_msg = f"Instrument {tradingsymbol} not found for exchange {exchange} and instrument type {instrument_type}."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]
            upstox_tradingsymbol_list.append(instrument_key)
        
        try:
            instrument_key_list = ','.join(upstox_tradingsymbol_list)
            ohlc_response = await asyncio.to_thread(
                self.mktQuote_instance.get_market_quote_ohlc, symbol=instrument_key_list, interval=interval_dict[interval], api_version=self.api_version
            )
            if ohlc_response._status == 'success':
                data = await self.convert_quote(dictionary=ohlc_response._data)
                return data
            else:
                error_msg = f"OHLC response failed: {ohlc_response.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except ApiException as e:
            self.logger.error(f"API exception while getting OHLC quote: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception while getting OHLC quote: {e}")
            raise

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
            ValueError: If the instrument is not found.
            Exception: If full market quote fails.
            ApiException: If an API error occurs.
        """
        upstox_tradingsymbol_list = []
        for tradingsymbol, value in tradingsymbol_list.items():
            exchange = value['exchange']
            instrument_type = value['instrument_type']
            instrument_rows = self.instrument_df.filter(
                (pl.col('segment') == f'{exchange}_{instrument_type}') &
                (pl.col('trading_symbol') == tradingsymbol)
            )
            if instrument_rows.is_empty():
                error_msg = f"Instrument {tradingsymbol} not found for exchange {exchange} and instrument type {instrument_type}."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]
            upstox_tradingsymbol_list.append(instrument_key)

        try:
            instrument_key_list = ','.join(upstox_tradingsymbol_list)
            full_mkt_response = await asyncio.to_thread(
                self.mktQuote_instance.get_full_market_quote, symbol=instrument_key_list, api_version=self.api_version
            )
            if full_mkt_response._status == 'success':
                data = await self.convert_quote(dictionary=full_mkt_response._data)
                return data
            else:
                error_msg = f"Full market response failed: {full_mkt_response.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except ApiException as e:
            self.logger.error(f"API exception while getting full market quote: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception while getting full market quote: {e}")
            raise

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
            ValueError: If the instrument is not found in the instrument data.
            ApiException: If an API exception occurs during the API call.
            Exception: For any other exceptions that occur during data retrieval.
        """
        try:
            instrument_rows = self.instrument_df.filter(
                (pl.col('segment') == f'{exchange}_{instrument_type}') &
                (pl.col('trading_symbol') == trading_symbol)
            )
            if instrument_rows.is_empty():
                error_msg = f"Instrument {trading_symbol} not found for exchange {exchange} and instrument type {instrument_type}."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            instrument_key = instrument_rows['instrument_key'][0]

            data_response = await asyncio.to_thread(
                self.historical_instance.get_historical_candle_data1,
                instrument_key=instrument_key,
                interval=interval,
                to_date=to_date,
                from_date=from_date,
                api_version=self.api_version
            )
            if data_response._status == 'success':
                    self.logger.info(f"Historical data for {trading_symbol} from: {from_date} to: {to_date} at interval: {interval} retrieved successfully.")
                    data = await self._convert_to_polars_df(data=data_response._data,
                                                     trading_symbol=trading_symbol,
                                                     instrument_type=instrument_type,
                                                     exchange=exchange,
                                                     interval=interval,
                                                     from_date=from_date,
                                                     to_date=to_date) 
                    return data
           
            else:
                error_msg = f"Historical data for {trading_symbol} from: {from_date} to: {to_date} at interval: {interval} retrieval failed.: {data_response.__dict__}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except ApiException as e:
            self.logger.error(f"API exception while getting historical data for {trading_symbol} from: {from_date} to: {to_date} at interval: {interval}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Exception while getting historical data for {trading_symbol} from: {from_date} to: {to_date} at interval: {interval}: {e}")
            raise

    async def convert_quote(self, dictionary: dict) -> dict:
        """
        Converts instrument tokens in the quote data to trading symbols.

        This function takes a dictionary of quote data where the keys are instrument tokens,
        and converts the instrument tokens to their corresponding trading symbols using the
        instrument DataFrame (`self.instrument_df`). It returns a new dictionary with trading
        symbols as keys and the original quote data as values.

        Args:
            dictionary (dict): A dictionary where each value contains quote data with an 'instrument_token' key.

        Returns:
            dict: A dictionary with trading symbols as keys and quote data as values.

        Raises:
            Exception: If an error occurs during the conversion process.
        """
        output_dict = {}
        for key, value in dictionary.items():
            instrument_key = value._instrument_token
            if instrument_key is None:
                self.logger.error(f"Missing 'instrument_token' in quote data for key {key}")
                continue
            try:
                temp_df = self.instrument_df.filter(
                    pl.col('instrument_key') == instrument_key
                )
                if temp_df.is_empty():
                    self.logger.error(f"No matching instrument for token {instrument_key}")
                    continue
                new_key = temp_df['trading_symbol'][0]
                output_dict[new_key] = value
            except Exception as e:
                self.logger.error(f"Error converting instrument token {instrument_key}: {e}")
                raise
        return output_dict

    async def _convert_to_polars_df(self,
                             data: dict,
                             trading_symbol: str,
                             instrument_type: str,
                             exchange: str,
                             interval: str,
                             from_date: str,
                             to_date: str) -> pl.DataFrame:
        """
        Converts provided candle data to a Polars DataFrame with a timezone-adjusted datetime column.
        
        Args:
            trading_symbol (str): The trading symbol of the instrument.
            data (dict): Dictionary containing candle data with datetime and OHLC values.
            interval (str): The interval for the historical data (e.g., '1minute', '5minute', '1day').
            from_date (str): The start date for the historical data in 'YYYY-MM-DD' format.
            to_date (str): The end date for the historical data in 'YYYY-MM-DD' format.

        Returns:
            pl.DataFrame: Polars DataFrame with adjusted datetime column.
        """
        data = data.__dict__
        candles = data.get('_candles', [])
        
        if candles:
            data_dict = {
                "datetime": [item[0] for item in candles],
                "open": [item[1] for item in candles],
                "high": [item[2] for item in candles],
                "low": [item[3] for item in candles],
                "close": [item[4] for item in candles],
                "volume": [item[5] for item in candles],
                "oi": [item[6] for item in candles],
            }
            df = pl.DataFrame(data_dict, strict=False)
            df = df.with_columns(
                pl.col("datetime")
                .str.strptime(pl.Datetime)
                .dt.convert_time_zone("Asia/Kolkata")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )

            tradingsymbol_list = {
                trading_symbol: {
                    'exchange': exchange,
                    'instrument_type': instrument_type
                }
            }
            todays_mkt_quote = await self.full_market_quote(tradingsymbol_list=tradingsymbol_list)

            row = {
                'datetime': todays_mkt_quote[trading_symbol]._timestamp,
                'open': todays_mkt_quote[trading_symbol]._ohlc._open,
                'high': todays_mkt_quote[trading_symbol]._ohlc._high,
                'low': todays_mkt_quote[trading_symbol]._ohlc._low,
                'close': todays_mkt_quote[trading_symbol]._ohlc._close,
                'volume': todays_mkt_quote[trading_symbol]._volume,
                'oi': todays_mkt_quote[trading_symbol]._oi,
            }

            todays_df = pl.DataFrame(row)
            todays_df = todays_df.with_columns(
                pl.col('datetime')
                .str.strptime(pl.Datetime)
                .dt.convert_time_zone("Asia/Kolkata")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )

            todays_df = todays_df.with_columns([
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Int64),
                pl.col("oi").cast(pl.Int64)
            ])

            combined_df = pl.concat([df, todays_df])
            combined_df = combined_df.sort('datetime')
            return combined_df

        else:
            self.logger.warning(f"Historical data for {trading_symbol} from: {from_date} to: {to_date} at interval: {interval} not found.")
            return pl.DataFrame(data={}, strict=False)
