import asyncio
import polars as pl
from datetime import datetime, date, timedelta
from app.utils.logger import get_logger
from app.utils.telegram import broadcast_signal
from app.execution.trade_executor import TradeExecutor
from app.portfolio.portfolio_manager import PortfolioManager
from app.data.datafeed import BrokerDataFeed, FundamentalDataFeed
from app.config import STRATEGY_CONFIGS


SIGNAL_DATE = datetime.today()

class MomentumStrategyV1:
    """
    Momentum Strategy Version 1:
    Identifies stocks that have hit their 52-week high or low and generates trading signals based on these events.
    """

    def __init__(self,
                 datafeed_broker_name: str,
                 executor_broker_name: str,
                 datafeed_account_name: str,
                 executor_account_name: str,
                 ):
        self.strategy_name = 'MomentumStrategyV1'
        self.strategy_config = STRATEGY_CONFIGS.get(self.strategy_name)
        self.datafeed_broker_name = datafeed_broker_name
        self.executor_broker_name = executor_broker_name
        self.datafeed_account_name = datafeed_account_name
        self.executor_account_name = executor_account_name
        self.logger = get_logger(
            main_folder_name=f'Strategies/{self.strategy_name}',
            broker_name=self.executor_broker_name,
            account_name=self.executor_account_name
        )
        self.isin_df = None
        self.brokerdatafeed = None
        self.fundamental_datafeed = None
        self.trade_executor = None
        self.portfolio_manager = None
        self.instrument_df = None

    async def initialize(self) -> None:
        try:
            self.fundamental_datafeed = FundamentalDataFeed()
            self.brokerdatafeed = BrokerDataFeed(
                broker_name=self.datafeed_broker_name,
                account_name=self.datafeed_account_name
            )
            await self.brokerdatafeed.initialize()

            self.trade_executor = TradeExecutor(
                broker_name=self.executor_broker_name,
                account_name=self.executor_account_name
            )
            await self.trade_executor.initialize()

            self.portfolio_manager = PortfolioManager(
                broker_name=self.executor_broker_name,
                account_name=self.executor_account_name
            )
            await self.portfolio_manager.initialize()

            self.instrument_df = self.brokerdatafeed.broker.instrument_df

            isin_file_path = self.strategy_config.get('ISIN_FILE_PATH')
            self.isin_df = pl.read_csv(isin_file_path, infer_schema_length=10000)

            self.logger.info("MomentumStrategyV1 initialized successfully.")
        except Exception as e:
            error_msg = (f'Failed to initialize {self.strategy_name} for broker: {self.executor_broker_name}, '
                         f'account: {self.executor_account_name}. Error: {e}')
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def execute(self) -> None:
        """
        Main execution method for the strategy.
        Generates signals and executes trades based on those signals.
        """
        try:
            qualified_futures = await self._get_recent_expiry_futures()
            if not qualified_futures:
                self.logger.info("No qualified futures contracts to trade.")
                return

            self.logger.info(f"Qualified futures contracts: {qualified_futures}")
            # await self._execute_trades(qualified_futures)
        except Exception as e:
            self.logger.error(f"Error during strategy execution: {e}")

    async def _get_recent_expiry_futures(self) -> dict:
        """
        Retrieves futures contracts that are nearest to expiry and meet the holding period criteria.
        """
        try:
            signals = await self.generate_signals()
            if signals is None:
                self.logger.info('No signals generated today.')
                return None

            list_of_qualified_stocks = signals['trading_symbol'].to_list()
            master_file_slice = self.instrument_df.filter(
                (pl.col('instrument_type') == 'FUT') &
                (pl.col('underlying_symbol').is_in(list_of_qualified_stocks))
            ).with_columns(
                pl.col('expiry').map_elements(
                    self._epoch_to_naive_datetime,
                    return_dtype=pl.Datetime
                ).alias('expiry')
            )

            nearest_expiry = master_file_slice['expiry'].min()
            todays_date = datetime.today()
            date_difference = nearest_expiry - todays_date

            holding_period = self.strategy_config.get('HOLDING_PERIOD')

            if date_difference.days > holding_period:
                futures_slice = master_file_slice.filter(pl.col('expiry') == nearest_expiry)
                if not futures_slice.is_empty():
                    tradingsymbol_list = {
                        symbol: {
                            'exchange': 'NSE',
                            'instrument_type': 'FUT',
                        } for symbol in futures_slice['trading_symbol'].to_list()
                    }
                    self.logger.info(f'Futures contracts generated today: {tradingsymbol_list}')
                    
                    telegram_msg = f'Signals for : {(SIGNAL_DATE + timedelta(days=1)).date()} 09:15:00\n\n'
                    i = 1
                    for contracts, values in tradingsymbol_list.items():
                        telegram_msg += f'{i}. {contracts}\n'
                        i += 1
                    telegram_msg += f'\nSignal Provider details:\nBroker: {self.datafeed_broker_name}\nAccount: {self.datafeed_account_name}'
                    broadcast_signal(signal=telegram_msg)
                    return tradingsymbol_list
                
                else:
                    self.logger.info('No futures contracts found for the nearest expiry.')
            else:
                self.logger.info("Expiry date is within the holding period. Skipping the signals.")
            return None
        except Exception as e:
            self.logger.exception(f"Error fetching recent expiry futures: {e}")
            return None

    async def generate_signals(self) -> pl.DataFrame:
        """
        Generates trading signals based on 52-week high/low criteria.
        """
        try:
            fo_trading_symbols = await self._futures_stocks()

            if not fo_trading_symbols:
                self.logger.info("No futures trading symbols found.")
                return None

            to_date = date.today()
            from_date = to_date - timedelta(days=1000)

            stock_df_store = []
            for trading_symbol in fo_trading_symbols:
                isin_code = self._get_isin_code(trading_symbol)
                if not isin_code:
                    self.logger.warning(f"ISIN code not found for symbol: {trading_symbol}")
                    continue

                mcap = self.fundamental_datafeed.marketCapData(isin_code=isin_code)
                hist_stock_df = await self.brokerdatafeed.historical_data(
                    trading_symbol=trading_symbol,
                    exchange='NSE',
                    instrument_type='EQ',
                    interval='day',
                    from_date=str(from_date),
                    to_date=str(to_date)
                )
                await asyncio.sleep(0.5)  

                if hist_stock_df.is_empty():
                    self.logger.warning(f"No historical data for symbol: {trading_symbol}")
                    continue

                hist_stock_df = self._process_historical_data(hist_stock_df, trading_symbol, mcap)
                stock_df_store.append(hist_stock_df)

            if not stock_df_store:
                self.logger.info("No historical data available to generate signals.")
                return None

            stock_data = pl.concat(stock_df_store)
            stock_data = stock_data.filter(
                (pl.col('datetime') == SIGNAL_DATE) &
                (
                    (pl.col('final_high_signal') == 1) |
                    (pl.col('final_low_signal') == 1)
                )
            )

            if stock_data.is_empty():
                self.logger.info('No signals generated today.')
                return None

            stock_data = stock_data.sort(self.strategy_config.get('SORT_BY'))
            stock_data = stock_data.head(self.strategy_config.get('PORTFOLIO_SIZE'))
            return stock_data
        except Exception as e:
            self.logger.error(f"Error generating signals: {e}")
            return None

    async def _execute_trades(self, tradingsymbol_list: dict) -> None:
        """
        Executes trades based on the qualified futures contracts.
        """
        try:
            for symbol, details in tradingsymbol_list.items():
                await self.trade_executor.place_order(
                    symbol=symbol,
                    quantity=self.strategy_config.get('ORDER_QUANTITY'),
                    side='buy',
                    order_type='market'
                )
                self.logger.info(f"Placed order for {symbol}")
        except Exception as e:
            self.logger.exception(f"Error executing trades: {e}")

    async def _futures_stocks(self) -> tuple:
        """
        Retrieves futures trading symbols based on the broker's segment.
        """
        try:
            if self.datafeed_broker_name.lower() == 'upstox':
                fo_stocks_list = self.instrument_df.filter(pl.col('segment') == 'NSE_FO')
                equity_df = self.instrument_df.filter(pl.col('segment') == 'NSE_EQ')
            elif self.datafeed_broker_name.lower() == 'zerodha':
                fo_stocks_list = self.instrument_df.filter(pl.col('segment') == 'NFO-FUT')
                equity_df = self.instrument_df.filter(
                    (pl.col('instrument_type') == 'EQ') & (pl.col('segment') == 'NSE')
                )
            else:
                self.logger.error(f"Unsupported broker: {self.datafeed_broker_name}")
                return tuple()

            fo_names = fo_stocks_list.select('name').unique()
            merged_df = equity_df.join(fo_names, on='name', how='inner')
            fo_trading_symbols = tuple(merged_df['trading_symbol'].unique())
            return fo_trading_symbols
        except Exception as e:
            self.logger.exception(f"Error retrieving futures stocks: {e}")
            return tuple()

    def _get_isin_code(self, trading_symbol: str) -> str:
        """
        Retrieves the ISIN code for a given trading symbol.
        """
        isin_record = self.isin_df.filter(pl.col('Security Id') == trading_symbol)
        if isin_record.is_empty():
            return None
        return isin_record['ISIN No'][0]

    def _process_historical_data(self, hist_stock_df: pl.DataFrame, trading_symbol: str, mcap: float) -> pl.DataFrame:
        """
        Processes historical stock data by adding necessary columns and calculating signals.
        """
        try:
            hist_stock_df = hist_stock_df.with_columns(
                pl.col("datetime").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("datetime"),
                pl.lit(trading_symbol).alias('trading_symbol'),
                pl.lit(mcap).alias('marketcap')
            )
            hist_stock_df = hist_stock_df.sort('datetime')
            hist_stock_df = hist_stock_df.with_columns([
                pl.col('high').rolling_max(window_size=252).alias('52w_high'),
                pl.col('low').rolling_min(window_size=252).alias('52w_low')
            ])
            hist_stock_df = hist_stock_df.with_columns([
                pl.when(pl.col('high') == pl.col('52w_high')).then(1).otherwise(0).alias('52w_high_signal'),
                pl.when(pl.col('low') == pl.col('52w_low')).then(1).otherwise(0).alias('52w_low_signal')
            ])
            hist_stock_df = hist_stock_df.with_columns([
                (
                    (pl.col('52w_high_signal') == 1) &
                    (
                        pl.col('52w_high_signal')
                        .shift(1)
                        .rolling_max(window_size=21)
                        .fill_null(0)
                        == 0
                    )
                ).cast(pl.Int8).alias('final_high_signal'),
                (
                    (pl.col('52w_low_signal') == 1) &
                    (
                        pl.col('52w_low_signal')
                        .shift(1)
                        .rolling_max(window_size=21)
                        .fill_null(0)
                        == 0
                    )
                ).cast(pl.Int8).alias('final_low_signal')
            ])
            return hist_stock_df
        except Exception as e:
            self.logger.exception(f"Error processing historical data for {trading_symbol}: {e}")
            return pl.DataFrame()

    def _epoch_to_naive_datetime(self, epoch_ms: int) -> datetime:
        """
        Converts epoch milliseconds to a naive datetime object.
        """
        try:
            dt_with_tz = datetime.fromtimestamp(epoch_ms / 1000)
            dt_naive = dt_with_tz.replace(tzinfo=None)
            return dt_naive
        except Exception as e:
            self.logger.exception(f"Error converting epoch to datetime: {e}")
            return datetime.min  









    async def load_positions(self) -> dict:
        try:
            open_positions = await self.portfolio_manager.get_current_positions()
            ...
        except Exception as e:
            ...

    async def shutdown(self) -> None:
        """
        Shuts down the strategy by closing data feeds and executors.
        """
        try:
            if self.brokerdatafeed:
                await self.brokerdatafeed.shutdown()
            if self.trade_executor:
                await self.trade_executor.shutdown()
            if self.portfolio_manager:
                await self.portfolio_manager.shutdown()
            self.logger.info(f"{self.strategy_name} shutdown successfully.")
        except Exception as e:
            self.logger.error(f"Error during strategy shutdown: {e}")