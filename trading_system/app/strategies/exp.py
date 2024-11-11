import time
import asyncio
import polars as pl
from app.utils.logger import get_logger
from datetime import datetime, date, timedelta
from app.execution.trade_executor import TradeExecutor
from app.portfolio.portfolio_manager import PortfolioManager
from app.data.datafeed import BrokerDataFeed, FundamentalDataFeed
from app.strategies.strategy_base import BaseStrategy
from app.config import STRATEGY_CONFIGS


class MomentumStrategyV1:

    def __init__(self,
                 datafeed_broker_name: str,
                 executor_broker_name: str,
                 datafeed_account_name: str,
                 executor_account_name: str):
        
        self.strategy_name = 'MomentumStrategyV1'
        self.strategy_config = STRATEGY_CONFIGS.get(self.strategy_name)
        self.datafeed_account_name = datafeed_account_name
        self.executor_account_name = executor_account_name
        self.datafeed_broker_name = datafeed_broker_name
        self.executor_broker_name = executor_broker_name
        self.logger = get_logger(main_folder_name=f'Strategies/{self.strategy_name}',
                                 broker_name=self.executor_broker_name,
                                 account_name=self.executor_account_name)
        self.isin_df = None
        self.brokerdatafeed = None
        self.fundamental_datafeed = None
        self.trade_executor = None
        self.portfolio_manager = None
        self.master_file = None

    async def initialize(self):
        try: 
            self.fundamental_datafeed = FundamentalDataFeed()
            self.brokerdatafeed = BrokerDataFeed(broker_name=self.datafeed_broker_name, account_name=self.datafeed_account_name)
            await self.brokerdatafeed.initialize()
            self.trade_executor = TradeExecutor(broker_name=self.executor_broker_name, account_name=self.executor_account_name)
            await self.trade_executor.initialize()
            self.portfolio_manager = PortfolioManager(broker_name=self.executor_broker_name, account_name=self.executor_account_name)
            await self.portfolio_manager.initialize()
            self.instrument_df = self.brokerdatafeed.broker.instrument_df
            self.isin_df = pl.read_csv('/home/admin/Strategy_Execution_Engine/trading_system/app/data/ISIN_Codes.csv', infer_schema_length=10000)
        except Exception as e:
            error_msg = f'Failed to initialize MomentumStrategyV1 for broker: {self.executor_broker_name}. account: {self.executor_account_name}, Error: {e}'
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def futures_stocks(self) -> tuple:
        
        if self.datafeed_broker_name == 'Upstox':
            fo_stocks_list = self.instrument_df.filter(pl.col('segment') == 'NSE_FO')
            equity_df = self.instrument_df.filter(pl.col('segment') == 'NSE_EQ')
            fo_names = fo_stocks_list['name'].unique().to_frame()
            merged_df = equity_df.join(fo_names, on='name')
            fo_trading_symbols = tuple(merged_df['trading_symbol'].unique())
            return fo_trading_symbols
        
        if self.datafeed_broker_name == 'Zerodha':
            fo_stocks_list = self.instrument_df.filter(pl.col('segment') == 'NFO-FUT')
            equity_df = self.instrument_df.filter(
                (pl.col('instrument_type') == 'EQ') & (pl.col('segment') == 'NSE')
            )
            fo_names = fo_stocks_list['name'].unique().to_frame()
            merged_df = equity_df.join(fo_names, on='name')
            fo_trading_symbols = tuple(merged_df['trading_symbol'].unique())
            return fo_trading_symbols

    async def generate_signals(self):
        fo_trading_symbols = await self.futures_stocks()
        
        to_date = date.today()
        from_date = to_date - timedelta(days=1000)

        stock_df_store = []
        for trading_symbol in fo_trading_symbols:
            isin_df = self.isin_df.filter(pl.col('Security Id') == trading_symbol)
            if not isin_df.is_empty():
                isin_code = isin_df['ISIN No'][0]
            else:
                isin_code = None
            mcap = self.fundamental_datafeed.marketCapData(isin_code=isin_code)
            hist_stock_df = await self.brokerdatafeed.historical_data(trading_symbol=trading_symbol,
                                                                    exchange='NSE',
                                                                    instrument_type='EQ',
                                                                    interval='day',
                                                                    from_date=str(from_date),
                                                                    to_date=str(to_date))
            time.sleep(0.5)
            if not hist_stock_df.is_empty():
                hist_stock_df = hist_stock_df.with_columns(
                    pl.col("datetime").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("datetime")
                )
                hist_stock_df = hist_stock_df.with_columns([
                    pl.lit(trading_symbol).alias('trading_symbol'),
                    pl.lit(mcap).alias('marketcap')
                ])
                hist_stock_df = hist_stock_df.sort('datetime')
                hist_stock_df = hist_stock_df.with_columns([
                    pl.col('high').rolling_max(window_size=252).alias('52w_high'),
                    pl.col('low').rolling_min(window_size=252).alias('52w_low')
                ])
                hist_stock_df = hist_stock_df.with_columns(
                    pl.when(pl.col('high') == pl.col('52w_high')).then(1).otherwise(0).alias('52w_high_signal'),
                    pl.when(pl.col('low') == pl.col('52w_low')).then(1).otherwise(0).alias('52w_low_signal')
                )
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

                stock_df_store.append(hist_stock_df)
        
        stock_data = pl.concat(stock_df_store)
        stock_data = stock_data.filter(
            (pl.col('datetime') == datetime(2024, 11, 7, 0, 0, 0)) &
            (
                (pl.col('final_high_signal') == 1) |
                (pl.col('final_low_signal') == 1)
            )
        )
        if not stock_data.is_empty():
            stock_data = stock_data.sort(self.strategy_config.get('SORT_BY'))
            stock_data = stock_data[:self.strategy_config.get('PORTFOLIO_SIZE')]
            return stock_data
        else:
            self.logger.info('No signals generated today.')

    async def get_recent_expiry_futures(self):
        
        qualified_stocks = await self.generate_signals()
        list_of_qualified_stocks = list(qualified_stocks['trading_symbol'])

        master_file_slice = self.instrument_df.filter(
            (pl.col('instrument_type') == 'FUT') &
            (pl.col('underlying_symbol').is_in(list_of_qualified_stocks))
        )
        master_file_slice = master_file_slice.with_columns(
            pl.col('expiry').map_elements(self.epoch_to_naive_datetime, return_dtype=pl.Datetime).alias('expiry')
        )
        nearest_expiry = master_file_slice['expiry'].min()

        todays_date = datetime.today()
        date_difference = nearest_expiry - todays_date
        if date_difference.days > self.strategy_config.get('HOLDING_PERIOD'):

            futures_slice = master_file_slice.filter(
                pl.col('expiry') == nearest_expiry
            )
            if not futures_slice.is_empty():
                
                tradingsymbol_list = {}
                for futures_contract in futures_slice['trading_symbol']:
                    tradingsymbol_list[futures_contract] = {
                        'exchange': 'NSE',
                        'instrument_type': 'FUT',
                    }
                
                self.logger.info(f'Futures contracts generated today are: {tradingsymbol_list}.')
                return tradingsymbol_list
            
            else:
                self.logger.info(f'Futures contracts not found.')
        else:
            self.logger.info(f"Difference in today's date and expiry date is less than holding period. Skipping the signals...")
    
    async def execute_trades(self):
        ...


    def epoch_to_naive_datetime(self, epoch_ms):

        dt_with_tz = datetime.fromtimestamp(epoch_ms / 1000)
        dt_naive = dt_with_tz.replace(tzinfo=None)
        return dt_naive

        
        
    





