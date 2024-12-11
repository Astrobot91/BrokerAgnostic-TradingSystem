import time
import asyncio
import polars as pl
from zoneinfo import ZoneInfo
from datetime import datetime, date, timedelta
from app.utils.logger import get_logger
from app.utils.telegram import broadcast_signal
from app.risk.risk_manager import RiskManager
from app.brokers.broker_factory import BrokerFactory
from app.strategies.strategy_base import BaseStrategy
from app.execution.trade_executor import TradeExecutor
from app.portfolio.portfolio_manager import PortfolioManager
from app.data.datafeed import BrokerDataFeed, FundamentalDataFeed
from app.config import STRATEGY_CONFIGS


ist = ZoneInfo('Asia/Kolkata')
POSITION_ID = int(time.time())
MARKET_OPENING_TIME = datetime.today().replace(hour=3, minute=45, second=1, microsecond=0).astimezone(ist)
SIGNAL_DATE = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

class MomentumStrategyV1(BaseStrategy):
    """
    Momentum Strategy Version 1:
    Identifies stocks that have hit their 52-week high or low and generates trading signals based on these events.
    """
    def __init__(self,
                 executor_broker_name: str,
                 executor_account_name: str,
                 ):
        self.strategy_name = 'MomentumStrategyV1'
        self.datafeed_broker_name = 'Upstox'
        self.datafeed_account_name = '2HCB67'
        self.executor_broker_name = executor_broker_name
        self.executor_account_name = executor_account_name
        self.logger = get_logger(
            main_folder_name=f'Brokers/{self.executor_broker_name}/Accounts/{self.executor_account_name}/Strategies/{self.strategy_name}',
            broker_name=self.executor_broker_name,
            account_name=self.executor_account_name
        )
        self.strategy_config = STRATEGY_CONFIGS.get(self.strategy_name)
        self.isin_df = None
        self.brokerdatafeed = None
        self.fundamental_datafeed = None
        self.trade_executor = None
        self.portfolio_manager = None
        self.risk_manager = None
        self.instrument_df = None

    async def initialize(self) -> None:
        try:
            self.datafeed_broker = BrokerFactory.get_broker(broker_name=self.datafeed_broker_name,
                                                            account_name=self.datafeed_account_name,
                                                            logger=self.logger)
            self.executor_broker = BrokerFactory.get_broker(broker_name=self.executor_broker_name,
                                                            account_name=self.executor_account_name,
                                                            logger=self.logger)
            await self.datafeed_broker.initialize()
            await self.executor_broker.initialize()

            self.fundamental_datafeed = FundamentalDataFeed(broker=self.datafeed_broker)
            self.brokerdatafeed = BrokerDataFeed(broker=self.datafeed_broker)
            self.trade_executor = TradeExecutor(broker=self.executor_broker,
                                                broker_name=self.executor_broker_name,
                                                account_name=self.executor_account_name,
                                                strategy_name=self.strategy_name)
            self.portfolio_manager = PortfolioManager(broker=self.executor_broker,
                                                      broker_name=self.executor_broker_name,
                                                      account_name=self.executor_account_name,
                                                      strategy_name=self.strategy_name)
            self.risk_manager = RiskManager(broker=self.executor_broker)
            self.instrument_df = self.brokerdatafeed.broker.instrument_df
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
            await self.trade_executor.remove_access_token()

            if self.is_evening():
                await self._check_exits()        
                open_positions = await self.portfolio_manager.load_positions(status='open')
                no_of_open_positions = len(open_positions)
                qualified_futures, nearest_expiry = await self._get_recent_expiry_futures()
                
                if qualified_futures.is_empty():
                    self.logger.info("No qualified futures contracts to trade.")
                    return
                self.logger.info(f"Qualified futures contracts: {qualified_futures}")
                
                exit_date = await self._get_exit_date(entry_date=SIGNAL_DATE + timedelta(days=1))
                if exit_date.date() <= nearest_expiry.date():
                    await self._save_new_positions(qualified_futures, no_of_open_positions)
                else:
                    self.logger.info(f'Cannot save new positions as expiry is under holding period.')

            if self.is_morning():
                while True:
                    if datetime.now(ist) > MARKET_OPENING_TIME:
                        await self._execute_trades()
                        break
                    
        except Exception as e:
            self.logger.error(f"Error during strategy execution: {e}")


    async def generate_signals(self) -> pl.DataFrame:
        """
        Generates trading signals based on 52-week high/low criteria.
        """
        try:
            futures_stocks_df = await self._futures_stocks()

            if futures_stocks_df.is_empty():
                self.logger.info("No futures trading symbols found.")
                return None

            to_date = date.today()
            from_date = to_date - timedelta(days=1000)
            
            column_names = futures_stocks_df.columns

            stock_df_store = []
            for row in futures_stocks_df.rows():
                row_dict = dict(zip(column_names, row))
                trading_symbol = row_dict['trading_symbol']
                isin_code = row_dict['isin']
                exchange_token = row_dict['exchange_token']

                if not isin_code:
                    self.logger.warning(f"ISIN code not found for symbol: {trading_symbol}")
                    continue

                mcap = self.fundamental_datafeed.marketCapData(isin_code=isin_code)
                hist_stock_df = await self.brokerdatafeed.historical_data(
                    exchange_token=exchange_token,
                    interval='day',
                    from_date=str(from_date),
                    to_date=str(to_date)
                )
                await asyncio.sleep(0.25)

                if hist_stock_df.is_empty():
                    self.logger.warning(f"No historical data for symbol: {trading_symbol}")
                    continue
    
                hist_stock_df = self._process_historical_data(hist_stock_df, trading_symbol, exchange_token, mcap)
                stock_df_store.append(hist_stock_df)

            if not stock_df_store:
                self.logger.info("No historical data available to generate signals.")
                return None

            stock_data = pl.concat(stock_df_store)
            stock_data = stock_data.filter(
                (pl.col('datetime').dt.date() == SIGNAL_DATE) &
                (
                    (pl.col('final_high_signal') == 1) |
                    (pl.col('final_low_signal') == 1)
                )
            )
            if stock_data.is_empty():
                self.logger.info('No signals generated today.')
                return pl.DataFrame(data={}, strict=False)
            stock_data = stock_data.sort(self.strategy_config.get('SORT_BY'))
            stock_data = stock_data.head(self.strategy_config.get('PORTFOLIO_SIZE'))
            return stock_data
        except Exception as e:
            self.logger.error(f"Error generating signals: {e}")
            raise Exception(f"Error generating signals: {e}")

    async def _get_recent_expiry_futures(self) -> tuple[pl.DataFrame, datetime]:
        """
        Retrieves futures contracts that are nearest to expiry and meet the holding period criteria.
        """
        try:
            signals = await self.generate_signals()
            if signals.is_empty():
                return (pl.DataFrame(data={}, strict=False), None)
            
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
            futures_slice = master_file_slice.filter(pl.col('expiry') == nearest_expiry)

            message_details = signals.join(
                            futures_slice,
                            left_on="trading_symbol",
                            right_on="asset_symbol",
                            how="inner"
                            ).drop(["trading_symbol", "exchange_token_right"]).rename({
                            "trading_symbol_right": "trading_symbol"
                            })
                                
            if not message_details.is_empty():
                self.logger.info(f'Futures contracts generated today: {futures_slice}')
                telegram_msg = f'Signals for : {(SIGNAL_DATE + timedelta(days=1)).replace(hour=9, minute=15, second=0)}\n'
                i = 1
                column_names = message_details.columns
                for row in message_details.rows():
                    row_dict = dict(zip(column_names, row))
                    direction = (
                        'Fresh 52W High' if row_dict['final_high_signal'] == 1 
                        else 'Fresh 52W Low' if row_dict['final_low_signal'] == 1 
                        else None
                    )
                    telegram_msg += f"\n{i}. {row_dict['underlying_symbol']}\n - ContractName: {row_dict['trading_symbol']}\n - Direction: {direction}\n - LotSize: {row_dict['lot_size']}\n - MarketCap: {row_dict['marketcap']}\n"
                    i += 1
                telegram_msg += f'\nSignal Provider details:\nBroker: {self.datafeed_broker_name}\nAccount: {self.datafeed_account_name}'
                broadcast_signal(signal=telegram_msg)
                return futures_slice, nearest_expiry
            else:
                self.logger.info('No futures contracts found for the nearest expiry.')
            return None
        except Exception as e:
            self.logger.exception(f"Error fetching recent expiry futures: {e}")
            return None

    async def _save_new_positions(self,
                                  qualified_instruments: pl.DataFrame,
                                  no_of_open_positions: int) -> None:
        """
        Saves new positions.
        """
        space_left_in_portfolio = self.strategy_config.get('PORTFOLIO_SIZE') - no_of_open_positions
        available_margin = ...
        # CHECK FOR TOMMORROWS EXITS, IF ANY STOCKS ARE EXITING TOMMORROW, RECORD THEIR MARGIN
        # BUT THIS HAS A FLAW, WHAT IF THE CURRENT POSITION SUFFERS FROM GAPDOWN TOMMORROW? 
        
        available_balance = await self.trade_executor.get_funds_and_margin()
        self.logger.info(f"Space left in the portfolio: {space_left_in_portfolio}")

        try:
            counter = 0
            total_charges = 0
            columns = qualified_instruments.columns
            for row in qualified_instruments.rows():
                if counter >= space_left_in_portfolio:
                    self.logger.info(f"No space left in the portfolio for the signals.")
                    break
                else:
                    row_dict = dict(zip(columns, row))

                    trading_symbol = row_dict['trading_symbol']
                    exchange_token = row_dict['exchange_token']
                    exchange = row_dict['exchange']
                    order_type = 'MARKET'
                    transaction_type = 'BUY'
                    product = 'D'
                    validity = 'DAY'
                    quantity = row_dict['lot_size']
                    entry_timestamp = SIGNAL_DATE + timedelta(days=1)
                    exit_timestamp = await self._get_exit_date(entry_date=SIGNAL_DATE + timedelta(days=1))
                    status = 'execute'
                    tag = f'{self.strategy_name}_{exchange_token}_{transaction_type}_{quantity}_{POSITION_ID}_ENTRY'

                    margin = await self.trade_executor.calculate_margin(exchange_token=exchange_token,
                                                                        quantity=quantity,
                                                                        product=product,
                                                                        transaction_type=transaction_type)
                    brokerage = await self.trade_executor.calculate_brokerage(exchange_token=exchange_token,
                                                                              quantity=quantity,
                                                                              product=product,
                                                                              transaction_type=transaction_type)
                    charges_per_lot = margin + brokerage
                    total_charges += charges_per_lot
                    if available_balance > total_charges:
                        await self.portfolio_manager.save_position(trading_symbol=trading_symbol,
                                                                exchange_token=exchange_token,
                                                                exchange=exchange,
                                                                order_type=order_type,
                                                                entry_transaction_type=transaction_type,
                                                                product=product,
                                                                validity=validity,
                                                                quantity=quantity,
                                                                entry_timestamp=str(entry_timestamp),
                                                                exit_timestamp=str(exit_timestamp),
                                                                entry_margin=margin,
                                                                entry_brokerage=brokerage,
                                                                status=status,
                                                                tag=tag)
                    else:
                        self.logger.error(f'Could not save order: {tag} due to insufficient balance.')
                counter += 1
        except Exception as e:
            self.logger.exception(f"Error saving new positions: {e}")

    async def _execute_trades(self) -> None:
        """
        Executes trades based on the qualified futures contracts.
        """
        try:
            execute_positions = await self.portfolio_manager.load_positions(status='execute')
            if execute_positions:
                for position in execute_positions:
                    if position.get('tag').endswith('EXIT'):
                        status = 'closed'
                        order_is_valid, exit_margin, exit_brokerage = await self.risk_manager.validate_order(exchange_token=position['exchange_token'],
                                                                                                            transaction_type=position['exit_transaction_type'],
                                                                                                            quantity=position['quantity'],
                                                                                                            trade_type='exit',
                                                                                                            product=position['product'])
                        if order_is_valid:
                            order_response = await self.trade_executor.place_order(exchange_token=position['exchange_token'],
                                                                                transaction_type=position['exit_transaction_type'],
                                                                                quantity=position['quantity'],
                                                                                product=position['product'],
                                                                                validity=position['validity'],
                                                                                order_type=position['order_type'],
                                                                                tag=position['tag'])
                            
                            order_details = await self.trade_executor.get_order_details(order_id=order_response['order_id'])
                            await self.trade_executor.save_order(order_details=order_details)                 
                            
                            exit_asset_price = position['quantity'] * order_details['average_price']
                            pnl = exit_asset_price - (position['entry_asset_price'] + exit_brokerage + position['entry_brokerage'])
                            await self.portfolio_manager.update_position(tag=position['tag'],
                                                                         updates={
                                                                             "exit_order_id": order_details['order_id'],
                                                                             "exit_price": order_details['average_price'],
                                                                             "exit_timestamp": order_details['order_timestamp'],
                                                                             "status": status,
                                                                             "exit_disclosed_quantity": order_details['disclosed_quantity'],
                                                                             "exit_filled_quantity": order_details['filled_quantity'],
                                                                             "exit_pending_quantity": order_details['pending_quantity'],
                                                                             "exit_asset_price": exit_asset_price,
                                                                             "exit_margin": exit_margin,
                                                                             "exit_brokerage": exit_brokerage,
                                                                             "pnl": pnl
                                                                            }
                                                                        )

                for position in execute_positions:
                    if position.get('tag').endswith('ENTRY'):
                        status = 'open'
                        order_is_valid, entry_margin, entry_brokerage = await self.risk_manager.validate_order(exchange_token=position['exchange_token'],
                                                                                                            transaction_type=position['entry_transaction_type'],
                                                                                                            quantity=position['quantity'],
                                                                                                            trade_type='entry',
                                                                                                            product=position['product'])
                        if order_is_valid:
                            order_response = await self.trade_executor.place_order(exchange_token=position['exchange_token'],
                                                                                transaction_type=position['entry_transaction_type'],
                                                                                quantity=position['quantity'],
                                                                                product=position['product'],
                                                                                validity=position['validity'],
                                                                                order_type=position['order_type'],
                                                                                tag=position['tag'])
                            
                            order_details = await self.trade_executor.get_order_details(order_id=order_response['order_id'])
                            await self.trade_executor.save_order(order_details=order_details)             
                            
                            entry_asset_price = order_details['average_price'] * position['quantity']
                            await self.portfolio_manager.update_position(tag=position['tag'],
                                                                         updates={
                                                                             "entry_order_id": order_details['order_id'],
                                                                             "entry_price": order_details['average_price'],
                                                                             "entry_timestamp": order_details['order_timestamp'],
                                                                             "status": status,
                                                                             "entry_disclosed_quantity": order_details['disclosed_quantity'],
                                                                             "entry_filled_quantity": order_details['filled_quantity'],
                                                                             "entry_pending_quantity": order_details['pending_quantity'],
                                                                             "entry_asset_price": entry_asset_price,
                                                                             "entry_margin": entry_margin,
                                                                             "entry_brokerage": entry_brokerage,
                                                                            }
                                                                        )
                        else:
                            self.logger.warning(f'Order Validation failed for order: {position}')
            else:
                self.logger.info(f"There are no orders to execute today.")
        except Exception as e:
            self.logger.exception(f"Error executing trades: {e}")

    async def _check_exits(self) -> list:
        try:
            open_positions = await self.portfolio_manager.load_positions(status='open')
            if open_positions:
                for position_dict in open_positions:
                    transaction_type = 'SELL'
                    status = 'execute'
                    exit_date = datetime.strptime(position_dict['exit_timestamp'], "%Y-%m-%d %H:%M:%S")
                    sl = position_dict['exit_price'] - ((position_dict['exit_price']) * (self.strategy_config.get('STOP_LOSS')))
                    ohlc_quote = await self.brokerdatafeed.ohlc_quote(exchange_token=position_dict['exchange_token'], interval='1d')
                    low = ohlc_quote['low']
                    
                    total_brokerage = 0
                    exit_brokerage = await self.trade_executor.calculate_brokerage(exchange_token=position_dict['exchange_token'],
                                                                                   quantity=position_dict['quantity'],
                                                                                   product=position_dict['product'],
                                                                                   transaction_type=transaction_type)

                    available_balance = await self.trade_executor.get_funds_and_margin()

                    if (datetime.today() >= exit_date) or ((low <= sl)):
                        tag = position_dict['tag']
                        if (datetime.today() >= exit_date):
                            tag = tag.replace('ENTRY', 'HOLD_EXIT')
                        elif ((low <= sl)):
                            tag = tag.replace('ENTRY', 'SL_EXIT')
                        
                        total_brokerage += exit_brokerage
                        if available_balance > total_brokerage:
                            await self.portfolio_manager.update_position(tag=position_dict['tag'],
                                                                        updates={
                                                                            'status': status,
                                                                            'exit_transaction_type': transaction_type,
                                                                            'tag': tag
                                                                            }
                                                                        )
                        else:
                            self.logger.error(f'Not enough balance for brokerage to execute the trade.')
                    else:
                        self.logger.info(f'Position: {position_dict} did not meet exit condition, skipping...')
            else:
                self.logger.warning(f'There are no postions open currently.')

        except Exception as e:
            error_msg = f'Error occured while processing exits: {e}'
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def _futures_stocks(self) -> pl.DataFrame:
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
            return merged_df
        except Exception as e:
            self.logger.exception(f"Error retrieving futures stocks: {e}")
            return pl.DataFrame()

    async def _get_exit_date(self, entry_date: datetime) -> datetime:
        
        market_holidays = await self.brokerdatafeed.market_holidays()
        holiday_dates = list(market_holidays['date'])
        
        trading_days = []
        current_date = entry_date
        while len(trading_days) < self.strategy_config.get('HOLDING_PERIOD'):
            if (current_date.weekday() < 5) and (current_date not in holiday_dates):
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        return max(trading_days)
    
    def _process_historical_data(self, hist_stock_df: pl.DataFrame, trading_symbol: str, exchange_token: str, mcap: float) -> pl.DataFrame:
        """
        Processes historical stock data by adding necessary columns and calculating signals.
        """
        try:
            hist_stock_df = hist_stock_df.with_columns(
                pl.col("datetime").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("datetime"),
                pl.lit(trading_symbol).alias('trading_symbol'),
                pl.lit(exchange_token).alias('exchange_token'),
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
            return pl.DataFrame(data={}, strict=False)

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

    def is_morning(self) -> bool:
        """
        Checks if the current time falls within the morning time window (8:00 AM to 10:00 AM IST).

        Returns:
            bool: True if the current time is between 8:00 AM and 10:00 AM IST, False otherwise.
        """
        ist = ZoneInfo('Asia/Kolkata')
        current_time = datetime.now(ist)
        morning_start_time = current_time.replace(hour=8, minute=0, second=0, microsecond=0)
        morning_end_time = current_time.replace(hour=10, minute=0, second=0, microsecond=0)

        return morning_start_time <= current_time < morning_end_time

    def is_evening(self) -> bool:
        """
        Checks if the current time falls within the morning time window (16:00 PM to 19:00 PM IST).

        Returns:
            bool: True if the current time is between 16:00 PM and 19:00 PM IST, False otherwise.
        """
        ist = ZoneInfo('Asia/Kolkata')
        current_time = datetime.now(ist)
        evening_start_time = current_time.replace(hour=16, minute=0, second=0, microsecond=0)
        evening_end_time = current_time.replace(hour=18, minute=0, second=0, microsecond=0)

        return evening_start_time <= current_time < evening_end_time