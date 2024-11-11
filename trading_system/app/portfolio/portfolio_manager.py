from app.utils.logger import get_logger
from app.brokers.broker_factory import BrokerFactory


class PortfolioManager:

    def __init__(self, broker_name: str, account_name: str):
        self.broker_name = broker_name
        self.account_name = account_name
        self.logger = get_logger(main_folder_name='PortfolioManager', broker_name=self.broker_name, account_name=self.account_name)
        self.broker = None
    
    async def initialize(self): 
        try:
            self.broker = BrokerFactory.get_broker(broker_name=self.broker_name, account_name=self.account_name)
            await self.broker.initialize()
            self.logger.info(f'PortfolioManager initialized for broker: {self.broker_name}, account: {self.account_name}')
        except Exception as e:
            error_msg = f"Failed to initialize PortfolioManager for broker: {self.broker_name}, account: {self.account_name}, Error: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)         
    
    async def get_orderbook(self) -> list:
        """
        Retrieves the current order book.

        Returns:
            list: List of orders.

        Raises:
            Exception: If unable to get order book.
        """
        try:
            orderbook_data = await self.broker.get_orderbook()
            return orderbook_data

        except Exception as e:
            error_msg = f"Exception occurred while getting order book: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        
    async def get_current_positions(self) -> list:
        """
        Retrieves the current positions.

        Returns:
            list: List of current positions.

        Raises:
            Exception: If unable to get current positions.
        """
        try:
            current_positions = await self.broker.get_current_positions()
            return current_positions

        except Exception as e:
            error_msg = f"Exception occurred while getting current positions: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        