from app.utils.logger import get_logger
from app.brokers.broker_factory import BrokerFactory


class TradeExecutor:

    def __init__(self, broker_name: str, account_name: str):
        self.broker_name = broker_name
        self.account_name = account_name
        self.logger = get_logger(main_folder_name='TradeExecutor', broker_name=self.broker_name, account_name=self.account_name)
        self.broker = None
    
    async def initialize(self):
        try:
            self.broker = BrokerFactory.get_broker(broker_name=self.broker_name, account_name=self.account_name)
            await self.broker.initialize()
            self.logger.info(f'TradeExecutor initialized for broker: {self.broker_name}, account: {self.account_name}')
        except Exception as e:
            error_msg = f"Failed to initialize TradeExecutor for broker: {self.broker_name}, account: {self.account_name}, Error: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    async def place_order(self,
                          broker_name: str,
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
        Places an order with any broker.

        Args:
            broker_name (str): The broker name.
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
            Exception: If order placement fails.
        """
        try:
            order_response = await self.broker.place_order(trading_symbol=trading_symbol,
                                                            transaction_type=transaction_type,
                                                            quantity=quantity,
                                                            product=product,
                                                            exchange=exchange,
                                                            instrument_type=instrument_type,
                                                            order_type=order_type,
                                                            price=price,
                                                            trigger_price=trigger_price,
                                                            validity=validity,
                                                            disclosed_quantity=disclosed_quantity,
                                                            is_amo=is_amo,
                                                            tag=tag)
            return order_response
        
        except Exception as e:
            error_msg = f"Exception occured during order placement: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        
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
        """
        try:
            order_response = await self.broker.modify_order(order_id=order_id,
                                                            quantity=quantity,
                                                            price=price,
                                                            order_type=order_type,
                                                            trigger_price=trigger_price,
                                                            validity=validity,
                                                            disclosed_quantity=disclosed_quantity)
            return order_response
        
        except Exception as e:
            error_msg = f"Exception occured during order modification: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    async def cancel_order(self, order_id: str) -> dict:
        """
        Cancels an existing order.

        Args:
            order_id (str): Order ID to cancel.

        Returns:
            dict: Order response data.

        Raises:
            Exception: If order cancellation fails.
        """
        try:
            order_response = await self.broker.cancel_order(order_id=order_id)
            return order_response
        
        except Exception as e:
            error_msg = f"Order cancellation failed: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        
    async def get_order_details(self, order_id: str) -> dict:
        """
        Retrieves order details for a specific order ID.

        Args:
            order_id (str): The order ID.

        Returns:
            dict: Order details.

        Raises:
            Exception: If unable to get order details.
        """
        try:
            order_details = await self.broker.get_order_details(order_id=order_id)
            return order_details
        
        except Exception as e:
            error_msg = f"Exception occurred while getting order details: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

