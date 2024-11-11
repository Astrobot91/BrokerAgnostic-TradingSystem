from abc import ABC, abstractmethod

class BaseBroker(ABC):
    """
    Abstract base class for all broker implementations.

    This class defines the interface that all broker classes must implement.
    It includes abstract methods for initializing the broker, placing orders,
    modifying orders, canceling orders, fetching order details, retrieving
    order books, getting current positions, and obtaining market data.

    Each broker-specific class should inherit from this base class and provide
    concrete implementations of all abstract methods.
    """

    @abstractmethod
    async def initialize(self):
        """
        Initialize the broker asynchronously.

        This method should set up any necessary configurations, authenticate with
        the broker's API, and prepare the broker instance for trading operations.

        Raises:
            Exception: If initialization fails.
        """
        pass

    @abstractmethod
    async def place_order(self,
                          trading_symbol: str,
                          transaction_type: str,
                          quantity: int,
                          product: str,
                          order_type: str = 'MARKET',
                          exchange: str = None,
                          instrument_type: str = 'EQ',
                          price: float = None,
                          trigger_price: float = None,
                          validity: str = None,
                          disclosed_quantity: int = None,       # Required for UPSTOX
                          is_amo: bool = False,                 # Required for UPSTOX
                          tag: str = None,
                          variety: str = 'regular') -> dict:    # Required for ZERODHA
        """
        Place an order with the broker.

        Args:
            trading_symbol (str): The trading symbol of the instrument.
            transaction_type (str): 'BUY' or 'SELL'.
            quantity (int): The quantity to trade.
            product (str): Product code ('I' for Intraday, 'D' for Delivery, etc.).
            order_type (str, optional): Order type ('MARKET', 'LIMIT', 'SL', 'SL-M'). Defaults to 'MARKET'.
            exchange (str, optional): Exchange code ('NSE', 'BSE', etc.). Defaults to None.
            instrument_type (str, optional): Instrument type ('EQ', 'FO'). Defaults to 'EQ'.
            price (float, optional): Price for LIMIT orders. Required if order_type is 'LIMIT'. Defaults to None.
            trigger_price (float, optional): Trigger price for SL orders. Defaults to None.
            validity (str, optional): Validity of the order ('DAY', 'IOC'). Defaults to None.
            disclosed_quantity (int, optional): Disclosed quantity (specific to certain brokers). Defaults to None.
            is_amo (bool, optional): After Market Order flag (specific to certain brokers). Defaults to False.
            tag (str, optional): Order tag or custom identifier. Defaults to None.
            variety (str, optional): Order variety (specific to certain brokers, e.g., 'regular', 'bo', 'co'). Defaults to 'regular'.

        Returns:
            dict: Order confirmation details.

        Raises:
            Exception: If order placement fails.
        """
        pass

    @abstractmethod
    async def modify_order(self,
                           order_id: str,
                           quantity: int = None,
                           price: float = None,             # Required for UPSTOX
                           order_type: str = None,          # Required for UPSTOX
                           trigger_price: float = None,     # Required for UPSTOX
                           validity: str = None,            # Required for UPSTOX
                           disclosed_quantity: int = None,
                           variety: str = 'regular') -> dict:   # Required for ZERODHA
        """
        Modify an existing order.

        Args:
            order_id (str): Order ID of the placed order.
            quantity (int, optional): New quantity for the order. Defaults to None.
            price (float, optional): New price for LIMIT orders. Defaults to None.
            order_type (str, optional): New order type ('MARKET', 'LIMIT', etc.). Defaults to None.
            trigger_price (float, optional): New trigger price for SL orders. Defaults to None.
            validity (str, optional): New validity ('DAY', 'IOC'). Defaults to None.
            disclosed_quantity (int, optional): New disclosed quantity. Defaults to None.
            variety (str, optional): Order variety (specific to certain brokers). Defaults to 'regular'.

        Returns:
            dict: Modified order details.

        Raises:
            Exception: If order modification fails.
        """
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str, variety: str = 'regular') -> dict:
        """
        Cancel an existing order.

        Args:
            order_id (str): Order ID to cancel.
            variety (str, optional): Order variety (specific to certain brokers). Defaults to 'regular'.

        Returns:
            dict: Order cancellation confirmation.

        Raises:
            Exception: If order cancellation fails.
        """
        pass

    @abstractmethod
    async def get_order_details(self, order_id: str) -> dict:
        """
        Get details of a specific order.

        Args:
            order_id (str): The order ID.

        Returns:
            dict: Order details.

        Raises:
            Exception: If unable to retrieve order details.
        """
        pass

    @abstractmethod
    async def get_orderbook(self) -> dict:
        """
        Retrieve the current order book.

        Returns:
            dict: A dictionary containing all orders.

        Raises:
            Exception: If unable to retrieve the order book.
        """
        pass

    @abstractmethod
    async def get_current_positions(self) -> dict:
        """
        Retrieve current positions held with the broker.

        Returns:
            dict: Current positions data.

        Raises:
            Exception: If unable to retrieve positions.
        """
        pass

    @abstractmethod
    async def ltp_quote(self, trading_symbols: list) -> dict:
        """
        Get the Last Traded Price (LTP) for a list of trading symbols.

        Args:
            trading_symbols (list): List of trading symbols.

        Returns:
            dict: A dictionary mapping trading symbols to their LTP.

        Raises:
            Exception: If unable to retrieve LTP data.
        """
        pass

    @abstractmethod
    async def ohlc_quote(self, trading_symbols: list) -> dict:
        """
        Get Open-High-Low-Close (OHLC) data for a list of trading symbols.

        Args:
            trading_symbols (list): List of trading symbols.

        Returns:
            dict: A dictionary mapping trading symbols to their OHLC data.

        Raises:
            Exception: If unable to retrieve OHLC data.
        """
        pass

    @abstractmethod
    async def full_market_quote(self, trading_symbols: list) -> dict:
        """
        Get full market quote including limit order depth for a list of trading symbols.

        Args:
            trading_symbols (list): List of trading symbols.

        Returns:
            dict: A dictionary containing full market data.

        Raises:
            Exception: If unable to retrieve market data.
        """
        pass

    @abstractmethod
    async def convert_quote(self, data: dict) -> dict:
        """
        Convert the market quote responses into a standardized format.

        Args:
            data (dict): Raw market data.

        Returns:
            dict: Standardized market data.

        Raises:
            Exception: If conversion fails.
        """
        pass

    @abstractmethod
    async def historical_data(self,
                              trading_symbol: str,
                              exchange: str,
                              instrument_type: str,
                              interval: str,
                              from_date: str,
                              to_date: str) -> dict:
        """
        Retrieve historical data for a specific trading symbol.

        Args:
            trading_symbol (str): The trading symbol.
            exchange (str): Exchange code (e.g., 'NSE', 'BSE').
            instrument_type (str): Instrument type (e.g., 'EQ', 'FO').
            interval (str): Data interval ('1minute', '5minute', '1day', etc.).
            from_date (str): Start date in 'YYYY-MM-DD' format.
            to_date (str): End date in 'YYYY-MM-DD' format.

        Returns:
            dict: Historical data.

        Raises:
            Exception: If unable to retrieve historical data.
        """
        pass
