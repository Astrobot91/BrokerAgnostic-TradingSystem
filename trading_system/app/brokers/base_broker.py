import polars as pl
from app.utils.logger import get_logger
from typing import Dict, Any, List
from abc import ABC, abstractmethod


class BaseBroker(ABC):
    """
    BaseBroker: Abstract base class for all brokers.
    Shared functionality and common interface for broker implementations.
    """
    def __init__(self, account_name: str) -> None:
        self.account_name = account_name
        self.logger = get_logger()

    @abstractmethod
    async def initialize(self):
        """Initialize the broker-specific configuration or connection."""
        pass

    @abstractmethod
    async def place_order(self, **kwargs) -> Dict[str, Any]:
        """Place an order."""
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an existing order."""
        pass

    @abstractmethod
    async def modify_order(self, **kwargs) -> Dict[str, Any]:
        """Modify an existing order."""
        pass

    @abstractmethod
    async def get_order_details(self, order_id: str) -> Dict[str, Any]:
        """Get details of an order."""
        pass

    @abstractmethod
    async def get_orderbook(self) -> List[Dict[str, Any]]:
        """Get orderbook"""
        pass

    @abstractmethod
    async def historical_data(self, **kwargs) -> pl.DataFrame:
        """Fetch historical data."""
        pass

    @abstractmethod
    async def get_funds_and_margin(self, segment: str = None) -> float:
        """Fetch available balance in the account"""
        pass

    @abstractmethod
    async def ltp_quote(self, exchange_token: str) -> float:
        """Fetch LTP quote."""
        pass

    @abstractmethod
    async def ohlc_quote(self, exchange_token: str, interval: str) -> dict:
        """Fetch OHLC quote."""
        pass

    @abstractmethod
    async def full_market_quote(self, exchange_token: str) -> dict:
        """Fetch full market quote."""
        pass
    
    @abstractmethod
    async def calculate_margin(self, instrument_dict: Dict[str, Any]) -> float:
        """Fetch required margin for the trade"""
        pass

    @abstractmethod
    async def calculate_brokerage(self, instrument_dict: Dict[str, Any]) -> float:
        """Fetch required brokerage for the trade"""
        pass

    @abstractmethod
    async def market_holidays(self) -> pl.DataFrame:
        """Fetch market holidays"""
        pass
