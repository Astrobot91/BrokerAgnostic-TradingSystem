from abc import ABC, abstractmethod


class BaseStrategy(ABC):
    """
    Abstract base class for trading strategies.
    All strategies should inherit from this class and implement the abstract methods.
    """

    def __init__(self, account_name: str, broker_name: str):
        """
        Initialize the strategy with a name and broker instance.

        :param broker_name: Broker name (Upstox, Zerodha, etc).
        :param account_name: Account name linked to the broker.
        """
        self.broker_name = broker_name
        self.account_name = account_name

    @abstractmethod
    async def initialize(self):
        """
        Method to perform any initialization tasks required by the strategy.
        """
        pass

    @abstractmethod
    async def execute(self):
        """
        Core logic of the strategy. This method will be called to perform trading operations.
        """
        pass
