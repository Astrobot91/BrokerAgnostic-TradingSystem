from abc import ABC, abstractmethod


class BaseStrategy(ABC):
    """
    Abstract base class for trading strategies.
    All strategies should inherit from this class and implement the abstract methods.
    """

    def __init__(self, strategy_name: str, broker, **kwargs):
        """
        Initialize the strategy with a name and broker instance.

        :param strategy_name: Name of the strategy.
        :param broker: Broker instance to execute trades.
        :param kwargs: Additional parameters specific to the strategy.
        """
        self.strategy_name = strategy_name
        self.broker = broker
        self.parameters = kwargs

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

    @abstractmethod
    async def shutdown(self):
        """
        Method to clean up resources when the strategy is stopped.
        """
        pass
