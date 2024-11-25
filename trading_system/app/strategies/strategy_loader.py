import importlib


class StrategyLoader:
    def __init__(self):
        self.strategies = {}

    def load_strategy(self, strategy_name: str):
        try:
            module = importlib.import_module(f"app.strategies.{strategy_name}")
            strategy_class = getattr(module, strategy_name)
            strategy_instance = strategy_class(strategy_name)
            self.strategies[strategy_name] = strategy_instance
            return strategy_instance
        except (ModuleNotFoundError, AttributeError) as e:
            raise e

    def unload_strategy(self, strategy_name: str):
        strategy = self.strategies.pop(strategy_name, None)
        if strategy:
            strategy.logger.info(f"Strategy '{strategy_name}' unloaded.")
            return True
        else:
            return False

    def get_strategy(self, strategy_name: str):
        return self.strategies.get(strategy_name)
