from app.brokers.zerodha_broker import ZerodhaBroker
from app.brokers.upstox_broker import UpstoxBroker
from app.config import SUPPORTED_BROKERS
from app.utils.logger import get_logger


class BrokerFactory:
    """A factory class to create broker instances based on the broker name."""

    @staticmethod
    def get_broker(broker_name: str, account_name: str):
        broker_name = broker_name.title()
        logger = get_logger(main_folder_name='Brokers/BrokerFactory')

        if broker_name not in SUPPORTED_BROKERS:
            logger.error(f"Broker '{broker_name}' is not supported.")
            raise ValueError(f"Broker '{broker_name}' is not supported.")

        if broker_name == 'Zerodha':
            return ZerodhaBroker(account_name)
        elif broker_name == 'Upstox':
            return UpstoxBroker(account_name)
        else:
            logger.error(f"No implementation for broker '{broker_name}'.")
            raise NotImplementedError(f"No implementation for broker '{broker_name}'.")