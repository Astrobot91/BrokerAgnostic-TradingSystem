from app.utils.logger import get_logger
from app.brokers.broker_factory import BrokerFactory


class RiskManager:

    def __init__(self,
                 broker_name: str,
                 account_name: str,
                 strategy_name: str):

        self.broker_name = broker_name
        self.account_name = account_name
        self.strategy_name = strategy_name
        self.logger = get_logger(main_folder_name='RiskManager', broker_name=self.broker_name, account_name=self.account_name)

    async def initialize(self):
        try:
            self.broker = BrokerFactory.get_broker(broker_name=self.broker_name, account_name=self.account_name)
            await self.broker.initialize()
            self.logger.info(f'RiskManager initialized for broker: {self.broker_name}, account: {self.account_name}')
        except Exception as e:
            error_msg = f'Failed to initialize RiskManager for broker: {self.broker_name}, account: {self.account_name}, Error: {e}'
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    async def validate_order(self,
                             exchange_token: str,
                             quantity: int,
                             product: str,
                             transaction_type: str,
                             price: float = 0) -> bool:
        instrument_dict = {
            'exchange_token': exchange_token,
            'quantity': quantity,
            'product': product,
            'transaction_type': transaction_type,
            'price': price
        }
        try:
            margin_required = await self.broker.calculate_margin(instrument_dict=instrument_dict)
            brokerage_required = await self.broker.calculate_brokerage(instrument_dict=instrument_dict)
            funds_and_margin = await self.broker.get_funds_and_margin()
            available_margin = funds_and_margin
            if available_margin > (margin_required + brokerage_required):
                self.logger.info(f'Order: {instrument_dict} validated successfully.')
                return True
            else:
                self.logger.warning(f'Order: {instrument_dict} validation failed.')
                return False
        except Exception as e:
            error_msg = f"Error occured while validating order: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)