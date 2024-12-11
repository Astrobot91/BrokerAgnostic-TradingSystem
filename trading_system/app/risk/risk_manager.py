from app.brokers.base_broker import BaseBroker


class RiskManager:

    def __init__(self, broker: BaseBroker):
        self.broker = broker
        self.logger = self.broker.logger
    
    async def validate_order(self,
                             exchange_token: str,
                             quantity: int,
                             product: str,
                             transaction_type: str,
                             trade_type: str,
                             price: float = 0) -> tuple[bool, float, float]:

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
                available_margin = await self.broker.get_funds_and_margin()

                if trade_type.lower() == 'entry': 
                    if available_margin > (margin_required + brokerage_required):
                        self.logger.info(f'Order: {instrument_dict} validated successfully.')
                        return True, margin_required, brokerage_required
                    else:
                        self.logger.warning(f'Order: {instrument_dict} validation failed.')
                        return False, margin_required, brokerage_required
                elif trade_type.lower() == 'exit':
                    if available_margin > brokerage_required:
                        self.logger.info(f'Order: {instrument_dict} validated successfully.')
                        return True, margin_required, brokerage_required
                    else:
                        self.logger.warning(f'Order: {instrument_dict} validation failed.')
                        return False, margin_required, brokerage_required
                else:
                    self.logger.error(f'Order Validation failed due to incorrect trade_type: {trade_type}')

            except Exception as e:
                error_msg = f"Error occured while validating order: {e}"
                self.logger.error(error_msg)
                raise Exception(error_msg)

