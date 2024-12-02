import os
import json
from app.brokers.base_broker import BaseBroker
from typing import Dict, Any


ORDERS_FILE = 'orders.json'

class TradeExecutor:
    """
    A class for executing and managing trades across multiple brokers. 
    TradeExecutor provides a unified interface for placing, modifying, and canceling orders 
    as well as retrieving order details for various brokers.
    """

    def __init__(self, broker: BaseBroker):
        """
        Initializes the TradeExecutor with specified broker and account details.

        Parameters:
            broker (BaseBroker): The object of the broker.
        """
        self.broker = broker
        self.logger = self.broker.logger

    async def place_order(self,
                          exchange_token: str,
                          transaction_type: str,
                          quantity: int,
                          product: str,
                          validity: str,
                          order_type: str = 'MARKET',
                          price: float = 0,
                          trigger_price: float = 0,
                          disclosed_quantity: int = 0,
                          is_amo: bool = False,
                          tag: str = '') -> Dict[str, str]:
        """
        Places a trade order with the specified broker.

        Parameters:
            exchange_token (str): The exchange token for the instrument.
            transaction_type (str): 'BUY' or 'SELL' transaction type.
            quantity (int): Number of units to trade.
            product (str): Product code, e.g., 'I' for Intraday, 'D' for Delivery.
            validity (str): Order validity, e.g., 'DAY' or 'IOC'.
            order_type (str, optional): Order type, e.g., 'MARKET', 'LIMIT', 'SL', etc. Defaults to 'MARKET'.
            price (float, optional): Price for LIMIT orders. Defaults to 0.
            trigger_price (float, optional): Trigger price for SL orders. Defaults to 0.
            disclosed_quantity (int, optional): Part of the quantity to disclose. Defaults to 0.
            is_amo (bool, optional): Flag for After Market Orders. Defaults to False.
            tag (str, optional): Optional order tag for reference. Defaults to an empty string.

        Returns:
            dict: A dictionary with details of the placed order.

        Raises:
            Exception: If order placement fails, logs the error and raises an exception.
        """
        try:
            order_response = await self.broker.place_order(exchange_token=exchange_token,
                                                           transaction_type=transaction_type,
                                                           quantity=quantity,
                                                           product=product,
                                                           order_type=order_type,
                                                           price=price,
                                                           trigger_price=trigger_price,
                                                           validity=validity,
                                                           disclosed_quantity=disclosed_quantity,
                                                           is_amo=is_amo,
                                                           tag=tag)
            return order_response
        except Exception as e:
            error_msg = f"Exception occurred during order placement: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def modify_order(self,
                           order_id: str,
                           quantity: int = None,
                           price: float = None,
                           order_type: str = None,
                           trigger_price: float = None,
                           disclosed_quantity: int = None,
                           validity: str = None) -> Dict[str, str]:
        """
        Modifies an existing order with the specified parameters.

        Parameters:
            order_id (str): The unique ID of the order to modify.
            quantity (int, optional): Updated quantity for the order. Defaults to None.
            price (float, optional): Updated price for LIMIT orders. Defaults to None.
            order_type (str, optional): Updated order type, e.g., 'MARKET', 'LIMIT'. Defaults to None.
            trigger_price (float, optional): Updated trigger price for SL orders. Defaults to None.
            disclosed_quantity (int, optional): Updated disclosed quantity. Defaults to None.
            validity (str, optional): Updated order validity, e.g., 'DAY', 'IOC'. Defaults to None.

        Returns:
            dict: A dictionary with details of the modified order.

        Raises:
            Exception: If modification fails, logs the error and raises an exception.
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
            error_msg = f"Exception occurred during order modification: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def cancel_order(self, order_id: str) -> Dict[str, str]:
        """
        Cancels an existing order using the order ID.

        Parameters:
            order_id (str): The unique ID of the order to cancel.

        Returns:
            dict: A dictionary with details of the canceled order.

        Raises:
            Exception: If order cancellation fails, logs the error and raises an exception.
        """
        try:
            order_response = await self.broker.cancel_order(order_id=order_id)
            return order_response
        except Exception as e:
            error_msg = f"Order cancellation failed: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def save_order(self, order_details: Dict[str, Any]):
        """
        Saves order response to the database.

        Parameters:
            order_details (dict): The response from an order execution, containing order details.
        
        Raises:
            Exception: Exception while saving the order.
        """
        broker_key = f"{self.broker_name.upper()}_ACCOUNTS"

        try:
            if os.path.exists(ORDERS_FILE):
                with open (ORDERS_FILE, 'r') as f:
                    data = json.load(f)
            else:
                data = {}

            if broker_key not in data:
                data[broker_key] = {}
            if self.account_name not in data[broker_key]:
                data[broker_key][self.account_name] = {}
            if self.strategy_name not in data[broker_key][self.account_name]:
                data[broker_key][self.account_name][self.strategy_name] = {'orders': []}

            data[broker_key][self.account_name][self.strategy_name]["orders"].append(order_details)

            with open (ORDERS_FILE, 'w') as f:
                json.dump(data, f, indent=4)
            self.logger.info(f"Order saved successfully for strategy: {self.strategy_name}")

        except Exception as e:
            error_msg = f"Failed to save order for strategy: {self.strategy_name}. Error: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)            

    async def get_order_details(self, order_id: str) -> Dict[str, Any]:
        """
        Retrieves detailed information for a specific order by ID.

        Parameters:
            order_id (str): The unique ID of the order.

        Returns:
            dict: A dictionary with order details, including status, price, and other relevant information.

        Raises:
            Exception: If retrieval of order details fails, logs the error and raises an exception.
        """
        try:
            order_details = await self.broker.get_order_details(order_id=order_id)
            return order_details
        except Exception as e:
            error_msg = f"Exception occurred while getting order details: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def calculate_margin(self,
                               exchange_token: str,
                               quantity: int,
                               product: str,
                               transaction_type: str,
                               price: float = None) -> float:
        """
        Asynchronously calculates the required margin for a specific trading instrument.

        Args:
            - exchange_token (str): Unique identifier for the instrument.
            - quantity (int): Number of units to be traded.
            - product (str): Product type, e.g., 'D' for delivery.
            - transaction_type (str): Type of transaction, either 'BUY' or 'SELL'.
            - price (Optional[float]): Price of the instrument. If omitted, the Last Traded Price (LTP) will be used.

        Example:
            "exchange_token": "011887",
            "quantity": 100,
            "product": "D",
            "transaction_type": "BUY",
            "price": 2300.0  # Optional

        Returns:
            float: The calculated final margin required for the specified instrument.

        Raises:
            ValueError: If the instrument's exchange_token is not found in the Upstox master file or if margin data is missing in the response.
            Exception: For other API-related errors, such as connectivity or response issues, or if margin retrieval fails due to API or response issues.
        """
        instrument_dict = {
            'exchange_token': exchange_token,
            'quantity': quantity,
            'product': product,
            'transaction_type': transaction_type,
            'price': price
        }
        try:
            margin = await self.broker.calculate_margin(instrument_dict=instrument_dict)
            return margin
        except Exception as e:
            error_msg = f"Exception while getting margin details for instrument: {instrument_dict}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        
    async def calculate_brokerage(self,
                                  exchange_token: str,
                                  quantity: int,
                                  product: str,
                                  transaction_type: str,
                                  price: float = 0) -> float:
        """
        Asynchronously calculates the brokerage for the specified instrument.

        Args:
            - 'exchange_token' (str): The exchange token of the instrument.
            - 'quantity' (int): The quantity to trade.
            - 'product' (str): Product type ('D' for Delivery, 'I' for Intraday, etc.).
            - 'transaction_type' (str): Transaction type, either 'BUY' or 'SELL'.
            - 'price' (float, optional): The price per unit for the instrument.

        Example:
            "exchange_token": "011887",
            "quantity": 100,
            "product": "D",
            "transaction_type": "BUY",
            "price": 2300.0      

        Returns:
            float: The total brokerage charges.

        Raises:
            ValueError: If the exchange token is not found in the instrument data.
            Exception: If the API request fails.
        """
        instrument_dict = {
            'exchange_token': exchange_token,
            'quantity': quantity,
            'product': product,
            'transaction_type': transaction_type,
            'price': price
        }
        try:
            brokerage = await self.broker.calculate_brokerage(instrument_dict=instrument_dict)
            return brokerage
        except Exception as e:
            error_msg = f"Exception while getting brokerage for instrument: {instrument_dict}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def get_funds_and_margin(self) -> float:
        """
        Retrieves the user's fund and margin details from Upstox.

        Parameters:
            access_token (str): The OAuth access token for authentication.
            segment (str, optional): The market segment to query. Use 'SEC' for Equity or 'COM' for Commodity.
                                    If not specified, the response will include both segments.

        Returns:
            dict: A dictionary containing the fund and margin details.

        Raises:
            Exception: If the API request fails or returns an error.
        """
        try:
            balance_response = await self.broker.get_funds_and_margin()
            return balance_response
        except Exception as e:
            error_msg = f"Retrieving Balance failed: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def remove_access_token(self) -> None:
        """
        Removes the access_token key from the JSON dictionary.

        Parameters:
            data (dict): The JSON data to process.

        Returns:
            dict: The updated JSON data with access_token removed.
       
        Raises:
            FileNotFoundError: If the tokens.json file does not exist.
            KeyError: If the specified broker or account does not exist in the file.        
        """
        TOKEN_FILE = 'tokens.json'
        if not os.path.exists(TOKEN_FILE):
            raise FileNotFoundError(f"{TOKEN_FILE} does not exist.") 
        
        try:
            with open(TOKEN_FILE, 'r') as file:
                data = json.load(file)
            
            broker_key = f"{self.broker_name.upper()}_ACCOUNTS"
            if broker_key in data and self.account_name in data[broker_key]:
                if 'access_token' in data[broker_key][self.account_name]:
                    del data[broker_key][self.account_name]['access_token']
                    self.logger.info(f'Access token removed successfully.')
                else:
                    self.logger.warning(f'No access token found.')
            else:
                raise KeyError(f"Broker: '{self.broker_name}' or account: {self.account_name} does not exist in tokens.json file.")
            
            with open (TOKEN_FILE, 'w') as file:
                json.dump(data, file, indent=4)
        except Exception as e:
            self.logger.error(f'An error occured while removing access token: {e}')