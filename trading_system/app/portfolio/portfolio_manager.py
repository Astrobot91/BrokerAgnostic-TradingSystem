import os
import json
from app.brokers.base_broker import BaseBroker
from typing import Dict, Any


POSITIONS_FILE = 'positions.json'

class PortfolioManager:
    """
    A manager class for handling portfolio operations across multiple brokers.
    This class provides a unified interface to initialize broker connections,
    retrieve current orders and positions, and manage trading actions for various brokers.
    """

    def __init__(self,
                 broker_name: str,
                 account_name: str,
                 strategy_name: str,
                 broker: BaseBroker):
        """
        Initializes the PortfolioManager with the given broker and account details.

        Parameters:
            broker_name (str): The name of the broker to connect to.
            account_name (str): The account identifier within the broker.
            strategy_name (str): The trading strategy associated with this manager instance.

        Initializes:
            logger: Logger instance specific to the broker and account.
            broker: Broker instance for handling operations, initialized later in `initialize`.
        """
        self.broker_name = broker_name
        self.account_name = account_name
        self.strategy_name = strategy_name
        self.broker = broker
        self.logger = self.broker.logger

    async def get_broker_orderbook(self) -> list:
        """
        Retrieves the broker's current order book.

        Returns:
            list: A list of dictionaries representing active orders in the order book.

        Raises:
            Exception: If retrieval of the order book fails, logs the error and raises an exception.
        """
        try:
            orderbook_data = await self.broker.get_orderbook()
            return orderbook_data
        except Exception as e:
            error_msg = f"Exception occurred while getting order book: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def get_current_broker_positions(self) -> list:
        """
        Retrieves the broker's current positions.

        Returns:
            list: A list of dictionaries representing the current portfolio positions.

        Raises:
            Exception: If retrieval of current positions fails, logs the error and raises an exception.
        """
        try:
            current_positions = await self.broker.get_current_positions()
            return current_positions
        except Exception as e:
            error_msg = f"Exception occurred while getting current positions: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def save_position(self,
                            entry_order_id: str = '',
                            exit_order_id: str = '',
                            trading_symbol: str = '',
                            exchange_token: str = '',
                            exchange: str = '',
                            order_type: str = '',
                            sl: float = 0.0,
                            target: float = 0.0,
                            entry_transaction_type: str = '',
                            exit_transaction_type: str = '',
                            product: str = '',
                            validity: str = '',
                            quantity: int = 0,
                            entry_price: float = 0.0,
                            exit_price: float = 0.0,
                            entry_timestamp: str = '',
                            exit_timestamp: str = '',
                            status: str = '',
                            variety: str = 'regular',
                            entry_disclosed_quantity: int = 0,
                            entry_filled_quantity: int = 0,
                            entry_pending_quantity: int = 0,
                            exit_disclosed_quantity: int = 0,
                            exit_filled_quantity: int = 0,
                            exit_pending_quantity: int = 0,
                            entry_margin: float = 0,
                            exit_margin: float = 0,
                            entry_brokerage: float = 0,
                            exit_brokerage: float = 0,
                            pnl: float = 0,
                            tag: str = '') -> None:
        """
        Saves an open position to the database, including relevant order details.

        Parameters:
            All parameters are optional and correspond to the keys in the `order_response` dictionary.

        Modifies:
            Creates an `order_response` dictionary from the parameters and adds it to the database.

        Raises:
            Exception: If unable to save the position, logs the error and raises an exception.
        """
        broker_key = f"{self.broker_name.upper()}_ACCOUNTS"

        order_response = {
            "entry_order_id": entry_order_id,
            "exit_order_id": exit_order_id,
            "trading_symbol": trading_symbol,
            "exchange_token": exchange_token,
            "exchange": exchange,
            "order_type": order_type,
            "sl": sl,
            "target": target,
            "entry_transaction_type": entry_transaction_type,
            "exit_transaction_type": exit_transaction_type,
            "product": product,
            "validity": validity,
            "quantity": quantity,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "entry_timestamp": entry_timestamp,
            "exit_timestamp": exit_timestamp,
            "status": status,
            "variety": variety,
            "entry_disclosed_quantity": entry_disclosed_quantity,
            "entry_filled_quantity": entry_filled_quantity,
            "entry_pending_quantity": entry_pending_quantity,
            "exit_disclosed_quantity": exit_disclosed_quantity,
            "exit_filled_quantity": exit_filled_quantity,
            "exit_pending_quantity": exit_pending_quantity,
            "entry_margin": entry_margin,
            "exit_margin": exit_margin,
            "entry_brokerage": entry_brokerage,
            "exit_brokerage": exit_brokerage,
            "pnl": pnl,
            "tag": tag
        }
        try:
            if os.path.exists(POSITIONS_FILE):
                with open(POSITIONS_FILE, 'r') as f:
                    data = json.load(f)
            else:
                data = {}

            if broker_key not in data:
                data[broker_key] = {}
            if self.account_name not in data[broker_key]:
                data[broker_key][self.account_name] = {}
            if self.strategy_name not in data[broker_key][self.account_name]:
                data[broker_key][self.account_name][self.strategy_name] = {'positions': []}

            data[broker_key][self.account_name][self.strategy_name]["positions"].append(order_response)

            with open(POSITIONS_FILE, 'w') as f:
                json.dump(data, f, indent=4)
            self.logger.info(f"Position added successfully for strategy: {self.strategy_name}")

        except Exception as e:
            error_msg = f"Failed to save position for strategy: {self.strategy_name}. Error: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    
    async def load_positions(self, status: str = None) -> list:
        """
        Retrieves a specific order by broker name, account name, strategy name, and order ID.
        
        Parameters:
            status(str): status of the orders to be loaded. (open, closed, pending)
        
        Returns:
            dict: The order details if found, otherwise None.
        """
        positions = {}
        if os.path.exists(POSITIONS_FILE):    
            with open(POSITIONS_FILE, 'r') as f:
                positions = json.load(f)

        broker_key = f"{self.broker_name.upper()}_ACCOUNTS"

        all_positions = (
            positions
            .get(broker_key, {})
            .get(self.account_name, {})
            .get(self.strategy_name, {})
            .get('positions', [])
        )

        filtered_positions = [
            position for position in all_positions
            if position.get('status') == status
        ]
        if status:    
            return filtered_positions
        else:
            return all_positions

    async def update_position(self, tag: str, updates: Dict[str, Any]) -> None:
        """
        Updates a specific position identified by `tag` with the given updates.

        Parameters:
            tag (str): Unique tag identifying the position.
            updates (dict): A dictionary of key-value pairs to update in the position.
        """
        positions = await self.load_positions()
        for position in positions:
            if position.get('tag') == tag:
                position.update(updates)
                await self.overwrite_positions(positions)
                self.logger.info(f"Position with tag {tag} updated successfully.")
                return
        self.logger.warning(f"Position with tag {tag} not found for update.")

    async def overwrite_positions(self, positions_list: list):
        """
        Saves the current positions to the database, overwriting any previous positions.

        Parameters:
            order_response (list): The  list of orders executed, containing order details.

        Modifies:
            Overwrites the positions with the new `order_response` in the database.

        Raises:
            Exception: If unable to save the position, logs the error and raises an exception.
        """
        broker_key = f"{self.broker_name.upper()}_ACCOUNTS"

        try:
            if os.path.exists(POSITIONS_FILE):
                with open(POSITIONS_FILE, 'r') as f:
                    data = json.load(f)
            else:
                data = {}

            if broker_key not in data:
                data[broker_key] = {}
            if self.account_name not in data[broker_key]:
                data[broker_key][self.account_name] = {}
            if self.strategy_name not in data[broker_key][self.account_name]:
                data[broker_key][self.account_name][self.strategy_name] = {}

            data[broker_key][self.account_name][self.strategy_name]["positions"] = positions_list

            with open(POSITIONS_FILE, 'w') as f:
                json.dump(data, f, indent=4)

            self.logger.info(f"Position saved successfully for strategy: {self.strategy_name}")

        except Exception as e:
            error_msg = f"Failed to save position for strategy: {self.strategy_name}. Error: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
