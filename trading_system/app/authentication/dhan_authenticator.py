import os
import json
import logging
from dhanhq import dhanhq
from app.authentication.base_authenticator import BaseAuthenticator
from app.utils.logger import get_logger
from app.config import DHAN_ACCOUNTS

TOKEN_FILE = 'tokens.json'

class DhanAuthenticator(BaseAuthenticator):
    
    def __init__(self, account_name: str):
        self.account_name = account_name
        self.broker_name = 'Dhan'
        self.account_config = DHAN_ACCOUNTS.get(self.account_name)
        self.client_id = self.account_config['CLIENT_ID']
        self.access_token = self.account_config.get('ACCESS_TOKEN')
        self.logger = get_logger(
            main_folder_name=f'Authenticators/DhanAuthenticator/Accounts/{self.account_name}',
            account_name=self.account_name,
            broker_name=self.broker_name
        )
        self.dhan_client = None

    async def authenticate(self):
        """
        Performs authentication process to obtain/validate the access token.

        Returns:
            str: Access token if authentication is successful.

        Raises:
            Exception: If authentication fails.
        """
        self.logger.info('Starting authentication process for Dhan')
        
        # If still no access_token, raise exception
        if not self.access_token:
            self.logger.error('No access token available for Dhan.')
            raise Exception('Authentication failed: No access token provided')
            
        # Initialize the Dhan client with the access token
        try:
            self.dhan_client = dhanhq(self.client_id, self.access_token)
            # Verify the token by making a simple API call
            self._verify_token()
            # Save the token for future use

            return self.access_token
        except Exception as e:
            self.logger.error(f'Failed to authenticate with Dhan: {e}')
            raise Exception(f'Dhan authentication failed: {str(e)}')

    def get_auth_token(self):
        """
        Retrieves the current access token.

        Returns:
            str: The access token.
        """
        return self.access_token
    
    def get_client(self):
        """
        Returns the authenticated Dhan client.
        
        Returns:
            dhanhq: The authenticated Dhan client.
        """
        if not self.dhan_client:
            self.dhan_client = dhanhq(self.client_id, self.access_token)
        return self.dhan_client

    def _verify_token(self):
        """
        Verifies that the access token is valid by making a simple API call.
        
        Raises:
            Exception: If the token verification fails.
        """
        try:
            # Make a simple API call to verify the token
            # For example, get user profile or account balance
            # Adjust this based on Dhan API capabilities
            self.dhan_client.get_user_details()
            self.logger.info('Access token verified successfully')
        except Exception as e:
            self.logger.error(f'Token verification failed: {e}')
            self.access_token = None
            raise Exception(f'Invalid access token: {str(e)}')

    def _save_access_token(self):
        """
        Saves the access token to a JSON file.
        """
        token_data = {
            'access_token': self.access_token,
        }
        try:
            if os.path.exists(TOKEN_FILE):
                with open(TOKEN_FILE, 'r') as f:
                    data = json.load(f)
            else:
                data = {'DHAN_ACCOUNTS': {}}

            if 'DHAN_ACCOUNTS' not in data:
                data['DHAN_ACCOUNTS'] = {}

            data['DHAN_ACCOUNTS'][self.account_name] = token_data

            with open(TOKEN_FILE, 'w') as f:
                json.dump(data, f, indent=4)
            self.logger.info('Access token saved to tokens.json')
        except Exception as e:
            self.logger.error(f"Error saving access token: {e}")

    def _load_access_token(self):
        """
        Loads the access token from a JSON file if available.
        """
        try:
            with open(TOKEN_FILE, 'r') as f:
                data = json.load(f)
                account_data = data.get('DHAN_ACCOUNTS', {}).get(self.account_name)
                if account_data:
                    self.access_token = account_data['access_token']
                    self.logger.info('Access token loaded from tokens.json')
                else:
                    self.access_token = None
        except FileNotFoundError:
            self.logger.info('tokens.json file not found')
            self.access_token = None
        except Exception as e:
            self.logger.error(f"Error loading access token: {e}")
            self.access_token = None