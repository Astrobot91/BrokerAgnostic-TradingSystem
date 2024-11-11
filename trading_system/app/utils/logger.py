import logging
import os
from threading import Lock
from datetime import date
from logging.handlers import RotatingFileHandler
import apscheduler


DEFAULT_LOG_HANDLER = '/home/admin/Strategy_Execution_Engine/trading_system/app/logs'


class DailyFileHandler(logging.FileHandler):
    """Custom FileHandler that creates a new log file every day."""

    def  __init__(self, log_dir, mode='a', encoding=None, delay=False):
        self.log_dir = log_dir
        self.current_date = date.today()
        filename = os.path.join(self.log_dir, self._get_filename())
        super().__init__(filename, mode, encoding, delay)

    def _get_filename(self):
        return f'{self.current_date}.log'
    
    def emit(self, record):
        try:
            self.acquire()
            new_date = date.today()

            if new_date != self.current_date:
                self.current_date = new_date
                self.close()
                self.baseFilename = os.path.abspath(
                    os.path.join(self.log_dir, self._get_filename())
                )
                self.stream = self._open()
            else:
                if self.stream is None:
                    self.stream = self._open()

            super().emit(record)
        finally:
            self.release()


class ContextFilter(logging.Filter):
    """ContextFilter to add broker_name, account_name, and strategy_name in every log record."""

    def __init__(self, broker_name, account_name):
        self.broker_name = broker_name
        self.account_name = account_name

    def filter(self, record):
        record.broker_name = self.broker_name
        record.account_name = self.account_name
        return True

    
def get_logger(main_folder_name: str = '', broker_name: str = '', account_name: str = '', log_dir = DEFAULT_LOG_HANDLER) -> logging.Logger:
    """Returns a logger instance with file and console handlers."""

    log_dir = os.path.join(DEFAULT_LOG_HANDLER, main_folder_name)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logger = logging.getLogger(f'{broker_name}_{account_name}')
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        context_filter = ContextFilter(broker_name, account_name)
        logger.addFilter(context_filter)
    
        file_handler = DailyFileHandler(log_dir=log_dir)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            '%(broker_name)s - %(account_name)s - %(asctime)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(broker_name)s - %(account_name)s - %(asctime)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
