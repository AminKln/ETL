import logging
import os
from datetime import datetime

from colorama import Fore, Style, init

init(autoreset=True)


SUCCESS_LEVEL = 25
logging.addLevelName(SUCCESS_LEVEL, "SUCCESS")

def success(self, message, *args, **kwargs):
    if self.isEnabledFor(SUCCESS_LEVEL):
        self._log(SUCCESS_LEVEL, message, args, **kwargs)

logging.Logger.success = success

class CustomFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\033[97m",  # White
        logging.INFO: "\033[94m",   # Blue
        SUCCESS_LEVEL: Fore.GREEN,  # Green
        logging.WARNING: "\033[93m",  # Yellow
        logging.ERROR: "\033[91m",  # Red
        logging.CRITICAL: "\033[95m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        '''
        Must format message without changing the original records message
        since other handlers with their own formatters will also be using it
        '''
        original_msg = record.msg
        
        color = self.COLORS.get(record.levelno, self.RESET)
        record.msg = f"{color}{original_msg}{self.RESET}"
        formatted_record = super().format(record)

        record.msg = original_msg

        return formatted_record

def configure_logger(log_dir="logs"):
    ''''
    Configures the root logger for the application.
    '''
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = os.path.join(log_dir, f"etl_{timestamp}.log")

    console_handler = logging.StreamHandler()
    console_formatter = CustomFormatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_formatter)

    file_handler = logging.FileHandler(log_file_path, mode="w")
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)