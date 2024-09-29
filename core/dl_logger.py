# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-09-29
# Description: This is the logger class for the data lake

import logging

class DLLogger:
    def __init__(self, name, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Create handler
        console_handler = logging.StreamHandler()

        # Create formatter and add it to handler
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(log_format)

        # Add handler to the logger
        self.logger.addHandler(console_handler)

        self.dl_log = self.logger

    def debug(self, message):
        self.dl_log.debug(message)

    def info(self, message):
        self.dl_log.info(message)

    def warning(self, message):
        self.dl_log.warning(message)

    def error(self, message):
        self.dl_log.error(message)

    def critical(self, message):
        self.dl_log.critical(message)

# Usage example:
# logger = DLLogger(__name__)
# logger.info("This is an info message")
# logger.error("This is an error message")
