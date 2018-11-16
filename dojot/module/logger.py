"""
Logging module
"""

import logging
from logging import config as config_log
from colorlog import ColoredFormatter

class Log:
    """
    Class for logging
    """

    def __init__(self, log_level=logging.DEBUG,
                 log_format="[%(log_color)s%(asctime)-8s%(reset)s] |%(log_color)s%(module)-8s%(reset)s| %(log_color)s%(levelname)s%(reset)s: %(log_color)s%(message)s%(reset)s", is_disabled=False):
        """
        Object initializator

        :param log_level: log level to be used at startup
        :param log_format: how each log line should be printed
        :param is_disabled: flag indicating whether the log should be initially
            disabled.
        """
       
        #Disable all others modules logs
        log_config = {
            'version': 1,
            'disable_existing_loggers': True,
        }

        date_format = '%d/%m/%y - %H:%M:%S'
        config_log.dictConfig(log_config)
        self.formatter = ColoredFormatter(log_format, date_format)
        self.log = logging.getLogger('dojomodulepython.' + __name__)
        self.log.setLevel(log_level)
        self.log.disabled = is_disabled

        if not getattr(self.log, 'handler_set', None):
            self.stream = logging.StreamHandler()
            self.stream.setLevel(log_level)
            self.stream.setFormatter(self.formatter)
            self.log.setLevel(log_level)
            self.log.addHandler(self.stream)
            self.log.handler_set = True

    def color_log(self):
        """
        Returns a logger
        """
        return self.log
